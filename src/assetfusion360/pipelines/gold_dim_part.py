"""
AssetFusion360 - Step 8.2L
Gold Part Dimension

Source: fact_transaction_details
Target: dim_part

One row per unique part code.
Preserves part identities and source attributes.
Includes a missing-part reporting member.
"""

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "data" / "04_gold_analytics"

SOURCE = GOLD / "fact_transaction_details"
FINAL = GOLD / "dim_part"
STAGING = GOLD / "_staging_dim_part"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "dim_part_manifest.json"
)

EXPECTED_ROWS = 2_301_927
EXPECTED_FILES = 461
EXPECTED_PARTS = 14_422
EXPECTED_MISSING = 27_891

MISSING_LABEL = "<MISSING_PART_CODE>"

FIELDS = [
    "part_code",
    "part_identity",
    "part_description",
    "part_class",
    "part_uom",
]

SCHEMA = pa.schema([
    pa.field("part_key", pa.int32(), nullable=False),
    pa.field("part_code", pa.string(), nullable=False),
    pa.field("part_identity", pa.int64()),
    pa.field("part_description", pa.string()),
    pa.field("part_class", pa.string()),
    pa.field("part_uom", pa.string()),
    pa.field("is_missing_part", pa.bool_(), nullable=False),
    pa.field("source_detail_count", pa.int64(), nullable=False),
])


def check_outputs():
    assert SOURCE.is_dir(), "Source Gold table missing"

    for path in [FINAL, STAGING, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    print("Output safety: PASS")


def collect_parts():
    files = sorted(SOURCE.glob("*.parquet"))
    assert len(files) == EXPECTED_FILES

    reference_schema = pq.read_schema(files[0])

    for field in FIELDS:
        assert field in reference_schema.names

    attributes = {}
    identity_to_code = {}
    counts = Counter()

    total_rows = 0
    missing_codes = 0
    missing_identities = 0
    missing_pairs = 0

    for index, path in enumerate(files, 1):
        assert pq.read_schema(path).equals(
            reference_schema,
            check_metadata=False
        )

        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=10000,
                columns=FIELDS,
            ):
                for row in batch.to_pylist():
                    total_rows += 1

                    code = row["part_code"]
                    identity = row["part_identity"]

                    if code is None:
                        missing_codes += 1

                    if identity is None:
                        missing_identities += 1

                    if code is None and identity is None:
                        missing_pairs += 1

                    # A complete part relationship requires
                    # both code and source identity.
                    if code is None or identity is None:
                        continue

                    attrs = (
                        identity,
                        row["part_description"],
                        row["part_class"],
                        row["part_uom"],
                    )

                    if code in attributes:
                        assert attributes[code] == attrs, (
                            f"Conflicting attributes: {code}"
                        )
                    else:
                        attributes[code] = attrs

                    if identity in identity_to_code:
                        assert identity_to_code[identity] == code, (
                            f"Identity/code conflict: {identity}"
                        )
                    else:
                        identity_to_code[identity] = code

                    counts[code] += 1

        if index % 100 == 0 or index == len(files):
            print(
                f"Processed files: {index}/{len(files)}",
                flush=True,
            )

    assert total_rows == EXPECTED_ROWS
    assert missing_codes == EXPECTED_MISSING
    assert missing_identities == EXPECTED_MISSING

    # Confirm missing codes and identities occur
    # together rather than on separate records.
    assert missing_pairs == EXPECTED_MISSING, (
        "Partial part references found; review required"
    )

    assert len(attributes) == EXPECTED_PARTS
    assert len(identity_to_code) == EXPECTED_PARTS

    assert sum(counts.values()) == (
        EXPECTED_ROWS - EXPECTED_MISSING
    )

    assert MISSING_LABEL not in attributes

    print("\nSOURCE VALIDATION")
    print("Source detail records:", f"{total_rows:,}")
    print("Distinct valid parts:", len(attributes))
    print("Missing part references:", missing_pairs)
    print("Attribute consistency: PASS")
    print("Part identity consistency: PASS")

    return attributes, counts, missing_pairs


def build_dim_part():
    print("=" * 68)
    print("STEP 8.2L - BUILD DIM_PART")
    print("=" * 68)

    check_outputs()

    attributes, counts, missing_count = collect_parts()

    records = [{
        "part_key": 0,
        "part_code": MISSING_LABEL,
        "part_identity": None,
        "part_description": "Missing part reference",
        "part_class": None,
        "part_uom": None,
        "is_missing_part": True,
        "source_detail_count": missing_count,
    }]

    for key, code in enumerate(sorted(attributes), 1):
        identity, description, part_class, uom = (
            attributes[code]
        )

        records.append({
            "part_key": key,
            "part_code": code,
            "part_identity": identity,
            "part_description": description,
            "part_class": part_class,
            "part_uom": uom,
            "is_missing_part": False,
            "source_detail_count": counts[code],
        })

    assert len(records) == EXPECTED_PARTS + 1

    assert len({
        row["part_key"] for row in records
    }) == len(records)

    assert len({
        row["part_code"] for row in records
    }) == len(records)

    assert sum(
        row["source_detail_count"] for row in records
    ) == EXPECTED_ROWS

    print("\nDIMENSION VALIDATION")
    print("Dimension rows:", len(records))
    print("Valid parts:", EXPECTED_PARTS)
    print("Missing-part member:", 1)
    print("Unique surrogate keys: PASS")
    print("Unique part codes: PASS")
    print("Source-to-dimension reconciliation: PASS")

    STAGING.mkdir(parents=True, exist_ok=False)

    staged_file = STAGING / "part-00000.parquet"

    table = pa.Table.from_pylist(
        records,
        schema=SCHEMA,
    )

    pq.write_table(
        table,
        staged_file,
        compression="snappy",
    )

    with pq.ParquetFile(str(staged_file)) as reader:
        assert reader.metadata.num_rows == len(records)

        assert reader.schema_arrow.equals(
            SCHEMA,
            check_metadata=False
        )

        verified = reader.read().to_pylist()

    assert verified == records

    print("Staged dimension validation: PASS")

    if FINAL.exists():
        raise FileExistsError(FINAL)

    STAGING.rename(FINAL)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "dim_part",
        "status": "COMPLETED",
        "rows": len(records),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "source_detail_rows": EXPECTED_ROWS,
        "distinct_parts": EXPECTED_PARTS,
        "missing_part_rows": missing_count,
        "attribute_conflicts": 0,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST.open(
        "x", encoding="utf-8"
    ) as handle:
        json.dump(manifest, handle, indent=2)

    print("\nGOLD DIM_PART: SUCCESS")
    print("STEP 8.2L DIM_PART: PASS")


if __name__ == "__main__":
    build_dim_part()
