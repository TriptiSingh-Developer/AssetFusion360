"""
AssetFusion360 - Detail Type Dimension

Source: Gold fact_transaction_details
Output: Gold dim_detail_type

Preserves original detail type values.
Creates an explicit missing-value member if needed.
"""

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "data" / "04_gold_analytics"

SOURCE = GOLD / "fact_transaction_details"
FINAL = GOLD / "dim_detail_type"
STAGING = GOLD / "_staging_dim_detail_type"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "dim_detail_type_manifest.json"
)

EXPECTED_ROWS = 2_301_927
EXPECTED_FILES = 461

MISSING_LABEL = "<MISSING_DETAIL_TYPE>"

SCHEMA = pa.schema([
    pa.field("detail_type_key", pa.int32(), nullable=False),
    pa.field("detail_type", pa.string(), nullable=False),
    pa.field("is_missing_type", pa.bool_(), nullable=False),
    pa.field("source_detail_count", pa.int64(), nullable=False),
])


def build_dimension():
    print("=" * 68)
    print("STEP 8.2H - BUILD DIM_DETAIL_TYPE")
    print("=" * 68)

    for path in [FINAL, STAGING, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    files = sorted(SOURCE.glob("*.parquet"))
    assert len(files) == EXPECTED_FILES

    counts = Counter()

    for index, path in enumerate(files, 1):
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=10000,
                columns=["detail_type"],
            ):
                counts.update(batch.column(0).to_pylist())

        if index % 100 == 0 or index == len(files):
            print(
                f"Processed files: {index}/{len(files)}",
                flush=True,
            )

    assert sum(counts.values()) == EXPECTED_ROWS

    non_null = [
        value for value in counts
        if value is not None
    ]

    assert all(isinstance(v, str) for v in non_null)
    assert MISSING_LABEL not in non_null

    records = []

    if counts[None] > 0:
        records.append({
            "detail_type_key": 0,
            "detail_type": MISSING_LABEL,
            "is_missing_type": True,
            "source_detail_count": counts[None],
        })

    for key, detail_type in enumerate(sorted(non_null), 1):
        records.append({
            "detail_type_key": key,
            "detail_type": detail_type,
            "is_missing_type": False,
            "source_detail_count": counts[detail_type],
        })

    assert records, "No detail types found"

    assert sum(
        row["source_detail_count"]
        for row in records
    ) == EXPECTED_ROWS

    assert len({
        row["detail_type_key"] for row in records
    }) == len(records)

    assert len({
        row["detail_type"] for row in records
    }) == len(records)

    print("\nDETAIL TYPE DISTRIBUTION")

    for record in records:
        print(
            f"{record['detail_type']}: "
            f"{record['source_detail_count']:,}"
        )

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

    print("\nSTAGED DIMENSION VALIDATION: PASS")

    if FINAL.exists():
        raise FileExistsError(FINAL)

    STAGING.rename(FINAL)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "dim_detail_type",
        "status": "COMPLETED",
        "rows": len(records),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "source_detail_rows": EXPECTED_ROWS,
        "missing_detail_types": counts[None],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST.open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print("\nDIM_DETAIL_TYPE")
    print("Dimension rows:", len(records))
    print("Missing detail types:", counts[None])
    print("Unique dimension keys: PASS")
    print("Source-to-dimension reconciliation: PASS")
    print("Published dimension: PASS")

    print("\nSTEP 8.2H DIM_DETAIL_TYPE: PASS")


if __name__ == "__main__":
    build_dimension()
