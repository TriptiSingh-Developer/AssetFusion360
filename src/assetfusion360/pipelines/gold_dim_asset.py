
"""
AssetFusion360 Capstone
Step 7.5B - Gold Asset Dimension

Source: AMPS Assets Silver
Target: Gold dim_asset

Rules:
- One row per asset identity.
- Preserve all 148,522 asset records.
- Preserve ambiguous business codes.
- Do not modify Bronze or Silver.
- Never overwrite existing Gold outputs.
- Validate before publication.
"""

import gc
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

SOURCE = (
    ROOT
    / "data"
    / "03_silver_cleaned_data"
    / "amps_assets"
    / "part-00000.parquet"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

FINAL_DIR = GOLD_ROOT / "dim_asset"

STAGING_DIR = GOLD_ROOT / "_staging_dim_asset"

FINAL_FILE = FINAL_DIR / "part-00000.parquet"

STAGED_FILE = STAGING_DIR / "part-00000.parquet"

MANIFEST = (
    ROOT
    / "reports"
    / "data_profiling_reports"
    / "gold_dim_asset_manifest.json"
)

EXPECTED_ROWS = 148_522

BATCH_SIZE = 5000


# ============================================================
# 2. SOURCE FIELD DEFINITIONS
# ============================================================

SOURCE_FIELDS = {
    "OBJ_SQLIDENTITY": pa.string(),
    "OBJ_CODE": pa.string(),
    "OBJ_STATUS": pa.string(),
    "OBJ_PARENT": pa.string(),
    "asset_identity_int": pa.int64(),
    "asset_commissioned_ts": pa.timestamp("us"),
    "asset_value_decimal": pa.decimal128(20, 6),
    "_bronze_source_file": pa.string(),
}


# ============================================================
# 3. GOLD DIMENSION SCHEMA
# ============================================================

GOLD_SCHEMA = pa.schema([
    pa.field(
        "asset_identity",
        pa.int64(),
        nullable=False
    ),
    pa.field(
        "asset_code",
        pa.string()
    ),
    pa.field(
        "asset_status",
        pa.string()
    ),
    pa.field(
        "parent_asset_code",
        pa.string()
    ),
    pa.field(
        "commissioned_at",
        pa.timestamp("us")
    ),
    pa.field(
        "asset_value",
        pa.decimal128(20, 6)
    ),
    pa.field(
        "asset_code_match_count",
        pa.int32()
    ),
    pa.field(
        "asset_code_ambiguous",
        pa.bool_()
    ),
    pa.field(
        "asset_code_placeholder",
        pa.bool_()
    ),
    pa.field(
        "source_file",
        pa.string()
    ),
])

EXPECTED_GOLD_COLUMNS = 10

assert len(GOLD_SCHEMA) == EXPECTED_GOLD_COLUMNS


# ============================================================
# 4. HELPER FUNCTIONS
# ============================================================

def normalize(value):
    if value is None:
        return None

    value = str(value).strip()

    return value or None


# ============================================================
# 5. OUTPUT SAFETY
# ============================================================

def check_outputs():
    if not SOURCE.is_file():
        raise FileNotFoundError(
            f"Silver source missing: {SOURCE}"
        )

    for path in [
        FINAL_DIR,
        STAGING_DIR,
        MANIFEST,
    ]:
        if path.exists():
            raise FileExistsError(
                "Existing Gold artifact must be preserved: "
                f"{path}"
            )

    print("Gold output safety: PASS")


# ============================================================
# 6. SILVER SOURCE VALIDATION
# ============================================================

def inspect_source():
    with pq.ParquetFile(str(SOURCE)) as reader:
        schema = reader.schema_arrow
        rows = reader.metadata.num_rows

    if rows != EXPECTED_ROWS:
        raise ValueError(
            f"Expected {EXPECTED_ROWS} rows, found {rows}"
        )

    if len(schema) != 43:
        raise ValueError(
            f"Expected 43 Silver columns, found {len(schema)}"
        )

    for field_name, expected_type in SOURCE_FIELDS.items():

        if field_name not in schema.names:
            raise ValueError(
                f"Missing Silver source field: {field_name}"
            )

        actual_type = schema.field(
            field_name
        ).type

        if not actual_type.equals(expected_type):
            raise ValueError(
                f"Unexpected type for {field_name}: "
                f"{actual_type}"
            )

    print(
        "Silver Assets records:",
        f"{rows:,}"
    )

    print(
        "Silver Assets columns:",
        len(schema)
    )

    print("Required Gold inputs: PASS")

    return schema


# ============================================================
# 7. ASSET CODE UNIQUENESS INDEX
# ============================================================

def build_asset_code_index():
    counts = Counter()

    identities = set()

    rows = 0

    with pq.ParquetFile(str(SOURCE)) as reader:

        for batch in reader.iter_batches(
            batch_size=BATCH_SIZE,
            columns=[
                "asset_identity_int",
                "OBJ_CODE",
            ],
        ):

            for record in batch.to_pylist():

                rows += 1

                identity = record[
                    "asset_identity_int"
                ]

                if identity is None:
                    raise ValueError(
                        "Missing asset identity"
                    )

                if identity in identities:
                    raise ValueError(
                        f"Duplicate asset identity: {identity}"
                    )

                identities.add(identity)

                code = normalize(
                    record["OBJ_CODE"]
                )

                if code is not None:
                    counts[code] += 1

    assert rows == EXPECTED_ROWS

    assert len(identities) == EXPECTED_ROWS

    assert len(counts) == 148_521, (
        "Asset code inventory differs "
        "from Step 7.4"
    )

    ambiguous = {
        code: count
        for code, count in counts.items()
        if count > 1
    }

    assert ambiguous == {
        "B223PA": 2
    }, (
        f"Unexpected ambiguous asset codes: "
        f"{ambiguous}"
    )

    print(
        "Unique asset identities:",
        len(identities)
    )

    print(
        "Distinct asset codes:",
        len(counts)
    )

    print(
        "Ambiguous asset codes:",
        ambiguous
    )

    return counts


# ============================================================
# 8. TRANSFORM ONE ASSET RECORD
# ============================================================

def transform_record(row, code_counts):

    code = normalize(
        row["OBJ_CODE"]
    )

    identity = row[
        "asset_identity_int"
    ]

    if identity is None:
        raise ValueError(
            "Null asset identity"
        )

    count = (
        code_counts.get(code, 0)
        if code is not None
        else 0
    )

    return {
        "asset_identity": identity,

        "asset_code": code,

        "asset_status": row[
            "OBJ_STATUS"
        ],

        "parent_asset_code": row[
            "OBJ_PARENT"
        ],

        "commissioned_at": row[
            "asset_commissioned_ts"
        ],

        "asset_value": row[
            "asset_value_decimal"
        ],

        "asset_code_match_count": count,

        "asset_code_ambiguous": (
            count > 1
        ),

        "asset_code_placeholder": (
            code is not None
            and code.upper() in {
                "*",
                "N/A",
                "NA",
                "NULL",
                "UNKNOWN",
            }
        ),

        "source_file": row[
            "_bronze_source_file"
        ],
    }


# ============================================================
# 9. MAIN GOLD TRANSFORMATION
# ============================================================

def build_gold_dim_asset():

    print("=" * 70)
    print("STEP 7.5B - BUILD GOLD DIM_ASSET")
    print("=" * 70)

    check_outputs()

    inspect_source()

    code_counts = build_asset_code_index()

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False
    )

    written = 0

    ambiguous_rows = 0

    placeholder_rows = 0

    with pq.ParquetFile(str(SOURCE)) as reader:

        with pq.ParquetWriter(
            str(STAGED_FILE),
            GOLD_SCHEMA,
            compression="snappy",
            version="2.6",
        ) as writer:

            for batch in reader.iter_batches(
                batch_size=BATCH_SIZE
            ):

                records = [
                    transform_record(
                        row,
                        code_counts
                    )
                    for row in batch.to_pylist()
                ]

                ambiguous_rows += sum(
                    row["asset_code_ambiguous"]
                    for row in records
                )

                placeholder_rows += sum(
                    row["asset_code_placeholder"]
                    for row in records
                )

                table = pa.Table.from_pylist(
                    records,
                    schema=GOLD_SCHEMA,
                )

                writer.write_table(table)

                written += table.num_rows

                print(
                    f"Processed: {written:,} "
                    f"of {EXPECTED_ROWS:,} assets",
                    flush=True,
                )

    # ========================================================
    # 10. STAGED GOLD VALIDATION
    # ========================================================

    assert written == EXPECTED_ROWS

    with pq.ParquetFile(
        str(STAGED_FILE)
    ) as reader:

        assert reader.metadata.num_rows == EXPECTED_ROWS

        assert reader.schema_arrow.equals(
            GOLD_SCHEMA,
            check_metadata=False,
        )

        verified_rows = 0

        identities = set()

        actual_ambiguous = 0

        for batch in reader.iter_batches(
            batch_size=BATCH_SIZE
        ):

            for record in batch.to_pylist():

                verified_rows += 1

                identity = record[
                    "asset_identity"
                ]

                assert identity not in identities

                identities.add(identity)

                actual_ambiguous += int(
                    record["asset_code_ambiguous"]
                )

                code = record[
                    "asset_code"
                ]

                expected_count = (
                    code_counts.get(code, 0)
                    if code is not None
                    else 0
                )

                assert (
                    record["asset_code_match_count"]
                    == expected_count
                )

        assert verified_rows == EXPECTED_ROWS

        assert len(identities) == EXPECTED_ROWS

        assert actual_ambiguous == 2

    assert ambiguous_rows == 2

    # ========================================================
    # 11. PUBLISH GOLD DATASET
    # ========================================================

    # All readers and writers are closed before
    # renaming, reducing Windows file-lock risks.

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(
            "Final Gold directory appeared "
            "during processing."
        )

    STAGING_DIR.rename(
        FINAL_DIR
    )

    assert FINAL_FILE.is_file()

    # ========================================================
    # 12. WRITE COMPLETION MANIFEST
    # ========================================================

    manifest_data = {
        "project": "AssetFusion360",

        "layer": "Gold",

        "table": "dim_asset",

        "status": "COMPLETED",

        "source": (
            SOURCE.relative_to(ROOT).as_posix()
        ),

        "output": (
            FINAL_FILE.relative_to(ROOT).as_posix()
        ),

        "rows": EXPECTED_ROWS,

        "columns": len(GOLD_SCHEMA),

        "unique_asset_identities": EXPECTED_ROWS,

        "distinct_asset_codes": len(code_counts),

        "ambiguous_asset_codes": {
            "B223PA": 2
        },

        "ambiguous_asset_rows": ambiguous_rows,

        "placeholder_asset_rows": placeholder_rows,

        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with MANIFEST.open(
        "x",
        encoding="utf-8"
    ) as handle:

        json.dump(
            manifest_data,
            handle,
            indent=2,
            ensure_ascii=False,
        )

    # ========================================================
    # 13. FINAL SUMMARY
    # ========================================================

    print("\n" + "=" * 70)

    print("GOLD DIM_ASSET SUMMARY")

    print("=" * 70)

    print(
        "Gold records:",
        f"{written:,}"
    )

    print(
        "Gold columns:",
        len(GOLD_SCHEMA)
    )

    print(
        "Unique identities:",
        f"{len(identities):,}"
    )

    print(
        "Ambiguous asset rows:",
        ambiguous_rows
    )

    print(
        "Placeholder asset rows:",
        placeholder_rows
    )

    print(
        "Gold output:",
        FINAL_FILE
    )

    print(
        "Manifest:",
        MANIFEST
    )

    print("\nGold dimension validation: PASS")

    print("GOLD DIM_ASSET BUILD: SUCCESS")

    return manifest_data


# ============================================================
# 14. KEDRO PIPELINE
# ============================================================

def create_gold_dim_asset_pipeline():

    return pipeline([
        node(
            func=build_gold_dim_asset,

            inputs=None,

            outputs="gold_dim_asset_manifest",

            name="build_gold_dim_asset",

            tags=[
                "gold",
                "assets",
                "dimension",
            ],
        )
    ])


# ============================================================
# 15. KEDRO RUNNER
# ============================================================

def run_kedro_pipeline():

    catalog = DataCatalog({
        "gold_dim_asset_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    runner.run(
        create_gold_dim_asset_pipeline(),
        catalog,
    )

    result = catalog.load(
        "gold_dim_asset_manifest"
    )

    if result.get("status") != "COMPLETED":
        raise RuntimeError(
            "Gold Asset Dimension "
            "manifest validation failed."
        )

    print(
        "\nKEDRO GOLD DIM_ASSET PIPELINE: SUCCESS"
    )


if __name__ == "__main__":
    run_kedro_pipeline()
