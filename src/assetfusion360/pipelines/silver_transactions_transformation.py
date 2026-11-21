
"""
AssetFusion360 Capstone
Step 6.5B - AMPS Transactions Silver Transformation

Preserves:
- All 755,122 records
- All 18 original Bronze columns
- All original field values and source lineage
- Business codes and optional references

Adds:
- 7 typed analytical columns
- 8 data-quality flags

No Bronze modifications or existing-output overwrites.
"""

import gc
import json
import re

from collections import Counter
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

BRONZE_DIR = (
    ROOT / "data" / "02_bronze_ingested_data"
    / "amps_transactions"
)

SILVER_ROOT = (
    ROOT / "data" / "03_silver_cleaned_data"
)

SILVER_DIR = SILVER_ROOT / "amps_transactions"

STAGING_DIR = (
    SILVER_ROOT / "_staging_amps_transactions"
)

MANIFEST_FILE = (
    ROOT / "reports" / "data_profiling_reports"
    / "amps_transactions_silver_transformation_manifest.json"
)

EXPECTED_FILES = 152
EXPECTED_ROWS = 755_122
EXPECTED_BRONZE_COLUMNS = 18
BATCH_SIZE = 5000


# ============================================================
# 2. SILVER COLUMN DEFINITIONS
# ============================================================

INTEGER_FIELDS = {
    "tra_code": "transaction_code_int",
    "tra_sqlidentity": "transaction_identity_int",
    "tra_req": "transaction_request_int",
    "tra_updatecount": "transaction_updatecount_int",
}

DATE_FIELDS = {
    "tra_date": "transaction_date_ts",
    "tra_created": "transaction_created_ts",
    "tra_updated": "transaction_updated_ts",
}

INTEGER_FLAGS = {
    "tra_code": "dq_transaction_code_invalid",
    "tra_sqlidentity": "dq_transaction_identity_invalid",
    "tra_req": "dq_transaction_request_invalid",
    "tra_updatecount": "dq_transaction_updatecount_invalid",
}

DATE_FLAGS = {
    "tra_date": "dq_transaction_date_invalid",
    "tra_created": "dq_transaction_created_invalid",
    "tra_updated": "dq_transaction_updated_invalid",
}

QUALITY_FLAGS = (
    list(INTEGER_FLAGS.values())
    + list(DATE_FLAGS.values())
    + ["dq_any_issue"]
)

TYPED_FIELDS = (
    [
        pa.field(name, pa.int64())
        for name in INTEGER_FIELDS.values()
    ]
    + [
        pa.field(name, pa.timestamp("us"))
        for name in DATE_FIELDS.values()
    ]
)

QUALITY_FIELDS = [
    pa.field(name, pa.bool_())
    for name in QUALITY_FLAGS
]

ADDED_FIELDS = TYPED_FIELDS + QUALITY_FIELDS

EXPECTED_SILVER_COLUMNS = (
    EXPECTED_BRONZE_COLUMNS + len(ADDED_FIELDS)
)

assert len(TYPED_FIELDS) == 7
assert len(QUALITY_FLAGS) == 8
assert EXPECTED_SILVER_COLUMNS == 33


# ============================================================
# 3. VALUE PARSING
# ============================================================

def missing(value):
    return value is None or str(value).strip() == ""


def parse_integer(value):
    if missing(value):
        return None, False

    text = str(value).strip()

    if not re.fullmatch(r"[+-]?\d+", text):
        return None, True

    try:
        result = int(text)
    except ValueError:
        return None, True

    if not -(2**63) <= result < 2**63:
        return None, True

    return result, False


@lru_cache(maxsize=100000)
def parse_date_cached(text):
    normalized = re.sub(
        r"\s+", " ", text.strip()
    )

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(
                normalized, fmt
            )
        except ValueError:
            continue

    return None


def parse_date(value):
    if missing(value):
        return None, False

    result = parse_date_cached(str(value))
    return result, result is None


# ============================================================
# 4. TRANSFORM A SINGLE RECORD
# ============================================================

def transform_row(source):
    result = dict(source)
    flags = {}

    for original, target in INTEGER_FIELDS.items():
        value, invalid = parse_integer(
            source.get(original)
        )

        result[target] = value
        flags[INTEGER_FLAGS[original]] = invalid

    for original, target in DATE_FIELDS.items():
        value, invalid = parse_date(
            source.get(original)
        )

        result[target] = value
        flags[DATE_FLAGS[original]] = invalid

    # Required source identifiers are flagged
    # when missing; tra_req remains optional.
    for source_field in [
        "tra_code",
        "tra_sqlidentity",
    ]:
        if missing(source.get(source_field)):
            flags[INTEGER_FLAGS[source_field]] = True

    flags["dq_any_issue"] = any(flags.values())

    result.update(flags)

    return result


# ============================================================
# 5. OUTPUT SAFETY
# ============================================================

def check_outputs():
    if not BRONZE_DIR.is_dir():
        raise FileNotFoundError(
            f"Bronze directory missing: {BRONZE_DIR}"
        )

    for path in [
        SILVER_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
    ]:
        if path.exists():
            raise FileExistsError(
                "Existing Silver artifact must be preserved: "
                f"{path}"
            )


# ============================================================
# 6. SOURCE VALIDATION
# ============================================================

def inspect_sources():
    files = sorted(
        BRONZE_DIR.glob("*.parquet")
    )

    if len(files) != EXPECTED_FILES:
        raise ValueError(
            f"Expected {EXPECTED_FILES} files, "
            f"found {len(files)}"
        )

    schema = pq.read_schema(files[0])

    if len(schema) != EXPECTED_BRONZE_COLUMNS:
        raise ValueError(
            "Unexpected Bronze column count"
        )

    required = [
        *INTEGER_FIELDS.keys(),
        *DATE_FIELDS.keys(),
        "tra_type",
        "tra_status",
        "tra_fromentity",
        "tra_fromcode",
        "tra_toentity",
        "tra_tocode",
    ]

    for field in required:
        if field not in schema.names:
            raise ValueError(
                f"Missing required field: {field}"
            )

    for field in schema:
        if not field.type.equals(pa.string()):
            raise ValueError(
                f"Unexpected Bronze type: {field.name}"
            )

    for field in ADDED_FIELDS:
        if field.name in schema.names:
            raise ValueError(
                f"Silver field collision: {field.name}"
            )

    total_rows = 0

    for path in files:
        metadata = pq.read_metadata(path)
        actual_schema = pq.read_schema(path)

        if not actual_schema.equals(
            schema,
            check_metadata=False,
        ):
            raise ValueError(
                f"Schema mismatch: {path.name}"
            )

        total_rows += metadata.num_rows

    if total_rows != EXPECTED_ROWS:
        raise ValueError(
            f"Bronze row mismatch: {total_rows}"
        )

    print("Bronze files:", len(files))
    print("Bronze records:", f"{total_rows:,}")
    print("Bronze columns:", len(schema))
    print("Bronze validation: PASS")

    return files, schema


# ============================================================
# 7. MAIN TRANSFORMATION
# ============================================================

def transform_transactions():
    print("=" * 70)
    print("STEP 6.5B - AMPS TRANSACTIONS SILVER")
    print("=" * 70)

    check_outputs()

    files, bronze_schema = inspect_sources()

    silver_schema = pa.schema(
        list(bronze_schema) + ADDED_FIELDS
    )

    quality_counts = Counter({
        name: 0
        for name in QUALITY_FLAGS
    })

    total_written = 0
    total_verified = 0

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False,
    )

    for index, source_path in enumerate(files, 1):
        staged_path = STAGING_DIR / source_path.name
        written = 0

        # Read and transform. All file handles are
        # closed before the file is validated.

        with pq.ParquetFile(str(source_path)) as source:
            expected_file_rows = source.metadata.num_rows

            with pq.ParquetWriter(
                str(staged_path),
                silver_schema,
                compression="snappy",
                version="2.6",
            ) as writer:

                for batch in source.iter_batches(
                    batch_size=BATCH_SIZE
                ):
                    source_records = (
                        pa.Table.from_batches(
                            [batch]
                        ).to_pylist()
                    )

                    transformed = [
                        transform_row(record)
                        for record in source_records
                    ]

                    for record in transformed:
                        for flag in QUALITY_FLAGS:
                            if record[flag]:
                                quality_counts[flag] += 1

                    silver_table = pa.Table.from_pylist(
                        transformed,
                        schema=silver_schema,
                    )

                    writer.write_table(silver_table)

                    written += silver_table.num_rows

        if written != expected_file_rows:
            raise ValueError(
                f"Row mismatch: {source_path.name}"
            )

        # Independently verify that the Silver file
        # preserves every original Bronze value.

        verified = 0

        with pq.ParquetFile(str(source_path)) as original:
            with pq.ParquetFile(str(staged_path)) as silver:

                if silver.metadata.num_rows != written:
                    raise ValueError(
                        f"Parquet rows mismatch: {source_path.name}"
                    )

                if not silver.schema_arrow.equals(
                    silver_schema,
                    check_metadata=False,
                ):
                    raise ValueError(
                        f"Silver schema mismatch: {source_path.name}"
                    )

                original_batches = original.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=bronze_schema.names,
                )

                silver_batches = silver.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=bronze_schema.names,
                )

                for source_batch, silver_batch in zip(
                    original_batches,
                    silver_batches,
                    strict=True,
                ):
                    if not source_batch.equals(
                        silver_batch,
                        check_metadata=False,
                    ):
                        raise ValueError(
                            "Bronze values changed: "
                            f"{source_path.name}"
                        )

                    verified += source_batch.num_rows

        if verified != written:
            raise ValueError(
                f"Original value count mismatch: "
                f"{source_path.name}"
            )

        total_written += written
        total_verified += verified

        print(
            f"[{index}/{EXPECTED_FILES}] "
            f"{source_path.name}: {written:,} rows "
            f"| total {total_written:,}",
            flush=True,
        )

    # ========================================================
    # 8. FINAL STAGING RECONCILIATION
    # ========================================================

    if total_written != EXPECTED_ROWS:
        raise ValueError(
            "Global Silver row count mismatch"
        )

    if total_verified != EXPECTED_ROWS:
        raise ValueError(
            "Global Bronze preservation check failed"
        )

    staged_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    if len(staged_files) != EXPECTED_FILES:
        raise ValueError(
            "Staged Silver file count mismatch"
        )

    staged_rows = 0

    for file in staged_files:
        metadata = pq.read_metadata(file)
        actual_schema = pq.read_schema(file)

        if not actual_schema.equals(
            silver_schema,
            check_metadata=False,
        ):
            raise ValueError(
                f"Final schema mismatch: {file.name}"
            )

        staged_rows += metadata.num_rows

    if staged_rows != EXPECTED_ROWS:
        raise ValueError(
            "Staged Parquet row count mismatch"
        )

    print("\nFINAL STAGING VALIDATION")
    print("-" * 60)

    print("Silver records:", f"{staged_rows:,}")
    print("Silver files:", len(staged_files))
    print("Silver columns:", len(silver_schema))
    print("Original Bronze values preserved: PASS")

    # ========================================================
    # 9. WINDOWS-SAFE PUBLICATION
    # ========================================================

    del staged_files
    gc.collect()

    if SILVER_DIR.exists():
        raise FileExistsError(
            "Final Silver directory appeared during processing"
        )

    # Rename only after all readers and writers close.
    # If publication fails, preserve staging for recovery.
    STAGING_DIR.rename(SILVER_DIR)

    # ========================================================
    # 10. INGESTION MANIFEST
    # ========================================================

    manifest = {
        "project": "AssetFusion360",
        "phase": "Silver",
        "step": "6.5",
        "dataset": "AMPS Transactions",
        "status": "COMPLETED",
        "engine": "PyArrow",
        "orchestration": "Kedro SequentialRunner",
        "bronze_source": (
            BRONZE_DIR.relative_to(ROOT).as_posix()
        ),
        "silver_output": (
            SILVER_DIR.relative_to(ROOT).as_posix()
        ),
        "bronze_rows": EXPECTED_ROWS,
        "silver_rows": total_written,
        "bronze_columns": len(bronze_schema),
        "silver_columns": len(silver_schema),
        "parquet_files": EXPECTED_FILES,
        "original_source_fields_preserved": True,
        "row_count_preserved": True,
        "quality_flag_counts": dict(quality_counts),
        "transformation_notes": [
            "Preserve original transaction business codes.",
            "Preserve mixed-format from/to reference codes.",
            "Keep tra_req null when absent.",
            "Convert tra_updatecount to integer, not date.",
            "Do not delete, deduplicate, or impute records.",
            "Preserve all original Bronze source-lineage fields.",
        ],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST_FILE.open(
        "x",
        encoding="utf-8",
    ) as stream:
        json.dump(
            manifest,
            stream,
            indent=2,
            ensure_ascii=False,
        )

    print("\n" + "=" * 70)
    print("AMPS TRANSACTIONS SILVER SUMMARY")
    print("=" * 70)

    print("Bronze records:", f"{EXPECTED_ROWS:,}")
    print("Silver records:", f"{total_written:,}")
    print("Parquet files:", EXPECTED_FILES)
    print("Bronze columns:", len(bronze_schema))
    print("Silver columns:", len(silver_schema))

    print("\nQUALITY FLAG COUNTS")

    for flag in QUALITY_FLAGS:
        print(f"{flag}: {quality_counts[flag]:,}")

    print("\nTRANSACTIONS SILVER TRANSFORMATION: SUCCESS")

    return manifest


# ============================================================
# 11. KEDRO PIPELINE
# ============================================================

def create_silver_transactions_pipeline():
    return pipeline([
        node(
            func=transform_transactions,
            inputs=None,
            outputs="amps_transactions_silver_manifest",
            name="transform_amps_transactions_silver",
            tags=["silver", "amps", "transactions"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "amps_transactions_silver_manifest": MemoryDataset()
    })

    runner = SequentialRunner()

    runner.run(
        create_silver_transactions_pipeline(),
        catalog,
    )

    result = catalog.load(
        "amps_transactions_silver_manifest"
    )

    if result.get("status") != "COMPLETED":
        raise RuntimeError(
            "Silver Transactions manifest validation failed"
        )

    print("\nKEDRO TRANSACTIONS SILVER PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
