
"""
AssetFusion360 Capstone
Step 6.6B - AMPS Transaction Details Silver

Preserves all 2,301,927 records and 35 original fields.

Adds:
    5 nullable integer fields
    6 nullable timestamp fields
    3 nullable decimal fields
    19 quality flags

Negative amounts and quantities are preserved.
No files are overwritten or deleted.
All original Bronze values are verified.
"""

import gc
import json
import re

from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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
    / "amps_transaction_details"
)

SILVER_ROOT = (
    ROOT / "data" / "03_silver_cleaned_data"
)

SILVER_DIR = (
    SILVER_ROOT / "amps_transaction_details"
)

STAGING_DIR = (
    SILVER_ROOT / "_staging_amps_transaction_details"
)

MANIFEST_FILE = (
    ROOT / "reports" / "data_profiling_reports"
    / "amps_transaction_details_silver_transformation_manifest.json"
)

EXPECTED_FILES = 461
EXPECTED_ROWS = 2_301_927
EXPECTED_BRONZE_COLUMNS = 35
BATCH_SIZE = 5000


# ============================================================
# 2. NEW SILVER COLUMNS
# ============================================================

INTEGER_FIELDS = {
    "trl_trans": "detail_transaction_id_int",
    "trl_event": "detail_workorder_id_int",
    "trl_sqlidentity": "detail_identity_int",
    "par_sqlidentity": "part_identity_int",
    "par_updatecount": "part_updatecount_int",
}

DATE_FIELDS = {
    "trl_date": "detail_date_ts",
    "trl_created": "detail_created_ts",
    "trl_updated": "detail_updated_ts",
    "par_created": "part_created_ts",
    "par_updated": "part_updated_ts",
    "par_laststatusupdate": "part_laststatusupdate_ts",
}

DECIMAL_FIELDS = {
    "trl_qty": "detail_quantity_decimal",
    "trl_price": "detail_price_decimal",
    "trl_avgprice": "detail_avgprice_decimal",
}

INTEGER_FLAGS = {
    source: f"dq_{source}_invalid"
    for source in INTEGER_FIELDS
}

DATE_FLAGS = {
    source: f"dq_{source}_invalid"
    for source in DATE_FIELDS
}

DECIMAL_FLAGS = {
    source: f"dq_{source}_invalid"
    for source in DECIMAL_FIELDS
}

INFORMATION_FLAGS = [
    "dq_negative_quantity",
    "dq_negative_price",
    "dq_zero_quantity",
    "dq_zero_price",
]

QUALITY_FLAGS = (
    list(INTEGER_FLAGS.values())
    + list(DATE_FLAGS.values())
    + list(DECIMAL_FLAGS.values())
    + INFORMATION_FLAGS
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
    + [
        pa.field(name, pa.decimal128(20, 6))
        for name in DECIMAL_FIELDS.values()
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

assert len(TYPED_FIELDS) == 14
assert len(QUALITY_FLAGS) == 19
assert EXPECTED_SILVER_COLUMNS == 68


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
        number = int(text)
    except ValueError:
        return None, True

    if not -(2**63) <= number < 2**63:
        return None, True

    return number, False


def parse_decimal(value):
    if missing(value):
        return None, False

    try:
        number = Decimal(str(value).strip())

        if not number.is_finite():
            return None, True

        number = number.quantize(
            Decimal("0.000001")
        )

        if abs(number) >= Decimal("100000000000000"):
            return None, True

        return number, False

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


@lru_cache(maxsize=100000)
def parse_date_cached(text):
    text = re.sub(r"\s+", " ", text.strip())

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
            return datetime.strptime(text, fmt)
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

    for original, target in DECIMAL_FIELDS.items():
        value, invalid = parse_decimal(
            source.get(original)
        )

        result[target] = value
        flags[DECIMAL_FLAGS[original]] = invalid

    # Required identifiers must be present.
    # trl_event and par_* identifiers can be null.
    for field in [
        "trl_trans",
        "trl_sqlidentity",
    ]:
        if missing(source.get(field)):
            flags[INTEGER_FLAGS[field]] = True

    qty = result["detail_quantity_decimal"]
    price = result["detail_price_decimal"]

    flags["dq_negative_quantity"] = (
        qty is not None and qty < 0
    )
    flags["dq_negative_price"] = (
        price is not None and price < 0
    )
    flags["dq_zero_quantity"] = (
        qty is not None and qty == 0
    )
    flags["dq_zero_price"] = (
        price is not None and price == 0
    )

    # Informational financial flags do not count
    # as parsing or required-field failures.
    error_flags = (
        list(INTEGER_FLAGS.values())
        + list(DATE_FLAGS.values())
        + list(DECIMAL_FLAGS.values())
    )

    flags["dq_any_issue"] = any(
        flags[name] for name in error_flags
    )

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

    required = (
        list(INTEGER_FIELDS)
        + list(DATE_FIELDS)
        + list(DECIMAL_FIELDS)
    )

    for field in required:
        if field not in schema.names:
            raise ValueError(
                f"Missing Bronze field: {field}"
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
                f"Bronze schema mismatch: {path.name}"
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

def transform_transaction_details():
    print("=" * 70)
    print("STEP 6.6B - TRANSACTION DETAILS SILVER")
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
        exist_ok=False
    )

    for index, source_path in enumerate(files, 1):
        staged_path = STAGING_DIR / source_path.name
        written = 0

        # Transform one Bronze file at a time.
        # Readers/writers close before verification.

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
                    original_records = (
                        pa.Table.from_batches(
                            [batch]
                        ).to_pylist()
                    )

                    silver_records = [
                        transform_row(record)
                        for record in original_records
                    ]

                    for record in silver_records:
                        for flag in QUALITY_FLAGS:
                            if record[flag]:
                                quality_counts[flag] += 1

                    silver_table = pa.Table.from_pylist(
                        silver_records,
                        schema=silver_schema,
                    )

                    writer.write_table(silver_table)

                    written += silver_table.num_rows

        if written != expected_file_rows:
            raise ValueError(
                f"Row mismatch: {source_path.name}"
            )

        # Verify actual staged Parquet.
        # Compare every original field and row
        # against the corresponding Bronze file.

        verified = 0

        with pq.ParquetFile(str(source_path)) as original:
            with pq.ParquetFile(str(staged_path)) as silver:

                if silver.metadata.num_rows != written:
                    raise ValueError(
                        f"Parquet row mismatch: {source_path.name}"
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
                            "Original Bronze values changed: "
                            f"{source_path.name}"
                        )

                    verified += source_batch.num_rows

        if verified != written:
            raise ValueError(
                f"Original-value verification failed: "
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
    # 8. GLOBAL STAGING VALIDATION
    # ========================================================

    if total_written != EXPECTED_ROWS:
        raise ValueError(
            "Global Silver row reconciliation failed"
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

    for path in staged_files:
        metadata = pq.read_metadata(path)
        actual_schema = pq.read_schema(path)

        if not actual_schema.equals(
            silver_schema,
            check_metadata=False,
        ):
            raise ValueError(
                f"Staged schema mismatch: {path.name}"
            )

        staged_rows += metadata.num_rows

    if staged_rows != EXPECTED_ROWS:
        raise ValueError(
            "Staged Parquet row total mismatch"
        )

    print("\nFINAL STAGING VALIDATION")
    print("-" * 60)
    print("Silver files:", len(staged_files))
    print("Silver records:", f"{staged_rows:,}")
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

    # If Windows denies the rename, preserve staging.
    # Do not rerun or delete the transformed files.
    STAGING_DIR.rename(SILVER_DIR)

    # ========================================================
    # 10. COMPLETION MANIFEST
    # ========================================================

    manifest = {
        "project": "AssetFusion360",
        "phase": "Silver",
        "step": "6.6",
        "dataset": "AMPS Transaction Details",
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
            "Preserve original Bronze fields and lineage.",
            "Retain optional work-order references.",
            "Retain optional part attributes.",
            "Parse six date/timestamp fields.",
            "Convert quantities and prices to decimal(20,6).",
            "Preserve negative quantities and prices.",
            "Keep parsing failures as null with flags.",
            "Do not filter or deduplicate records.",
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
    print("AMPS TRANSACTION DETAILS SILVER SUMMARY")
    print("=" * 70)

    print("Bronze records:", f"{EXPECTED_ROWS:,}")
    print("Silver records:", f"{total_written:,}")
    print("Parquet files:", EXPECTED_FILES)
    print("Bronze columns:", len(bronze_schema))
    print("Silver columns:", len(silver_schema))

    print("\nQUALITY FLAG COUNTS")

    for flag in QUALITY_FLAGS:
        print(f"{flag}: {quality_counts[flag]:,}")

    print(
        "\nTRANSACTION DETAILS SILVER TRANSFORMATION: SUCCESS"
    )

    return manifest


# ============================================================
# 11. KEDRO PIPELINE
# ============================================================

def create_silver_transaction_details_pipeline():
    return pipeline([
        node(
            func=transform_transaction_details,
            inputs=None,
            outputs="amps_transaction_details_silver_manifest",
            name="transform_amps_transaction_details_silver",
            tags=["silver", "amps", "transaction_details"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "amps_transaction_details_silver_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    runner.run(
        create_silver_transaction_details_pipeline(),
        catalog,
    )

    result = catalog.load(
        "amps_transaction_details_silver_manifest"
    )

    if result.get("status") != "COMPLETED":
        raise RuntimeError(
            "Transaction Details Silver manifest validation failed"
        )

    print(
        "\nKEDRO TRANSACTION DETAILS SILVER PIPELINE: SUCCESS"
    )


if __name__ == "__main__":
    run_kedro_pipeline()
