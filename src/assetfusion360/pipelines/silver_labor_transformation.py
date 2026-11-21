
"""
AssetFusion360 Capstone
Step 6.4B - AMPS Labor Silver Transformation

- Preserve every Bronze record and original column.
- Parse labor dates and selected numeric values.
- Preserve negative hours, rates, and costs.
- Add parsing and informational quality flags.
- Validate every Silver Parquet file against Bronze.
- Never overwrite existing outputs.
- Publish only after complete validation.
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
# 1. PROJECT CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

BRONZE_DIR = (
    ROOT / "data" / "02_bronze_ingested_data"
    / "amps_labor"
)

SILVER_ROOT = (
    ROOT / "data" / "03_silver_cleaned_data"
)

SILVER_DIR = SILVER_ROOT / "amps_labor"

STAGING_DIR = (
    SILVER_ROOT / "_staging_amps_labor"
)

MANIFEST_FILE = (
    ROOT / "reports" / "data_profiling_reports"
    / "amps_labor_silver_transformation_manifest.json"
)

EXPECTED_FILES = 647
EXPECTED_ROWS = 3_230_423
EXPECTED_BRONZE_COLUMNS = 22

BATCH_SIZE = 5000


# ============================================================
# 2. SILVER SCHEMA
# ============================================================

TYPED_FIELDS = [
    pa.field("labor_identity_int", pa.int64()),
    pa.field("labor_workorder_id_int", pa.int64()),
    pa.field("labor_date_ts", pa.timestamp("us")),
    pa.field("labor_hours_decimal", pa.decimal128(20, 6)),
    pa.field("labor_rate_decimal", pa.decimal128(20, 6)),
    pa.field("labor_cost_decimal", pa.decimal128(20, 6)),
]

QUALITY_FLAGS = [
    "dq_labor_identity_invalid",
    "dq_workorder_reference_invalid",
    "dq_labor_date_invalid",
    "dq_labor_hours_invalid",
    "dq_labor_rate_invalid",
    "dq_labor_cost_invalid",
    "dq_negative_labor_hours",
    "dq_negative_labor_cost",
    "dq_zero_labor_rate",
    "dq_any_issue",
]

QUALITY_FIELDS = [
    pa.field(name, pa.bool_())
    for name in QUALITY_FLAGS
]

ADDED_FIELDS = TYPED_FIELDS + QUALITY_FIELDS

EXPECTED_SILVER_COLUMNS = (
    EXPECTED_BRONZE_COLUMNS + len(ADDED_FIELDS)
)

assert EXPECTED_SILVER_COLUMNS == 38


# ============================================================
# 3. VALUE PARSING
# ============================================================

def missing(value):
    return (
        value is None
        or str(value).strip() == ""
    )


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

        # Decimal(20,6) has 14 integer digits.
        if abs(number) >= Decimal("100000000000000"):
            return None, True

        return number, False

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


@lru_cache(maxsize=100000)
def parse_date_cached(text):
    normalized = re.sub(
        r"\s+",
        " ",
        text.strip()
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
                normalized,
                fmt
            )
        except ValueError:
            continue

    return None


def parse_date(value):
    if missing(value):
        return None, False

    parsed = parse_date_cached(
        str(value)
    )

    return parsed, parsed is None


# ============================================================
# 4. TRANSFORM ONE RECORD
# ============================================================

def transform_row(source):
    result = dict(source)

    identity, identity_invalid = parse_integer(
        source.get("boo_sqlidentity")
    )

    event, event_invalid = parse_integer(
        source.get("boo_event")
    )

    labor_date, date_invalid = parse_date(
        source.get("boo_date")
    )

    hours, hours_invalid = parse_decimal(
        source.get("boo_hours")
    )

    rate, rate_invalid = parse_decimal(
        source.get("boo_rate")
    )

    cost, cost_invalid = parse_decimal(
        source.get("boo_cost")
    )

    result.update({
        "labor_identity_int": identity,
        "labor_workorder_id_int": event,
        "labor_date_ts": labor_date,
        "labor_hours_decimal": hours,
        "labor_rate_decimal": rate,
        "labor_cost_decimal": cost,
    })

    flags = {
        "dq_labor_identity_invalid": (
            identity_invalid
            or missing(source.get("boo_sqlidentity"))
        ),
        "dq_workorder_reference_invalid": (
            event_invalid
            or missing(source.get("boo_event"))
        ),
        "dq_labor_date_invalid": date_invalid,
        "dq_labor_hours_invalid": hours_invalid,
        "dq_labor_rate_invalid": rate_invalid,
        "dq_labor_cost_invalid": cost_invalid,
        "dq_negative_labor_hours": (
            hours is not None and hours < 0
        ),
        "dq_negative_labor_cost": (
            cost is not None and cost < 0
        ),
        "dq_zero_labor_rate": (
            rate is not None and rate == 0
        ),
    }

    # Distinguish data errors from valid financial
    # reversals and zero-rate transactions.
    error_flags = [
        "dq_labor_identity_invalid",
        "dq_workorder_reference_invalid",
        "dq_labor_date_invalid",
        "dq_labor_hours_invalid",
        "dq_labor_rate_invalid",
        "dq_labor_cost_invalid",
    ]

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
                "Existing output must be preserved: "
                f"{path}"
            )


# ============================================================
# 6. INSPECT BRONZE
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

    required_columns = [
        "boo_event",
        "boo_sqlidentity",
        "boo_date",
        "boo_hours",
        "boo_rate",
        "boo_cost",
    ]

    for field in required_columns:
        if field not in schema.names:
            raise ValueError(
                f"Missing Bronze field: {field}"
            )

    for field in schema:
        if not field.type.equals(pa.string()):
            raise ValueError(
                f"Unexpected field type: {field.name}"
            )

    for new_field in ADDED_FIELDS:
        if new_field.name in schema.names:
            raise ValueError(
                f"Silver field collision: {new_field.name}"
            )

    total_rows = 0

    for file in files:
        metadata = pq.read_metadata(file)
        actual_schema = pq.read_schema(file)

        if not actual_schema.equals(
            schema,
            check_metadata=False,
        ):
            raise ValueError(
                f"Bronze schema mismatch: {file.name}"
            )

        total_rows += metadata.num_rows

    if total_rows != EXPECTED_ROWS:
        raise ValueError(
            f"Bronze rows mismatch: {total_rows}"
        )

    print("Bronze files:", len(files))
    print("Bronze records:", f"{total_rows:,}")
    print("Bronze columns:", len(schema))
    print("Bronze validation: PASS")

    return files, schema


# ============================================================
# 7. MAIN SILVER TRANSFORMATION
# ============================================================

def transform_labor():
    print("=" * 70)
    print("STEP 6.4B - AMPS LABOR SILVER")
    print("=" * 70)

    check_outputs()

    files, bronze_schema = inspect_sources()

    silver_schema = pa.schema(
        list(bronze_schema) + ADDED_FIELDS
    )

    quality_counts = Counter({
        flag: 0
        for flag in QUALITY_FLAGS
    })

    total_written = 0
    total_verified = 0

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False,
    )

    # One input Parquet produces one output Parquet.
    for index, source_path in enumerate(files, 1):

        staging_file = (
            STAGING_DIR / source_path.name
        )

        written = 0

        # Close reader and writer before validation.
        with pq.ParquetFile(str(source_path)) as reader:
            expected_file_rows = reader.metadata.num_rows

            with pq.ParquetWriter(
                str(staging_file),
                silver_schema,
                compression="snappy",
                version="2.6",
            ) as writer:

                for batch in reader.iter_batches(
                    batch_size=BATCH_SIZE
                ):
                    input_table = (
                        pa.Table.from_batches([batch])
                    )

                    source_records = (
                        input_table.to_pylist()
                    )

                    converted_records = [
                        transform_row(record)
                        for record in source_records
                    ]

                    for record in converted_records:
                        for flag in QUALITY_FLAGS:
                            if record[flag]:
                                quality_counts[flag] += 1

                    silver_table = (
                        pa.Table.from_pylist(
                            converted_records,
                            schema=silver_schema,
                        )
                    )

                    writer.write_table(
                        silver_table
                    )

                    written += silver_table.num_rows

        if written != expected_file_rows:
            raise ValueError(
                f"Silver row mismatch: {source_path.name}"
            )

        # Validate the actual published-format file,
        # including original Bronze field equality.

        file_verified = 0

        with pq.ParquetFile(
            str(source_path)
        ) as source_check:

            with pq.ParquetFile(
                str(staging_file)
            ) as silver_check:

                if silver_check.metadata.num_rows != written:
                    raise ValueError(
                        f"Parquet row mismatch: {source_path.name}"
                    )

                if not silver_check.schema_arrow.equals(
                    silver_schema,
                    check_metadata=False,
                ):
                    raise ValueError(
                        f"Schema mismatch: {source_path.name}"
                    )

                original_batches = (
                    source_check.iter_batches(
                        batch_size=BATCH_SIZE,
                        columns=bronze_schema.names,
                    )
                )

                preserved_batches = (
                    silver_check.iter_batches(
                        batch_size=BATCH_SIZE,
                        columns=bronze_schema.names,
                    )
                )

                for original, preserved in zip(
                    original_batches,
                    preserved_batches,
                    strict=True,
                ):
                    if not original.equals(
                        preserved,
                        check_metadata=False,
                    ):
                        raise ValueError(
                            "Bronze values changed in "
                            f"{source_path.name}"
                        )

                    file_verified += original.num_rows

        if file_verified != written:
            raise ValueError(
                f"Original-value validation failed: "
                f"{source_path.name}"
            )

        total_written += written
        total_verified += file_verified

        print(
            f"[{index}/{EXPECTED_FILES}] "
            f"{source_path.name}: {written:,} rows "
            f"| total {total_written:,}",
            flush=True,
        )

    # ========================================================
    # 8. GLOBAL RECONCILIATION
    # ========================================================

    if total_written != EXPECTED_ROWS:
        raise ValueError(
            "Global Silver record reconciliation failed"
        )

    if total_verified != EXPECTED_ROWS:
        raise ValueError(
            "Original Bronze value reconciliation failed"
        )

    staged_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    if len(staged_files) != EXPECTED_FILES:
        raise ValueError(
            "Silver Parquet inventory mismatch"
        )

    print("\nFINAL SILVER STAGING VALIDATION")
    print("-" * 60)

    print("Silver files:", len(staged_files))
    print("Silver records:", f"{total_written:,}")
    print("Silver columns:", len(silver_schema))
    print("Original Bronze values preserved: PASS")

    # ========================================================
    # 9. WINDOWS-SAFE PUBLICATION
    # ========================================================

    # Release references before renaming.
    # Do not remove staging if publication fails.
    del staged_files
    gc.collect()

    if SILVER_DIR.exists():
        raise FileExistsError(
            "Final Silver directory appeared during execution"
        )

    STAGING_DIR.rename(SILVER_DIR)

    # ========================================================
    # 10. WRITE MANIFEST
    # ========================================================

    manifest = {
        "project": "AssetFusion360",
        "phase": "Silver",
        "step": "6.4",
        "dataset": "AMPS Labor",
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
        "financial_value_policy": (
            "Preserve negative hours/costs and "
            "zero rates; flag for later investigation."
        ),
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
    ) as handle:
        json.dump(
            manifest,
            handle,
            indent=2,
            ensure_ascii=False,
        )

    print("\n" + "=" * 70)
    print("AMPS LABOR SILVER SUMMARY")
    print("=" * 70)

    print("Bronze records:", f"{EXPECTED_ROWS:,}")
    print("Silver records:", f"{total_written:,}")
    print("Parquet files:", EXPECTED_FILES)
    print("Bronze columns:", len(bronze_schema))
    print("Silver columns:", len(silver_schema))

    print("\nQUALITY FLAG COUNTS")

    for flag in QUALITY_FLAGS:
        print(
            f"{flag}: {quality_counts[flag]:,}"
        )

    print("\nLABOR SILVER TRANSFORMATION: SUCCESS")

    return manifest


# ============================================================
# 11. KEDRO
# ============================================================

def create_silver_labor_pipeline():
    return pipeline([
        node(
            func=transform_labor,
            inputs=None,
            outputs="amps_labor_silver_manifest",
            name="transform_amps_labor_silver",
            tags=["silver", "amps", "labor"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "amps_labor_silver_manifest": MemoryDataset()
    })

    runner = SequentialRunner()

    runner.run(
        create_silver_labor_pipeline(),
        catalog,
    )

    result = catalog.load(
        "amps_labor_silver_manifest"
    )

    if result.get("status") != "COMPLETED":
        raise RuntimeError(
            "Silver Labor manifest validation failed"
        )

    print("\nKEDRO LABOR SILVER PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
