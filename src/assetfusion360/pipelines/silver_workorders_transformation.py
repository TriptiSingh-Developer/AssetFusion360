
"""
AssetFusion360 - Step 6.3B
AMPS Work Orders Bronze-to-Silver Transformation

Preserves all rows and original fields.
Does not modify Bronze.
Does not overwrite existing Silver outputs.
Uses PyArrow, Kedro and Windows-safe file handling.
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


ROOT = Path(__file__).resolve().parents[3]

BRONZE_DIR = (
    ROOT / "data" / "02_bronze_ingested_data"
    / "amps_workorders"
)

SILVER_ROOT = ROOT / "data" / "03_silver_cleaned_data"
SILVER_DIR = SILVER_ROOT / "amps_workorders"
STAGING_DIR = SILVER_ROOT / "_staging_amps_workorders"

MANIFEST_FILE = (
    ROOT / "reports" / "data_profiling_reports"
    / "amps_workorders_silver_transformation_manifest.json"
)

EXPECTED_FILES = 447
EXPECTED_ROWS = 2_231_535
EXPECTED_BRONZE_COLUMNS = 59
BATCH_SIZE = 5000

DATE_FIELDS = [
    "evt_date",
    "evt_reported",
    "evt_start",
    "evt_due",
    "evt_completed",
    "evt_created",
    "evt_updated",
]

DATE_OUTPUTS = {
    field: f"{field}_ts"
    for field in DATE_FIELDS
}

DATE_FLAGS = {
    field: f"dq_{field}_invalid"
    for field in DATE_FIELDS
}

NUMERIC_FIELDS = {
    "evt_sqlidentity": (
        "wo_identity_int",
        "dq_identity_invalid",
    ),
    "evt_duration": (
        "wo_duration_decimal",
        "dq_duration_invalid",
    ),
    "evt_updatecount": (
        "wo_updatecount_int",
        "dq_updatecount_invalid",
    ),
}

OTHER_FLAGS = [
    "dq_workorder_code_missing",
    "dq_asset_ref_missing",
    "dq_any_issue",
]

ALL_FLAGS = (
    list(DATE_FLAGS.values())
    + [config[1] for config in NUMERIC_FIELDS.values()]
    + OTHER_FLAGS
)

ADDED_FIELDS = (
    [
        pa.field(output, pa.timestamp("us"))
        for output in DATE_OUTPUTS.values()
    ]
    + [
        pa.field("wo_identity_int", pa.int64()),
        pa.field("wo_duration_decimal", pa.decimal128(20, 6)),
        pa.field("wo_updatecount_int", pa.int64()),
    ]
    + [
        pa.field(flag, pa.bool_())
        for flag in ALL_FLAGS
    ]
)

EXPECTED_SILVER_COLUMNS = (
    EXPECTED_BRONZE_COLUMNS + len(ADDED_FIELDS)
)

assert EXPECTED_SILVER_COLUMNS == 82


def missing(value):
    return value is None or str(value).strip() == ""


def parse_int(value):
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


def parse_decimal(value):
    if missing(value):
        return None, False

    try:
        result = Decimal(str(value).strip())

        if not result.is_finite():
            return None, True

        result = result.quantize(
            Decimal("0.000001")
        )

        if abs(result) >= Decimal("100000000000000"):
            return None, True

        return result, False

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


@lru_cache(maxsize=100000)
def parse_date_cached(text):
    normalized = re.sub(r"\s+", " ", text.strip())

    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            pass

    return None


def parse_date(value):
    if missing(value):
        return None, False

    result = parse_date_cached(str(value))
    return result, result is None


def transform_row(source):
    result = dict(source)
    flags = {}

    for field in DATE_FIELDS:
        converted, invalid = parse_date(
            source.get(field)
        )

        result[DATE_OUTPUTS[field]] = converted
        flags[DATE_FLAGS[field]] = invalid

    identity, identity_invalid = parse_int(
        source.get("evt_sqlidentity")
    )

    duration, duration_invalid = parse_decimal(
        source.get("evt_duration")
    )

    updatecount, updatecount_invalid = parse_int(
        source.get("evt_updatecount")
    )

    result["wo_identity_int"] = identity
    result["wo_duration_decimal"] = duration
    result["wo_updatecount_int"] = updatecount

    flags["dq_identity_invalid"] = (
        identity_invalid
        or missing(source.get("evt_sqlidentity"))
    )

    flags["dq_duration_invalid"] = duration_invalid
    flags["dq_updatecount_invalid"] = updatecount_invalid

    flags["dq_workorder_code_missing"] = missing(
        source.get("evt_code")
    )

    flags["dq_asset_ref_missing"] = missing(
        source.get("evt_object")
    )

    flags["dq_any_issue"] = any(flags.values())

    result.update(flags)
    return result


def check_outputs():
    if not BRONZE_DIR.is_dir():
        raise FileNotFoundError(BRONZE_DIR)

    for path in [
        SILVER_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
    ]:
        if path.exists():
            raise FileExistsError(
                f"Existing output must be preserved: {path}"
            )


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
        raise ValueError("Unexpected Bronze schema")

    required = [
        "evt_code",
        "evt_object",
        *DATE_FIELDS,
        *NUMERIC_FIELDS.keys(),
    ]

    for field in required:
        if field not in schema.names:
            raise ValueError(
                f"Missing field: {field}"
            )

    if any(
        not f.type.equals(pa.string())
        for f in schema
    ):
        raise ValueError(
            "Bronze fields must have string types"
        )

    if any(
        field.name in schema.names
        for field in ADDED_FIELDS
    ):
        raise ValueError(
            "Silver field name collision"
        )

    total = 0

    for path in files:
        metadata = pq.read_metadata(path)
        file_schema = pq.read_schema(path)

        if not schema.equals(
            file_schema,
            check_metadata=False,
        ):
            raise ValueError(
                f"Schema mismatch: {path.name}"
            )

        total += metadata.num_rows

    if total != EXPECTED_ROWS:
        raise ValueError(
            f"Bronze row mismatch: {total}"
        )

    print("Source files:", len(files))
    print("Source records:", f"{total:,}")
    print("Source schema: PASS")

    return files, schema


def transform_workorders():
    print("=" * 70)
    print("STEP 6.3 - AMPS WORK ORDERS SILVER")
    print("=" * 70)

    check_outputs()
    files, source_schema = inspect_sources()

    silver_schema = pa.schema(
        list(source_schema) + ADDED_FIELDS
    )

    quality_counts = Counter({
        flag: 0 for flag in ALL_FLAGS
    })

    total_written = 0
    total_verified = 0

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False,
    )

    for file_number, source_path in enumerate(files, 1):
        staged_path = (
            STAGING_DIR / source_path.name
        )

        source_reader = pq.ParquetFile(
            str(source_path)
        )

        expected_batch_rows = (
            source_reader.metadata.num_rows
        )

        written = 0

        try:
            with pq.ParquetWriter(
                str(staged_path),
                silver_schema,
                compression="snappy",
                version="2.6",
            ) as writer:

                for batch in source_reader.iter_batches(
                    batch_size=BATCH_SIZE
                ):
                    source_table = pa.Table.from_batches(
                        [batch]
                    )

                    records = source_table.to_pylist()

                    silver_records = [
                        transform_row(row)
                        for row in records
                    ]

                    for record in silver_records:
                        for flag in ALL_FLAGS:
                            if record[flag]:
                                quality_counts[flag] += 1

                    silver_table = pa.Table.from_pylist(
                        silver_records,
                        schema=silver_schema,
                    )

                    writer.write_table(
                        silver_table
                    )

                    written += silver_table.num_rows

        finally:
            source_reader.close()

        if written != expected_batch_rows:
            raise ValueError(
                f"Row mismatch: {source_path.name}"
            )

        # Validate output on disk and verify all
        # original Bronze values are unchanged.

        source_check = pq.ParquetFile(
            str(source_path)
        )

        silver_check = pq.ParquetFile(
            str(staged_path)
        )

        try:
            if silver_check.metadata.num_rows != written:
                raise ValueError(
                    f"Parquet row mismatch: {source_path.name}"
                )

            if not silver_check.schema_arrow.equals(
                silver_schema,
                check_metadata=False,
            ):
                raise ValueError(
                    f"Silver schema mismatch: {source_path.name}"
                )

            source_batches = source_check.iter_batches(
                batch_size=BATCH_SIZE
            )

            output_batches = silver_check.iter_batches(
                batch_size=BATCH_SIZE,
                columns=source_schema.names,
            )

            file_verified = 0

            for original, output in zip(
                source_batches,
                output_batches,
                strict=True,
            ):
                if not original.equals(
                    output,
                    check_metadata=False,
                ):
                    raise ValueError(
                        "Bronze source values changed: "
                        f"{source_path.name}"
                    )

                file_verified += original.num_rows

            if file_verified != written:
                raise ValueError(
                    f"Verification count mismatch: {source_path.name}"
                )

            total_verified += file_verified

        finally:
            source_check.close()
            silver_check.close()

        total_written += written

        print(
            f"[{file_number}/{EXPECTED_FILES}] "
            f"{source_path.name}: "
            f"{written:,} rows | "
            f"total {total_written:,}",
            flush=True,
        )

    if total_written != EXPECTED_ROWS:
        raise ValueError(
            "Global Silver row count mismatch"
        )

    if total_verified != EXPECTED_ROWS:
        raise ValueError(
            "Global Bronze preservation check failed"
        )

    staged_files = list(
        STAGING_DIR.glob("*.parquet")
    )

    if len(staged_files) != EXPECTED_FILES:
        raise ValueError(
            "Staged file inventory mismatch"
        )

    print("\nFINAL STAGING VALIDATION")
    print("Silver records:", f"{total_written:,}")
    print("Silver files:", len(staged_files))
    print("Silver columns:", len(silver_schema))
    print("Original Bronze values preserved: PASS")

    # Explicitly release file handles before moving
    # the directory. Important on Windows.
    gc.collect()

    if SILVER_DIR.exists():
        raise FileExistsError(
            "Final Silver directory appeared unexpectedly"
        )

    STAGING_DIR.rename(SILVER_DIR)

    manifest = {
        "project": "AssetFusion360",
        "phase": "Silver",
        "step": "6.3",
        "dataset": "AMPS Work Orders",
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
        "bronze_columns": len(source_schema),
        "silver_columns": len(silver_schema),
        "parquet_files": EXPECTED_FILES,
        "original_source_fields_preserved": True,
        "row_count_preserved": True,
        "quality_flag_counts": dict(quality_counts),
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
        )

    print("\n" + "=" * 70)
    print("WORK ORDERS SILVER SUMMARY")
    print("=" * 70)
    print("Bronze rows:", f"{EXPECTED_ROWS:,}")
    print("Silver rows:", f"{total_written:,}")
    print("Parquet files:", EXPECTED_FILES)
    print("Original columns:", len(source_schema))
    print("Silver columns:", len(silver_schema))

    print("\nQUALITY FLAGS")
    for flag, count in quality_counts.items():
        print(f"{flag}: {count:,}")

    print("\nWORK ORDERS SILVER TRANSFORMATION: SUCCESS")

    return manifest


def create_silver_workorders_pipeline():
    return pipeline([
        node(
            func=transform_workorders,
            inputs=None,
            outputs="amps_workorders_silver_manifest",
            name="transform_amps_workorders_silver",
            tags=["silver", "amps", "workorders"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "amps_workorders_silver_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    runner.run(
        create_silver_workorders_pipeline(),
        catalog,
    )

    result = catalog.load(
        "amps_workorders_silver_manifest"
    )

    if result.get("status") != "COMPLETED":
        raise RuntimeError(
            "Silver manifest validation failed"
        )

    print("\nKEDRO WORK ORDERS SILVER PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
