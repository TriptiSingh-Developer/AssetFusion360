
"""
AssetFusion360 Capstone
Step 6.2 - AMPS Assets Silver Transformation

Architecture:
    Bronze Parquet -> PyArrow streaming transformation
    -> Validated Silver Parquet -> Kedro manifest

Rules:
    - Never modify Bronze files.
    - Never overwrite existing Silver files.
    - Preserve all 148,522 asset records.
    - Preserve all 32 original Bronze fields.
    - Add typed fields and data-quality indicators.
    - Retain nulls and malformed source values.
    - Do not deduplicate, filter, or impute.
    - Validate record counts and schemas.
"""

import hashlib
import json
import re

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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

BRONZE_FILE = (
    ROOT
    / "data"
    / "02_bronze_ingested_data"
    / "amps_assets"
    / "part-00000.parquet"
)

SILVER_ROOT = (
    ROOT / "data" / "03_silver_cleaned_data"
)

SILVER_DIR = SILVER_ROOT / "amps_assets"

STAGING_DIR = (
    SILVER_ROOT / "_staging_amps_assets"
)

SILVER_FILE = (
    SILVER_DIR / "part-00000.parquet"
)

MANIFEST_FILE = (
    ROOT
    / "reports"
    / "data_profiling_reports"
    / "amps_assets_silver_transformation_manifest.json"
)

EXPECTED_ROWS = 148_522
EXPECTED_BRONZE_COLUMNS = 32

BATCH_SIZE = 5000

REQUIRED_COLUMNS = [
    "OBJ_CODE",
    "OBJ_SQLIDENTITY",
    "OBJ_COMMISS",
    "OBJ_CREATED",
    "OBJ_VALUE",
]

NEW_FIELDS = [
    pa.field(
        "asset_identity_int",
        pa.int64()
    ),
    pa.field(
        "asset_commissioned_ts",
        pa.timestamp("us")
    ),
    pa.field(
        "asset_created_ts",
        pa.timestamp("us")
    ),
    pa.field(
        "asset_value_decimal",
        pa.decimal128(20, 6)
    ),
    pa.field(
        "dq_asset_code_missing",
        pa.bool_()
    ),
    pa.field(
        "dq_asset_code_placeholder",
        pa.bool_()
    ),
    pa.field(
        "dq_asset_identity_invalid",
        pa.bool_()
    ),
    pa.field(
        "dq_commission_date_invalid",
        pa.bool_()
    ),
    pa.field(
        "dq_created_date_invalid",
        pa.bool_()
    ),
    pa.field(
        "dq_asset_value_invalid",
        pa.bool_()
    ),
    pa.field(
        "dq_any_issue",
        pa.bool_()
    ),
]


# ============================================================
# 2. GENERAL UTILITIES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def relative(path):
    return path.relative_to(ROOT).as_posix()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def is_missing(value):
    return (
        value is None
        or str(value).strip() == ""
    )


def parse_integer(value):
    if is_missing(value):
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
    if is_missing(value):
        return None, False

    try:
        result = Decimal(str(value).strip())

        if not result.is_finite():
            return None, True

        result = result.quantize(
            Decimal("0.000001")
        )

        # Decimal(20,6) allows at most
        # 14 digits before the decimal point.
        if abs(result) >= Decimal("100000000000000"):
            return None, True

        return result, False

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


# ============================================================
# 3. DATE PARSING
# ============================================================

def parse_timestamp(value):
    if is_missing(value):
        return None, False

    text = str(value).strip()

    # Source dates include repeated spaces,
    # such as "Feb 20 2009  9:36AM".

    text = re.sub(r"\s+", " ", text)

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for date_format in formats:
        try:
            parsed = datetime.strptime(
                text,
                date_format
            )

            return parsed, False

        except ValueError:
            continue

    return None, True


# ============================================================
# 4. PROTECT EXISTING FILES
# ============================================================

def check_safety():
    if not BRONZE_FILE.is_file():
        raise FileNotFoundError(
            f"Bronze file missing: {BRONZE_FILE}"
        )

    for path in [
        SILVER_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
    ]:
        if path.exists():
            raise FileExistsError(
                "Existing Silver artifact detected. "
                f"Will not overwrite: {path}"
            )

    print("Silver output safety: PASS")
    print("Bronze source protection: PASS")


# ============================================================
# 5. READ AND VALIDATE BRONZE
# ============================================================

def inspect_bronze():
    bronze = pq.ParquetFile(
        str(BRONZE_FILE)
    )

    schema = bronze.schema_arrow

    if bronze.metadata.num_rows != EXPECTED_ROWS:
        raise ValueError(
            "Bronze record count mismatch."
        )

    if len(schema) != EXPECTED_BRONZE_COLUMNS:
        raise ValueError(
            "Bronze column count mismatch."
        )

    for column in REQUIRED_COLUMNS:
        if column not in schema.names:
            raise ValueError(
                f"Missing required Bronze column: {column}"
            )

    for field in NEW_FIELDS:
        if field.name in schema.names:
            raise ValueError(
                f"Silver field already exists: {field.name}"
            )

    if any(
        not field.type.equals(pa.string())
        for field in schema
    ):
        raise ValueError(
            "Unexpected Bronze field type."
        )

    print("Bronze records:", bronze.metadata.num_rows)
    print("Bronze columns:", len(schema))
    print("Bronze schema: PASS")

    return bronze, schema


# ============================================================
# 6. TRANSFORM ONE RECORD
# ============================================================

def transform_record(record):
    result = dict(record)

    asset_code = record.get("OBJ_CODE")

    identity, identity_invalid = parse_integer(
        record.get("OBJ_SQLIDENTITY")
    )

    commissioned, commissioned_invalid = (
        parse_timestamp(
            record.get("OBJ_COMMISS")
        )
    )

    created, created_invalid = parse_timestamp(
        record.get("OBJ_CREATED")
    )

    value, value_invalid = parse_decimal(
        record.get("OBJ_VALUE")
    )

    code_missing = is_missing(asset_code)

    code_placeholder = (
        not code_missing
        and str(asset_code).strip().upper()
        in {"*", "N/A", "NA", "NULL", "UNKNOWN"}
    )

    # An absent identifier is an issue,
    # distinct from a parsing failure.
    identity_missing = is_missing(
        record.get("OBJ_SQLIDENTITY")
    )

    any_issue = any([
        code_missing,
        code_placeholder,
        identity_invalid,
        identity_missing,
        commissioned_invalid,
        created_invalid,
        value_invalid,
    ])

    result.update({
        "asset_identity_int": identity,
        "asset_commissioned_ts": commissioned,
        "asset_created_ts": created,
        "asset_value_decimal": value,
        "dq_asset_code_missing": code_missing,
        "dq_asset_code_placeholder": code_placeholder,
        "dq_asset_identity_invalid": (
            identity_invalid or identity_missing
        ),
        "dq_commission_date_invalid": (
            commissioned_invalid
        ),
        "dq_created_date_invalid": created_invalid,
        "dq_asset_value_invalid": value_invalid,
        "dq_any_issue": any_issue,
    })

    return result


# ============================================================
# 7. STREAMING SILVER TRANSFORMATION
# ============================================================

def transform_assets():
    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 6.2")
    print("AMPS ASSETS SILVER TRANSFORMATION")
    print("=" * 70)

    check_safety()

    bronze, bronze_schema = inspect_bronze()

    bronze_hash_before = sha256_file(
        BRONZE_FILE
    )

    silver_schema = pa.schema(
        list(bronze_schema) + NEW_FIELDS
    )

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False
    )

    staging_file = (
        STAGING_DIR / "part-00000.parquet"
    )

    quality_counts = {
        field.name: 0
        for field in NEW_FIELDS
        if field.name.startswith("dq_")
    }

    rows_written = 0

    with pq.ParquetWriter(
        str(staging_file),
        silver_schema,
        compression="snappy",
        version="2.6",
    ) as writer:

        for batch in bronze.iter_batches(
            batch_size=BATCH_SIZE
        ):
            input_table = pa.Table.from_batches(
                [batch]
            )

            source_rows = input_table.to_pylist()

            transformed_rows = [
                transform_record(record)
                for record in source_rows
            ]

            for record in transformed_rows:
                for flag in quality_counts:
                    if record[flag]:
                        quality_counts[flag] += 1

            silver_table = pa.Table.from_pylist(
                transformed_rows,
                schema=silver_schema,
            )

            writer.write_table(silver_table)

            rows_written += silver_table.num_rows

            print(
                f"Processed: {rows_written:,} "
                f"of {EXPECTED_ROWS:,} assets",
                flush=True,
            )

    # ========================================================
    # 8. VALIDATE STAGED SILVER PARQUET
    # ========================================================

    print("\nVALIDATING SILVER DATASET")
    print("-" * 65)

    silver_metadata = pq.read_metadata(
        staging_file
    )

    silver_actual_schema = pq.read_schema(
        staging_file
    )

    if rows_written != EXPECTED_ROWS:
        raise ValueError(
            "Silver row reconciliation failed."
        )

    if silver_metadata.num_rows != EXPECTED_ROWS:
        raise ValueError(
            "Silver Parquet row count mismatch."
        )

    if not silver_actual_schema.equals(
        silver_schema,
        check_metadata=False,
    ):
        raise ValueError(
            "Silver Parquet schema mismatch."
        )

    # Verify the Bronze input remains unchanged.
    bronze_hash_after = sha256_file(
        BRONZE_FILE
    )

    if bronze_hash_after != bronze_hash_before:
        raise RuntimeError(
            "Bronze source checksum changed."
        )

    # Validate row-count preservation and original
    # Bronze field values against the staged Silver.
    # Check batches in the same physical row order.

    bronze_verify = pq.ParquetFile(
        str(BRONZE_FILE)
    )

    silver_verify = pq.ParquetFile(
        str(staging_file)
    )

    source_columns = bronze_schema.names

    silver_original_batches = (
        silver_verify.iter_batches(
            batch_size=BATCH_SIZE,
            columns=source_columns,
        )
    )

    verified_original_rows = 0

    for bronze_batch, silver_batch in zip(
        bronze_verify.iter_batches(
            batch_size=BATCH_SIZE,
            columns=source_columns,
        ),
        silver_original_batches,
        strict=True,
    ):
        if not bronze_batch.equals(
            silver_batch,
            check_metadata=False,
        ):
            raise ValueError(
                "Original Bronze values were modified "
                "during Silver transformation."
            )

        verified_original_rows += (
            bronze_batch.num_rows
        )

    if verified_original_rows != EXPECTED_ROWS:
        raise ValueError(
            "Original-field verification count mismatch."
        )

    print("Silver records:", f"{rows_written:,}")
    print("Silver columns:", len(silver_schema))
    print("Bronze original fields preserved: PASS")
    print("Bronze SHA-256 unchanged: PASS")
    print("Silver row reconciliation: PASS")
    print("Silver schema validation: PASS")

    # ========================================================
    # 9. PUBLISH FINAL SILVER DATA
    # ========================================================

    if SILVER_DIR.exists():
        raise FileExistsError(
            "Silver output appeared during processing."
        )

    STAGING_DIR.rename(SILVER_DIR)

    manifest = {
        "project": "AssetFusion360",
        "phase": "Silver",
        "step": "6.2",
        "dataset": "AMPS Assets",
        "status": "COMPLETED",
        "engine": "PyArrow",
        "orchestration": "Kedro SequentialRunner",
        "bronze_source": relative(BRONZE_FILE),
        "silver_output": relative(SILVER_FILE),
        "bronze_sha256": bronze_hash_before,
        "bronze_rows": EXPECTED_ROWS,
        "silver_rows": rows_written,
        "bronze_columns": len(bronze_schema),
        "silver_columns": len(silver_schema),
        "original_source_fields_preserved": True,
        "row_count_preserved": True,
        "quality_flag_counts": quality_counts,
        "silver_parquet_row_groups": (
            silver_metadata.num_row_groups
        ),
        "silver_parquet_size_bytes": (
            SILVER_FILE.stat().st_size
        ),
        "transformation_rules": [
            "Preserve all original Bronze fields.",
            "Parse selected date values into timestamps.",
            "Parse asset identity into nullable int64.",
            "Parse asset value into nullable decimal(20,6).",
            "Preserve missing values.",
            "Flag malformed nonempty dates and values.",
            "Flag missing and placeholder asset codes.",
            "Never filter or deduplicate source records.",
        ],
        "created_utc": utc_now(),
    }

    MANIFEST_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with MANIFEST_FILE.open(
        "x",
        encoding="utf-8"
    ) as handle:
        json.dump(
            manifest,
            handle,
            indent=2,
            ensure_ascii=False,
        )

    print("\n" + "=" * 70)
    print("AMPS ASSETS SILVER SUMMARY")
    print("=" * 70)
    print("Bronze rows:", f"{EXPECTED_ROWS:,}")
    print("Silver rows:", f"{rows_written:,}")
    print("Original columns:", len(bronze_schema))
    print("Silver columns:", len(silver_schema))

    print("\nQUALITY FLAG COUNTS")
    for flag, count in quality_counts.items():
        print(f"{flag}: {count:,}")

    print("\nSilver output:", SILVER_FILE)
    print("Manifest:", MANIFEST_FILE)
    print("\nASSETS SILVER TRANSFORMATION: SUCCESS")

    return manifest


# ============================================================
# 10. KEDRO PIPELINE
# ============================================================

def create_silver_assets_pipeline():
    return pipeline([
        node(
            func=transform_assets,
            inputs=None,
            outputs="amps_assets_silver_manifest",
            name="transform_amps_assets_silver",
            tags=["silver", "amps", "assets"],
        )
    ])


# ============================================================
# 11. KEDRO RUNNER
# ============================================================

def run_kedro_pipeline():
    catalog = DataCatalog({
        "amps_assets_silver_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    runner.run(
        create_silver_assets_pipeline(),
        catalog,
    )

    manifest = catalog.load(
        "amps_assets_silver_manifest"
    )

    if (
        not isinstance(manifest, dict)
        or manifest.get("status") != "COMPLETED"
    ):
        raise RuntimeError(
            "Kedro Silver Assets manifest validation failed."
        )

    print("\nKEDRO SILVER ASSETS PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
