
"""
AssetFusion360 Capstone Project
STEP 5.4F - AMPS Transaction Details Bronze Ingestion

Technology:
    Kedro + PySpark + PyArrow

Source:
    data/01_raw_source_data/
    nyc_parks_asset_management/transaction_details_batches/

Output:
    data/02_bronze_ingested_data/amps_transaction_details/

Manifest:
    reports/data_profiling_reports/
    amps_transaction_details_bronze_ingestion_manifest.json

Expected:
    461 CSV batches
    2,301,927 records
    31 original columns
    4 lineage metadata columns
    35 Bronze columns

Rules:
    - Preserve all source records.
    - Preserve source columns as strings.
    - Do not filter, clean, deduplicate, or impute.
    - Process one CSV batch at a time.
    - Read CSV using PySpark.
    - Write Parquet using PyArrow.
    - Validate source checksums.
    - Validate each Parquet file.
    - Reconcile total record counts.
    - Never overwrite existing Bronze data.
"""

import csv
import hashlib
import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
    / "transaction_details_batches"
)

BRONZE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "02_bronze_ingested_data"
)

OUTPUT_DIR = (
    BRONZE_ROOT
    / "amps_transaction_details"
)

STAGING_DIR = (
    BRONZE_ROOT
    / "_staging_amps_transaction_details"
)

MANIFEST_FILE = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
    / "amps_transaction_details_bronze_ingestion_manifest.json"
)

EXPECTED_FILES = 461
EXPECTED_ROWS = 2_301_927
EXPECTED_COLUMNS = 31

ARROW_BATCH_SIZE = 5000

SOURCE_PREFIX = "transaction_details_batch_"

REQUIRED_COLUMNS = [
    "trl_trans",
    "trl_event",
    "trl_sqlidentity",
    "trl_date",
    "trl_qty",
    "trl_price",
]

LINEAGE_COLUMNS = [
    "_bronze_source_file",
    "_bronze_ingested_at_utc",
    "_bronze_batch_id",
    "_bronze_source_batch_number",
]


# ============================================================
# 2. GENERAL UTILITIES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def relative_path(path):
    return path.relative_to(
        PROJECT_ROOT
    ).as_posix()


def calculate_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_csv_header(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return next(csv.reader(handle), [])


def expected_source_files():
    return [
        SOURCE_DIR
        / f"{SOURCE_PREFIX}{number:05d}.csv"
        for number in range(
            1,
            EXPECTED_FILES + 1,
        )
    ]


# ============================================================
# 3. OUTPUT SAFETY
# ============================================================

def check_output_safety():

    print("\nOUTPUT SAFETY CHECK")
    print("-" * 65)

    protected_paths = [
        OUTPUT_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
        MANIFEST_FILE.with_suffix(".json.tmp"),
    ]

    for path in protected_paths:
        if path.exists():
            raise FileExistsError(
                "Existing output detected. "
                "No overwrite is allowed:\n"
                f"{path}"
            )

    if not BRONZE_ROOT.is_dir():
        raise FileNotFoundError(
            f"Bronze root does not exist: {BRONZE_ROOT}"
        )

    print("Final Bronze output: AVAILABLE")
    print("Staging directory: AVAILABLE")
    print("Manifest: AVAILABLE")
    print("OUTPUT SAFETY CHECK: PASS")


# ============================================================
# 4. SOURCE INVENTORY AND SCHEMA VALIDATION
# ============================================================

def validate_sources():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source directory missing: {SOURCE_DIR}"
        )

    source_files = expected_source_files()

    expected_names = {
        path.name
        for path in source_files
    }

    actual_names = {
        path.name
        for path in SOURCE_DIR.iterdir()
        if path.is_file()
        and path.suffix.lower() == ".csv"
    }

    if actual_names != expected_names:
        missing = sorted(
            expected_names - actual_names
        )

        unexpected = sorted(
            actual_names - expected_names
        )

        raise ValueError(
            "Source batch inventory mismatch.\n"
            f"Missing: {missing[:10]}\n"
            f"Unexpected: {unexpected[:10]}"
        )

    reference_header = read_csv_header(
        source_files[0]
    )

    if len(reference_header) != EXPECTED_COLUMNS:
        raise ValueError(
            "Incorrect source column count."
        )

    if len(reference_header) != len(
        set(reference_header)
    ):
        raise ValueError(
            "Duplicate source column names."
        )

    for column in REQUIRED_COLUMNS:
        if column not in reference_header:
            raise ValueError(
                f"Required column missing: {column}"
            )

    for column in LINEAGE_COLUMNS:
        if column in reference_header:
            raise ValueError(
                f"Source/metadata conflict: {column}"
            )

    total_source_bytes = 0

    for path in source_files:

        if path.stat().st_size == 0:
            raise ValueError(
                f"Empty source file: {path.name}"
            )

        if read_csv_header(path) != reference_header:
            raise ValueError(
                f"Source schema mismatch: {path.name}"
            )

        total_source_bytes += (
            path.stat().st_size
        )

    print("\nSOURCE VALIDATION")
    print("-" * 65)
    print("CSV batches:", len(source_files))
    print("Original columns:", len(reference_header))
    print(
        "Total source bytes:",
        f"{total_source_bytes:,}",
    )
    print("SOURCE VALIDATION: PASS")

    return source_files, reference_header


# ============================================================
# 5. SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_AMPS_TransactionDetails_Bronze"
        )
        .master("local[2]")
        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )
        .config(
            "spark.sql.csv.parser.columnPruning.enabled",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 6. SOURCE AND PARQUET SCHEMAS
# ============================================================

def create_schemas(source_columns):

    spark_schema = StructType([
        StructField(
            column,
            StringType(),
            True,
        )
        for column in source_columns
    ])

    arrow_schema = pa.schema([
        pa.field(
            column,
            pa.string(),
            nullable=True,
        )
        for column in (
            source_columns + LINEAGE_COLUMNS
        )
    ])

    return spark_schema, arrow_schema


# ============================================================
# 7. VERIFY ONE PARQUET FILE
# ============================================================

def verify_parquet_part(
    parquet_path,
    arrow_schema,
    expected_rows,
    expected_lineage,
):

    parquet_file = pq.ParquetFile(
        str(parquet_path)
    )

    actual_rows = (
        parquet_file.metadata.num_rows
    )

    if actual_rows != expected_rows:
        raise ValueError(
            f"Parquet row mismatch: {parquet_path.name}"
        )

    if not parquet_file.schema_arrow.equals(
        arrow_schema,
        check_metadata=False,
    ):
        raise ValueError(
            f"Parquet schema mismatch: {parquet_path.name}"
        )

    inspected_rows = 0

    for batch in parquet_file.iter_batches(
        batch_size=ARROW_BATCH_SIZE,
        columns=LINEAGE_COLUMNS,
    ):

        for index, column in enumerate(
            LINEAGE_COLUMNS
        ):

            values = (
                batch.column(index).to_pylist()
            )

            expected_value = expected_lineage[
                column
            ]

            if any(
                value != expected_value
                for value in values
            ):
                raise ValueError(
                    "Lineage validation failed: "
                    f"{parquet_path.name} / {column}"
                )

        inspected_rows += batch.num_rows

    if inspected_rows != expected_rows:
        raise ValueError(
            "Parquet lineage scan mismatch: "
            f"{parquet_path.name}"
        )

    return {
        "rows": actual_rows,
        "row_groups": (
            parquet_file.metadata.num_row_groups
        ),
        "bytes": parquet_path.stat().st_size,
    }


# ============================================================
# 8. PROCESS ONE CSV SOURCE BATCH
# ============================================================

def process_source_batch(
    spark,
    csv_path,
    batch_index,
    source_columns,
    spark_schema,
    arrow_schema,
    ingestion_timestamp,
    ingestion_batch_id,
):

    filename = csv_path.name
    batch_number = f"{batch_index:05d}"

    expected_filename = (
        f"{SOURCE_PREFIX}{batch_number}.csv"
    )

    if filename != expected_filename:
        raise ValueError(
            f"Unexpected source filename: {filename}"
        )

    source_checksum_before = (
        calculate_sha256(csv_path)
    )

    parquet_filename = (
        f"{SOURCE_PREFIX}{batch_number}.parquet"
    )

    parquet_path = (
        STAGING_DIR / parquet_filename
    )

    if parquet_path.exists():
        raise FileExistsError(
            f"Parquet part already exists: {parquet_path}"
        )

    dataframe = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("enforceSchema", "false")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .schema(spark_schema)
        .csv(str(csv_path.resolve()))
    )

    if dataframe.columns != source_columns:
        raise ValueError(
            f"Spark schema mismatch: {filename}"
        )

    lineage = {
        "_bronze_source_file": filename,
        "_bronze_ingested_at_utc": (
            ingestion_timestamp
        ),
        "_bronze_batch_id": (
            ingestion_batch_id
        ),
        "_bronze_source_batch_number": (
            batch_number
        ),
    }

    buffered_records = []
    rows_written = 0

    with pq.ParquetWriter(
        str(parquet_path),
        arrow_schema,
        compression="snappy",
        version="2.6",
    ) as writer:

        # Stream rows from Spark into bounded
        # Python batches, then write using PyArrow.

        for row in dataframe.toLocalIterator():

            values = row.asDict(
                recursive=False
            )

            record = {
                column: values.get(column)
                for column in source_columns
            }

            record.update(lineage)

            buffered_records.append(record)

            if len(buffered_records) >= ARROW_BATCH_SIZE:

                table = pa.Table.from_pylist(
                    buffered_records,
                    schema=arrow_schema,
                )

                writer.write_table(table)

                rows_written += table.num_rows

                buffered_records.clear()

        if buffered_records:

            table = pa.Table.from_pylist(
                buffered_records,
                schema=arrow_schema,
            )

            writer.write_table(table)

            rows_written += table.num_rows

            buffered_records.clear()

    validation = verify_parquet_part(
        parquet_path=parquet_path,
        arrow_schema=arrow_schema,
        expected_rows=rows_written,
        expected_lineage=lineage,
    )

    source_checksum_after = (
        calculate_sha256(csv_path)
    )

    if (
        source_checksum_before
        != source_checksum_after
    ):
        raise RuntimeError(
            f"Source checksum changed: {filename}"
        )

    return {
        "source_file": relative_path(
            csv_path
        ),
        "source_filename": filename,
        "source_batch_number": batch_number,
        "source_sha256": (
            source_checksum_before
        ),
        "source_size_bytes": (
            csv_path.stat().st_size
        ),
        "parquet_filename": parquet_filename,
        "records": rows_written,
        "parquet_row_groups": (
            validation["row_groups"]
        ),
        "parquet_size_bytes": (
            validation["bytes"]
        ),
        "status": "PASS",
    }


# ============================================================
# 9. VALIDATE ALL PARQUET OUTPUTS
# ============================================================

def validate_complete_bronze(
    results,
    arrow_schema,
):

    actual_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    expected_names = {
        result["parquet_filename"]
        for result in results
    }

    actual_names = {
        path.name
        for path in actual_files
    }

    if (
        len(results) != EXPECTED_FILES
        or len(actual_files) != EXPECTED_FILES
        or actual_names != expected_names
    ):
        raise ValueError(
            "Complete Parquet inventory mismatch."
        )

    total_records = 0
    total_row_groups = 0
    total_parquet_bytes = 0

    for result in results:

        parquet_path = (
            STAGING_DIR
            / result["parquet_filename"]
        )

        metadata = pq.read_metadata(
            parquet_path
        )

        schema = pq.read_schema(
            parquet_path
        )

        if not schema.equals(
            arrow_schema,
            check_metadata=False,
        ):
            raise ValueError(
                "Final schema mismatch: "
                f"{parquet_path.name}"
            )

        if metadata.num_rows != result["records"]:
            raise ValueError(
                "Final row count mismatch: "
                f"{parquet_path.name}"
            )

        total_records += metadata.num_rows

        total_row_groups += (
            metadata.num_row_groups
        )

        total_parquet_bytes += (
            parquet_path.stat().st_size
        )

    if total_records != EXPECTED_ROWS:
        raise ValueError(
            "Global record reconciliation FAILED.\n"
            f"Expected: {EXPECTED_ROWS:,}\n"
            f"Actual: {total_records:,}\n"
            "Staging remains unpublished."
        )

    print("\nCOMPLETE BRONZE VALIDATION")
    print("-" * 65)

    print("Parquet files:", len(actual_files))
    print("Bronze records:", f"{total_records:,}")
    print("Bronze columns:", len(arrow_schema))
    print("Parquet row groups:", total_row_groups)

    print(
        "Parquet bytes:",
        f"{total_parquet_bytes:,}",
    )

    print("GLOBAL RECORD RECONCILIATION: PASS")
    print("GLOBAL PARQUET SCHEMA: PASS")
    print("GLOBAL BATCH INVENTORY: PASS")

    return {
        "records": total_records,
        "files": len(actual_files),
        "columns": len(arrow_schema),
        "row_groups": total_row_groups,
        "parquet_bytes": total_parquet_bytes,
    }


# ============================================================
# 10. MAIN INGESTION FUNCTION
# ============================================================

def ingest_amps_transaction_details_bronze():

    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 5.4F")
    print("AMPS TRANSACTION DETAILS BRONZE INGESTION")
    print("=" * 70)

    check_output_safety()

    source_files, source_columns = (
        validate_sources()
    )

    spark_schema, arrow_schema = (
        create_schemas(source_columns)
    )

    ingestion_timestamp = utc_now()

    ingestion_batch_id = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%S%fZ")
    )

    spark = None
    spark_version = None

    results = []
    cumulative_records = 0

    try:

        spark = create_spark_session()
        spark_version = spark.version

        STAGING_DIR.mkdir(
            parents=False,
            exist_ok=False,
        )

        print("\nPROCESSING SOURCE BATCHES")
        print("-" * 65)

        for index, source_path in enumerate(
            source_files,
            start=1,
        ):

            result = process_source_batch(
                spark=spark,
                csv_path=source_path,
                batch_index=index,
                source_columns=source_columns,
                spark_schema=spark_schema,
                arrow_schema=arrow_schema,
                ingestion_timestamp=(
                    ingestion_timestamp
                ),
                ingestion_batch_id=(
                    ingestion_batch_id
                ),
            )

            results.append(result)

            cumulative_records += (
                result["records"]
            )

            print(
                f"[{index:03d}/{EXPECTED_FILES}] "
                f"{source_path.name}: "
                f"{result['records']:,} records | "
                f"Cumulative: {cumulative_records:,}",
                flush=True,
            )

    finally:

        if spark is not None:
            spark.stop()

    validation = validate_complete_bronze(
        results,
        arrow_schema,
    )

    final_files, final_columns = (
        validate_sources()
    )

    if (
        [path.name for path in final_files]
        != [path.name for path in source_files]
    ):
        raise RuntimeError(
            "Source inventory changed during ingestion."
        )

    if final_columns != source_columns:
        raise RuntimeError(
            "Source schema changed during ingestion."
        )

    # Recheck source checksums before publication.

    for result, source_path in zip(
        results,
        source_files,
    ):

        current_hash = calculate_sha256(
            source_path
        )

        if current_hash != result["source_sha256"]:
            raise RuntimeError(
                "Source checksum changed before "
                f"publication: {source_path.name}"
            )

    if (
        OUTPUT_DIR.exists()
        or MANIFEST_FILE.exists()
        or MANIFEST_FILE.with_suffix(".json.tmp").exists()
    ):
        raise FileExistsError(
            "Final output appeared during ingestion."
        )

    print("\nPUBLISHING VALIDATED BRONZE OUTPUT")
    print("-" * 65)

    STAGING_DIR.rename(
        OUTPUT_DIR
    )

    manifest = {
        "project": "AssetFusion360",
        "phase": "Bronze",
        "step": "5.4F",
        "dataset": "AMPS Transaction Details",
        "status": "COMPLETED",
        "orchestration": "Kedro SequentialRunner",
        "ingestion_engine": "PySpark",
        "parquet_writer": "PyArrow",
        "spark_version": spark_version,
        "pyarrow_version": pa.__version__,
        "source_directory": relative_path(
            SOURCE_DIR
        ),
        "bronze_directory": relative_path(
            OUTPUT_DIR
        ),
        "expected_source_batches": (
            EXPECTED_FILES
        ),
        "processed_source_batches": (
            len(results)
        ),
        "expected_records": EXPECTED_ROWS,
        "bronze_records": (
            validation["records"]
        ),
        "original_source_columns": (
            len(source_columns)
        ),
        "bronze_columns": (
            validation["columns"]
        ),
        "source_column_names": (
            source_columns
        ),
        "metadata_columns": (
            LINEAGE_COLUMNS
        ),
        "ingestion_timestamp_utc": (
            ingestion_timestamp
        ),
        "ingestion_batch_id": (
            ingestion_batch_id
        ),
        "arrow_batch_size": (
            ARROW_BATCH_SIZE
        ),
        "parquet_files": (
            validation["files"]
        ),
        "parquet_row_groups": (
            validation["row_groups"]
        ),
        "parquet_size_bytes": (
            validation["parquet_bytes"]
        ),
        "validation": {
            "source_batch_inventory": "PASS",
            "source_header_consistency": "PASS",
            "parquet_schema": "PASS",
            "parquet_record_counts": "PASS",
            "source_lineage": "PASS",
            "source_checksums": "PASS",
            "global_record_reconciliation": "PASS",
        },
        "processing_rules": [
            "No source records filtered.",
            "No deduplication.",
            "No missing-value imputation.",
            "No business transformations.",
            "All original fields preserved as strings.",
            "Four lineage metadata columns added.",
            "One Parquet file per CSV batch.",
        ],
        "source_batch_manifest": results,
        "created_utc": utc_now(),
    }

    MANIFEST_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Exclusive creation prevents manifest overwriting.

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
    print("TRANSACTION DETAILS BRONZE SUMMARY")
    print("=" * 70)

    print("CSV batches:", len(results))
    print(
        "Parquet files:",
        validation["files"],
    )
    print(
        "Bronze records:",
        f"{validation['records']:,}",
    )
    print(
        "Original columns:",
        len(source_columns),
    )
    print(
        "Bronze columns:",
        validation["columns"],
    )
    print(
        "Parquet row groups:",
        validation["row_groups"],
    )

    print("\nBronze output:")
    print(OUTPUT_DIR)

    print("\nManifest:")
    print(MANIFEST_FILE)

    print(
        "\nTRANSACTION DETAILS "
        "BRONZE INGESTION: SUCCESS"
    )

    return manifest


# ============================================================
# 11. KEDRO PIPELINE DEFINITION
# ============================================================

def create_bronze_transaction_details_pipeline():

    return pipeline([
        node(
            func=ingest_amps_transaction_details_bronze,
            inputs=None,
            outputs=(
                "amps_transaction_details_bronze_manifest"
            ),
            name="ingest_amps_transaction_details_bronze",
            tags=[
                "bronze",
                "amps",
                "transaction_details",
            ],
        )
    ])


# ============================================================
# 12. KEDRO RUNNER
# ============================================================

def run_kedro_pipeline():

    catalog = DataCatalog({
        "amps_transaction_details_bronze_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    runner.run(
        create_bronze_transaction_details_pipeline(),
        catalog,
    )

    manifest = catalog.load(
        "amps_transaction_details_bronze_manifest"
    )

    if not isinstance(manifest, dict):
        raise RuntimeError(
            "Kedro manifest is not a dictionary."
        )

    if manifest.get("status") != "COMPLETED":
        raise RuntimeError(
            "Kedro Transaction Details ingestion "
            "did not complete successfully."
        )

    print("\n" + "=" * 70)
    print(
        "KEDRO TRANSACTION DETAILS PIPELINE: SUCCESS"
    )
    print("=" * 70)


# ============================================================
# 13. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_kedro_pipeline()
