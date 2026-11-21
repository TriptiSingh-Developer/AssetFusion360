
"""
AssetFusion360 Capstone Project
STEP 5.4C - AMPS Work Orders Bronze Ingestion

Technology:
    Kedro 1.8.0
    PySpark 3.5.7
    PyArrow 26.0.1

Source:
    data/01_raw_source_data/
    nyc_parks_asset_management/workorders_batches/

Output:
    data/02_bronze_ingested_data/amps_workorders/

Manifest:
    reports/data_profiling_reports/
    amps_workorders_bronze_ingestion_manifest.json

Design:
    - Process one CSV batch at a time.
    - Preserve every original source column as a string.
    - Do not deduplicate, clean, or impute records.
    - Write Parquet using PyArrow, not Spark.
    - Preserve source-file lineage.
    - Validate each batch independently.
    - Reconcile total records with prior profiling.
    - Verify raw source checksums.
    - Refuse to overwrite existing outputs.
    - Execute through a Kedro SequentialRunner.
"""

import csv
import hashlib
import json
import os
import re
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
    / "workorders_batches"
)

BRONZE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "02_bronze_ingested_data"
)

OUTPUT_DIR = (
    BRONZE_ROOT
    / "amps_workorders"
)

STAGING_DIR = (
    BRONZE_ROOT
    / "_staging_amps_workorders"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

MANIFEST_FILE = (
    REPORT_DIR
    / "amps_workorders_bronze_ingestion_manifest.json"
)


# ============================================================
# 2. EXPECTED SOURCE PROFILE
# ============================================================

DATASET_NAME = "AMPS Work Orders"

EXPECTED_FILES = 447
EXPECTED_ROWS = 2_231_535
EXPECTED_COLUMNS = 55

ARROW_BATCH_SIZE = 5000

FILE_PATTERN = re.compile(
    r"^workorders_batch_(\d{5})\.csv$",
    re.IGNORECASE
)

REQUIRED_COLUMNS = [
    "evt_code",
    "evt_object",
    "evt_sqlidentity",
]

METADATA_COLUMNS = [
    "_bronze_source_file",
    "_bronze_ingested_at_utc",
    "_bronze_batch_id",
    "_bronze_source_batch_number",
]


# ============================================================
# 3. GENERAL HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def relative_path(path):
    return path.relative_to(
        PROJECT_ROOT
    ).as_posix()


def sha256_file(path):

    checksum = hashlib.sha256()

    with path.open("rb") as handle:

        while True:

            chunk = handle.read(
                1024 * 1024
            )

            if not chunk:
                break

            checksum.update(chunk)

    return checksum.hexdigest()


def read_header(path):

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as handle:

        return next(
            csv.reader(handle),
            []
        )


def save_json_atomic(path, document):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    if temporary.exists():
        raise FileExistsError(
            f"Temporary manifest exists: {temporary}"
        )

    with temporary.open(
        "x",
        encoding="utf-8"
    ) as handle:

        json.dump(
            document,
            handle,
            indent=2,
            ensure_ascii=False
        )

    os.replace(
        temporary,
        path
    )


# ============================================================
# 4. PREVENT ACCIDENTAL OVERWRITING
# ============================================================

def check_output_safety():

    print("\nOUTPUT SAFETY CHECK")
    print("-" * 65)

    protected_paths = [
        OUTPUT_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
        MANIFEST_FILE.with_suffix(
            MANIFEST_FILE.suffix + ".tmp"
        ),
    ]

    for path in protected_paths:

        if path.exists():

            raise FileExistsError(
                "An output or staging artifact "
                "already exists.\n"
                "No existing data will be overwritten.\n"
                f"Path: {path}"
            )

    print("Final Bronze directory: AVAILABLE")
    print("Staging directory: AVAILABLE")
    print("Ingestion manifest: AVAILABLE")


# ============================================================
# 5. VALIDATE ALL SOURCE FILES
# ============================================================

def discover_source_files():

    print("\nDISCOVERING WORK ORDER BATCHES")
    print("-" * 65)

    if not SOURCE_DIR.is_dir():

        raise FileNotFoundError(
            f"Source directory missing:\n{SOURCE_DIR}"
        )

    actual_csv_files = sorted(
        path
        for path in SOURCE_DIR.iterdir()
        if path.is_file()
        and path.suffix.lower() == ".csv"
    )

    if len(actual_csv_files) != EXPECTED_FILES:

        raise ValueError(
            "Unexpected number of source batches.\n"
            f"Expected: {EXPECTED_FILES}\n"
            f"Actual: {len(actual_csv_files)}"
        )

    expected_names = {
        f"workorders_batch_{number:05d}.csv"
        for number in range(
            1,
            EXPECTED_FILES + 1
        )
    }

    actual_names = {
        path.name
        for path in actual_csv_files
    }

    if expected_names != actual_names:

        missing = sorted(
            expected_names - actual_names
        )

        unexpected = sorted(
            actual_names - expected_names
        )

        raise ValueError(
            "Source batch filenames do not match "
            "the expected inventory.\n"
            f"Missing: {missing[:10]}\n"
            f"Unexpected: {unexpected[:10]}"
        )

    source_files = [
        SOURCE_DIR
        / f"workorders_batch_{number:05d}.csv"
        for number in range(
            1,
            EXPECTED_FILES + 1
        )
    ]

    reference_header = None
    total_bytes = 0

    for source_path in source_files:

        if source_path.stat().st_size == 0:

            raise ValueError(
                f"Empty source file: {source_path.name}"
            )

        header = read_header(
            source_path
        )

        if reference_header is None:
            reference_header = header

        if len(header) != EXPECTED_COLUMNS:

            raise ValueError(
                "Incorrect source column count: "
                f"{source_path.name}"
            )

        if header != reference_header:

            raise ValueError(
                "Source schema mismatch: "
                f"{source_path.name}"
            )

        if len(set(header)) != len(header):

            raise ValueError(
                "Duplicate source field: "
                f"{source_path.name}"
            )

        total_bytes += (
            source_path.stat().st_size
        )

    for required in REQUIRED_COLUMNS:

        if required not in reference_header:

            raise ValueError(
                f"Required source column missing: {required}"
            )

    for metadata_field in METADATA_COLUMNS:

        if metadata_field in reference_header:

            raise ValueError(
                "Metadata field conflicts with source: "
                f"{metadata_field}"
            )

    print(
        f"Source batches: {len(source_files)}"
    )

    print(
        f"Source columns: {len(reference_header)}"
    )

    print(
        f"Total source size: {total_bytes:,} bytes"
    )

    print(
        "Source schema consistency: PASS"
    )

    return source_files, reference_header


# ============================================================
# 6. CREATE SPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_AMPS_WorkOrders_Bronze"
        )
        .master("local[2]")
        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )
        .config(
            "spark.sql.csv.parser.columnPruning.enabled",
            "false"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    return spark


# ============================================================
# 7. CREATE SOURCE AND ARROW SCHEMAS
# ============================================================

def create_spark_schema(source_columns):

    return StructType([
        StructField(
            column_name,
            StringType(),
            True
        )
        for column_name in source_columns
    ])


def create_arrow_schema(source_columns):

    all_columns = (
        source_columns
        + METADATA_COLUMNS
    )

    return pa.schema([
        pa.field(
            column_name,
            pa.string(),
            nullable=True
        )
        for column_name in all_columns
    ])


# ============================================================
# 8. READ ONE CSV BATCH WITH PYSPARK
# ============================================================

def read_spark_batch(
    spark,
    csv_path,
    spark_schema,
    source_columns
):

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
            "Spark schema differs from expected "
            f"source schema: {csv_path.name}"
        )

    return dataframe


# ============================================================
# 9. WRITE ONE ARROW BATCH
# ============================================================

def write_arrow_records(
    writer,
    records,
    schema
):

    if not records:
        return 0

    table = pa.Table.from_pylist(
        records,
        schema=schema
    )

    writer.write_table(table)

    return table.num_rows


# ============================================================
# 10. VALIDATE ONE PARQUET FILE
# ============================================================

def validate_parquet_part(
    parquet_path,
    arrow_schema,
    expected_rows,
    source_filename,
    ingestion_timestamp,
    ingestion_batch_id,
    source_batch_number
):

    parquet_file = pq.ParquetFile(
        str(parquet_path)
    )

    actual_rows = (
        parquet_file.metadata.num_rows
    )

    actual_schema = (
        parquet_file.schema_arrow
    )

    if actual_rows != expected_rows:

        raise ValueError(
            "Parquet row count mismatch: "
            f"{parquet_path.name}"
        )

    if not actual_schema.equals(
        arrow_schema,
        check_metadata=False
    ):

        raise ValueError(
            "Parquet schema mismatch: "
            f"{parquet_path.name}"
        )

    metadata_values = {
        "_bronze_source_file": source_filename,
        "_bronze_ingested_at_utc": (
            ingestion_timestamp
        ),
        "_bronze_batch_id": (
            ingestion_batch_id
        ),
        "_bronze_source_batch_number": (
            source_batch_number
        ),
    }

    inspected_rows = 0

    for batch in parquet_file.iter_batches(
        batch_size=ARROW_BATCH_SIZE,
        columns=METADATA_COLUMNS
    ):

        for index, column_name in enumerate(
            METADATA_COLUMNS
        ):

            values = (
                batch.column(index).to_pylist()
            )

            expected_value = (
                metadata_values[column_name]
            )

            if any(
                value != expected_value
                for value in values
            ):

                raise ValueError(
                    "Parquet source-lineage metadata "
                    f"mismatch in {parquet_path.name}: "
                    f"{column_name}"
                )

        inspected_rows += batch.num_rows

    if inspected_rows != expected_rows:

        raise ValueError(
            "Parquet metadata scan count mismatch: "
            f"{parquet_path.name}"
        )

    return {
        "rows": actual_rows,
        "row_groups": (
            parquet_file.metadata.num_row_groups
        ),
        "size_bytes": (
            parquet_path.stat().st_size
        ),
    }


# ============================================================
# 11. PROCESS ONE SOURCE CSV FILE
# ============================================================

def process_source_batch(
    spark,
    source_path,
    source_columns,
    spark_schema,
    arrow_schema,
    ingestion_timestamp,
    ingestion_batch_id
):

    filename = source_path.name

    match = FILE_PATTERN.match(
        filename
    )

    if match is None:

        raise ValueError(
            f"Unexpected source filename: {filename}"
        )

    source_batch_number = (
        match.group(1)
    )

    source_checksum_before = (
        sha256_file(source_path)
    )

    source_dataframe = read_spark_batch(
        spark,
        source_path,
        spark_schema,
        source_columns
    )

    parquet_name = (
        "workorders_batch_"
        f"{source_batch_number}.parquet"
    )

    parquet_path = (
        STAGING_DIR
        / parquet_name
    )

    if parquet_path.exists():

        raise FileExistsError(
            f"Parquet part already exists: {parquet_path}"
        )

    writer = None
    records = []
    total_written = 0

    try:

        writer = pq.ParquetWriter(
            where=str(parquet_path),
            schema=arrow_schema,
            compression="snappy",
            version="2.6"
        )

        # Spark reads the CSV source.
        # Python receives rows incrementally.
        # PyArrow writes bounded Parquet row groups.

        for spark_row in (
            source_dataframe.toLocalIterator()
        ):

            values = spark_row.asDict(
                recursive=False
            )

            record = {
                column_name: values.get(
                    column_name
                )
                for column_name in source_columns
            }

            record["_bronze_source_file"] = (
                filename
            )

            record["_bronze_ingested_at_utc"] = (
                ingestion_timestamp
            )

            record["_bronze_batch_id"] = (
                ingestion_batch_id
            )

            record["_bronze_source_batch_number"] = (
                source_batch_number
            )

            records.append(record)

            if len(records) >= ARROW_BATCH_SIZE:

                total_written += (
                    write_arrow_records(
                        writer,
                        records,
                        arrow_schema
                    )
                )

                records.clear()

        if records:

            total_written += (
                write_arrow_records(
                    writer,
                    records,
                    arrow_schema
                )
            )

            records.clear()

    finally:

        if writer is not None:
            writer.close()

    validation = validate_parquet_part(
        parquet_path=parquet_path,
        arrow_schema=arrow_schema,
        expected_rows=total_written,
        source_filename=filename,
        ingestion_timestamp=(
            ingestion_timestamp
        ),
        ingestion_batch_id=(
            ingestion_batch_id
        ),
        source_batch_number=(
            source_batch_number
        ),
    )

    source_checksum_after = (
        sha256_file(source_path)
    )

    if (
        source_checksum_before
        != source_checksum_after
    ):

        raise RuntimeError(
            "Raw source checksum changed during "
            f"processing: {filename}"
        )

    return {
        "source_file": relative_path(
            source_path
        ),
        "source_filename": filename,
        "source_batch_number": (
            source_batch_number
        ),
        "source_sha256": (
            source_checksum_before
        ),
        "source_size_bytes": (
            source_path.stat().st_size
        ),
        "parquet_filename": parquet_name,
        "records": total_written,
        "parquet_row_groups": (
            validation["row_groups"]
        ),
        "parquet_size_bytes": (
            validation["size_bytes"]
        ),
        "status": "PASS",
    }


# ============================================================
# 12. VALIDATE COMPLETE STAGING DATASET
# ============================================================

def validate_staging_dataset(
    results,
    arrow_schema
):

    print("\nVALIDATING COMPLETE WORK ORDERS BRONZE")
    print("-" * 65)

    actual_parquet_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    if len(actual_parquet_files) != EXPECTED_FILES:

        raise ValueError(
            "Unexpected number of Bronze Parquet "
            "files in staging."
        )

    expected_parquet_names = {
        result["parquet_filename"]
        for result in results
    }

    actual_parquet_names = {
        path.name
        for path in actual_parquet_files
    }

    if (
        expected_parquet_names
        != actual_parquet_names
    ):

        raise ValueError(
            "Parquet inventory mismatch."
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
            check_metadata=False
        ):

            raise ValueError(
                "Final schema validation failed: "
                f"{parquet_path.name}"
            )

        if (
            metadata.num_rows
            != result["records"]
        ):

            raise ValueError(
                "Final row count validation failed: "
                f"{parquet_path.name}"
            )

        total_records += (
            metadata.num_rows
        )

        total_row_groups += (
            metadata.num_row_groups
        )

        total_parquet_bytes += (
            parquet_path.stat().st_size
        )

    if total_records != EXPECTED_ROWS:

        raise ValueError(
            "Source profile reconciliation FAILED.\n"
            f"Expected records: {EXPECTED_ROWS:,}\n"
            f"Bronze records: {total_records:,}\n"
            "Staging has not been published."
        )

    print(
        f"Parquet files: {len(actual_parquet_files)}"
    )

    print(
        f"Validated records: {total_records:,}"
    )

    print(
        f"Parquet columns: {len(arrow_schema)}"
    )

    print(
        f"Parquet row groups: {total_row_groups:,}"
    )

    print(
        "Parquet output size: "
        f"{total_parquet_bytes:,} bytes"
    )

    print("Global row reconciliation: PASS")
    print("Global schema validation: PASS")
    print("Batch inventory validation: PASS")

    return {
        "total_records": total_records,
        "parquet_files": (
            len(actual_parquet_files)
        ),
        "parquet_columns": (
            len(arrow_schema)
        ),
        "parquet_row_groups": (
            total_row_groups
        ),
        "total_parquet_bytes": (
            total_parquet_bytes
        ),
    }


# ============================================================
# 13. MAIN KEDRO INGESTION NODE
# ============================================================

def ingest_amps_workorders_bronze():

    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 5.4C")
    print("AMPS WORK ORDERS BRONZE INGESTION")
    print("KEDRO + PYSPARK + PYARROW")
    print("=" * 70)

    check_output_safety()

    source_files, source_columns = (
        discover_source_files()
    )

    spark_schema = create_spark_schema(
        source_columns
    )

    arrow_schema = create_arrow_schema(
        source_columns
    )

    ingestion_timestamp = utc_now()

    ingestion_batch_id = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%S%fZ")
    )

    spark = None
    results = []
    total_written = 0
    spark_version = None

    try:

        spark = create_spark()

        spark_version = spark.version

        # Staging is created only after the
        # source and destination checks pass.

        STAGING_DIR.mkdir(
            parents=False,
            exist_ok=False
        )

        print("\nPROCESSING SOURCE BATCHES")
        print("-" * 65)

        for index, source_path in enumerate(
            source_files,
            start=1
        ):

            result = process_source_batch(
                spark=spark,
                source_path=source_path,
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

            results.append(
                result
            )

            total_written += (
                result["records"]
            )

            print(
                f"[{index:03d}/{EXPECTED_FILES}] "
                f"{source_path.name}: "
                f"{result['records']:,} rows | "
                f"Cumulative: {total_written:,}"
            )

    finally:

        if spark is not None:
            spark.stop()

    # The Spark session has stopped.
    # Validate the entire staged dataset.

    validation = validate_staging_dataset(
        results,
        arrow_schema
    )

    # Final source inventory check:
    # no files were added or removed
    # during the ingestion run.

    final_source_files, final_columns = (
        discover_source_files()
    )

    if (
        [p.name for p in final_source_files]
        != [p.name for p in source_files]
    ):

        raise RuntimeError(
            "Raw source inventory changed."
        )

    if final_columns != source_columns:

        raise RuntimeError(
            "Raw source schema changed."
        )

    print("\nPUBLISHING VALIDATED BRONZE OUTPUT")
    print("-" * 65)

    if OUTPUT_DIR.exists():

        raise FileExistsError(
            "Final Bronze directory appeared "
            "during processing. Refusing overwrite."
        )

    # Both folders are under the same Bronze root.
    # Publish only after all validations pass.

    STAGING_DIR.rename(
        OUTPUT_DIR
    )

    manifest = {
        "project": "AssetFusion360",
        "phase": "Bronze",
        "step": "5.4C",
        "dataset": DATASET_NAME,
        "status": "COMPLETED",
        "orchestration": "Kedro SequentialRunner",
        "ingestion_engine": "PySpark",
        "local_parquet_writer": "PyArrow",
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
        "expected_records": (
            EXPECTED_ROWS
        ),
        "bronze_records": (
            validation["total_records"]
        ),
        "original_source_columns": (
            len(source_columns)
        ),
        "bronze_columns": (
            validation["parquet_columns"]
        ),
        "source_column_names": (
            source_columns
        ),
        "metadata_columns": (
            METADATA_COLUMNS
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
            validation["parquet_files"]
        ),
        "parquet_row_groups": (
            validation["parquet_row_groups"]
        ),
        "total_parquet_bytes": (
            validation["total_parquet_bytes"]
        ),
        "validation": {
            "all_expected_batches_present": True,
            "source_headers_consistent": True,
            "all_source_columns_preserved": True,
            "all_source_columns_string": True,
            "all_batch_parquet_counts_verified": True,
            "all_batch_metadata_verified": True,
            "source_checksums_unchanged_during_processing": True,
            "total_records_match_profile": True,
            "no_duplicate_source_batches_ingested": True,
        },
        "processing_rules": [
            "No record filtering.",
            "No business-key deduplication.",
            "No source column removal.",
            "No missing-value imputation.",
            "No numeric or date type conversion.",
            "Source values follow Spark CSV parsing semantics.",
            "Four ingestion metadata fields added.",
            "Each source batch becomes one Parquet part.",
        ],
        "source_batch_manifest": results,
        "created_utc": utc_now(),
    }

    save_json_atomic(
        MANIFEST_FILE,
        manifest
    )

    print("\n" + "=" * 70)
    print("AMPS WORK ORDERS BRONZE INGESTION SUMMARY")
    print("=" * 70)

    print(
        f"Source CSV batches: {len(results)}"
    )

    print(
        "Bronze Parquet files: "
        f"{validation['parquet_files']}"
    )

    print(
        f"Bronze records: {validation['total_records']:,}"
    )

    print(
        f"Original columns: {len(source_columns)}"
    )

    print(
        f"Bronze columns: {validation['parquet_columns']}"
    )

    print(
        "Parquet row groups: "
        f"{validation['parquet_row_groups']}"
    )

    print("\nBronze output:")
    print(OUTPUT_DIR)

    print("\nIngestion manifest:")
    print(MANIFEST_FILE)

    print("\n" + "=" * 70)
    print("WORK ORDERS BRONZE INGESTION: SUCCESS")
    print("=" * 70)

    return manifest


# ============================================================
# 14. KEDRO PIPELINE DEFINITION
# ============================================================

def create_bronze_workorders_pipeline():

    return pipeline([
        node(
            func=ingest_amps_workorders_bronze,
            inputs=None,
            outputs="amps_workorders_bronze_manifest",
            name="ingest_amps_workorders_bronze",
            tags=[
                "bronze",
                "amps",
                "workorders"
            ],
        )
    ])


# ============================================================
# 15. KEDRO RUNNER
# ============================================================

def run_kedro_pipeline():

    workorders_pipeline = (
        create_bronze_workorders_pipeline()
    )

    catalog = DataCatalog({
        "amps_workorders_bronze_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    runner.run(
        workorders_pipeline,
        catalog
    )

    # Load the manifest data from the catalog.
    # Do not treat a MemoryDataset as a dictionary.

    manifest = catalog.load(
        "amps_workorders_bronze_manifest"
    )

    if not isinstance(manifest, dict):

        raise RuntimeError(
            "Kedro manifest output is not a dictionary."
        )

    if manifest.get("status") != "COMPLETED":

        raise RuntimeError(
            "Kedro ingestion status was not COMPLETED."
        )

    print("\n" + "=" * 70)
    print("KEDRO WORK ORDERS PIPELINE: SUCCESS")
    print("=" * 70)


# ============================================================
# 16. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_kedro_pipeline()
