
"""
AssetFusion360 Capstone Project
Step 5.4D - AMPS Labor Bronze Ingestion

Stack:
    Kedro + PySpark + PyArrow

Source:
    data/01_raw_source_data/
    nyc_parks_asset_management/labor_batches/

Output:
    data/02_bronze_ingested_data/amps_labor/

Manifest:
    reports/data_profiling_reports/
    amps_labor_bronze_ingestion_manifest.json

Rules:
    - Process all 647 source batches.
    - Preserve all 18 original columns as strings.
    - Add four ingestion metadata columns.
    - Preserve every source record.
    - Do not clean, filter, deduplicate, or impute.
    - Read with PySpark.
    - Write local Parquet using PyArrow.
    - Use bounded batches of 5,000 records.
    - Validate each Parquet part.
    - Verify source SHA-256 checksums.
    - Reconcile exactly 3,230,423 records.
    - Do not overwrite existing outputs.
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
    StructField,
    StructType,
    StringType,
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
    / "labor_batches"
)

BRONZE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "02_bronze_ingested_data"
)

OUTPUT_DIR = BRONZE_ROOT / "amps_labor"

STAGING_DIR = BRONZE_ROOT / "_staging_amps_labor"

MANIFEST_FILE = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
    / "amps_labor_bronze_ingestion_manifest.json"
)

EXPECTED_FILES = 647
EXPECTED_ROWS = 3_230_423
EXPECTED_COLUMNS = 18

BATCH_SIZE = 5000

SOURCE_PATTERN = re.compile(
    r"^labor_batch_(\d{5})\.csv$"
)

REQUIRED_COLUMNS = [
    "boo_event",
    "boo_sqlidentity",
    "boo_hours",
    "boo_cost",
]

METADATA_COLUMNS = [
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
    return path.relative_to(PROJECT_ROOT).as_posix()


def calculate_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def read_header(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return next(csv.reader(handle), [])


def save_manifest(path, document):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    if path.exists() or temporary.exists():
        raise FileExistsError(
            "Manifest destination already exists."
        )

    with temporary.open(
        "x",
        encoding="utf-8",
    ) as handle:
        json.dump(
            document,
            handle,
            indent=2,
            ensure_ascii=False,
        )

    # Refuse to replace an existing final manifest.
    if path.exists():
        raise FileExistsError(
            f"Manifest appeared during processing: {path}"
        )

    temporary.rename(path)


# ============================================================
# 3. OUTPUT SAFETY
# ============================================================

def validate_output_safety():
    print("\nOUTPUT SAFETY CHECK")
    print("-" * 65)

    protected = [
        OUTPUT_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
        MANIFEST_FILE.with_suffix(
            MANIFEST_FILE.suffix + ".tmp"
        ),
    ]

    for path in protected:
        if path.exists():
            raise FileExistsError(
                "Existing artifact detected. "
                "No overwrite allowed:\n"
                f"{path}"
            )

    print("Final output: AVAILABLE")
    print("Staging output: AVAILABLE")
    print("Manifest output: AVAILABLE")


# ============================================================
# 4. DISCOVER AND VALIDATE SOURCES
# ============================================================

def discover_sources():
    print("\nDISCOVERING AMPS LABOR SOURCE BATCHES")
    print("-" * 65)

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source directory missing: {SOURCE_DIR}"
        )

    expected_names = [
        f"labor_batch_{i:05d}.csv"
        for i in range(1, EXPECTED_FILES + 1)
    ]

    actual_names = {
        p.name
        for p in SOURCE_DIR.iterdir()
        if p.is_file() and p.suffix.lower() == ".csv"
    }

    if set(expected_names) != actual_names:
        missing = sorted(
            set(expected_names) - actual_names
        )
        extra = sorted(
            actual_names - set(expected_names)
        )

        raise ValueError(
            f"Source inventory mismatch.\n"
            f"Missing: {missing[:10]}\n"
            f"Unexpected: {extra[:10]}"
        )

    files = [
        SOURCE_DIR / name
        for name in expected_names
    ]

    reference_header = None
    total_source_bytes = 0

    for path in files:
        if path.stat().st_size == 0:
            raise ValueError(
                f"Empty CSV file: {path.name}"
            )

        header = read_header(path)

        if reference_header is None:
            reference_header = header

        if len(header) != EXPECTED_COLUMNS:
            raise ValueError(
                f"Unexpected column count: {path.name}"
            )

        if header != reference_header:
            raise ValueError(
                f"CSV header mismatch: {path.name}"
            )

        if len(set(header)) != len(header):
            raise ValueError(
                f"Duplicate CSV fields: {path.name}"
            )

        total_source_bytes += path.stat().st_size

    for name in REQUIRED_COLUMNS:
        if name not in reference_header:
            raise ValueError(
                f"Required field missing: {name}"
            )

    for name in METADATA_COLUMNS:
        if name in reference_header:
            raise ValueError(
                f"Metadata field already exists: {name}"
            )

    print("Source batches:", len(files))
    print("Source columns:", len(reference_header))
    print("Source bytes:", f"{total_source_bytes:,}")
    print("Source header validation: PASS")

    return files, reference_header


# ============================================================
# 5. SPARK SESSION AND SCHEMAS
# ============================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("AssetFusion360_AMPS_Labor_Bronze")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.csv.parser.columnPruning.enabled",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_spark_schema(columns):
    return StructType([
        StructField(name, StringType(), True)
        for name in columns
    ])


def create_arrow_schema(columns):
    return pa.schema([
        pa.field(name, pa.string(), nullable=True)
        for name in columns + METADATA_COLUMNS
    ])


def read_spark_csv(spark, path, schema, columns):
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
        .schema(schema)
        .csv(str(path.resolve()))
    )

    if dataframe.columns != columns:
        raise RuntimeError(
            f"Spark schema mismatch: {path.name}"
        )

    return dataframe


# ============================================================
# 6. WRITE ARROW BATCH
# ============================================================

def write_arrow_batch(writer, records, schema):
    if not records:
        return 0

    table = pa.Table.from_pylist(
        records,
        schema=schema,
    )

    writer.write_table(table)
    return table.num_rows


# ============================================================
# 7. VALIDATE ONE PARQUET PART
# ============================================================

def validate_parquet_part(
    path,
    schema,
    expected_rows,
    source_file,
    ingestion_time,
    ingestion_batch,
    batch_number,
):
    parquet_file = pq.ParquetFile(str(path))

    actual_rows = parquet_file.metadata.num_rows

    if actual_rows != expected_rows:
        raise ValueError(
            f"Parquet row mismatch: {path.name}"
        )

    if not parquet_file.schema_arrow.equals(
        schema,
        check_metadata=False,
    ):
        raise ValueError(
            f"Parquet schema mismatch: {path.name}"
        )

    expected_metadata = {
        "_bronze_source_file": source_file,
        "_bronze_ingested_at_utc": ingestion_time,
        "_bronze_batch_id": ingestion_batch,
        "_bronze_source_batch_number": batch_number,
    }

    scanned = 0

    for record_batch in parquet_file.iter_batches(
        batch_size=BATCH_SIZE,
        columns=METADATA_COLUMNS,
    ):
        for index, column in enumerate(METADATA_COLUMNS):
            values = record_batch.column(index).to_pylist()

            if any(
                value != expected_metadata[column]
                for value in values
            ):
                raise ValueError(
                    f"Lineage mismatch: {path.name}, {column}"
                )

        scanned += record_batch.num_rows

    if scanned != expected_rows:
        raise ValueError(
            f"Metadata scan mismatch: {path.name}"
        )

    return {
        "rows": actual_rows,
        "row_groups": parquet_file.metadata.num_row_groups,
        "bytes": path.stat().st_size,
    }


# ============================================================
# 8. PROCESS ONE CSV BATCH
# ============================================================

def process_batch(
    spark,
    source_path,
    source_columns,
    spark_schema,
    arrow_schema,
    ingestion_time,
    ingestion_batch,
):
    filename = source_path.name
    match = SOURCE_PATTERN.fullmatch(filename)

    if match is None:
        raise ValueError(
            f"Invalid source batch filename: {filename}"
        )

    batch_number = match.group(1)

    source_hash_before = calculate_sha256(source_path)

    df = read_spark_csv(
        spark,
        source_path,
        spark_schema,
        source_columns,
    )

    parquet_filename = (
        f"labor_batch_{batch_number}.parquet"
    )

    parquet_path = STAGING_DIR / parquet_filename

    if parquet_path.exists():
        raise FileExistsError(
            f"Parquet file already exists: {parquet_path}"
        )

    writer = None
    records = []
    total_written = 0

    try:
        writer = pq.ParquetWriter(
            where=str(parquet_path),
            schema=arrow_schema,
            compression="snappy",
            version="2.6",
        )

        # Stream records from Spark in bounded batches.
        # Do not collect the complete dataset.

        for row in df.toLocalIterator():
            source_values = row.asDict(recursive=False)

            record = {
                column: source_values.get(column)
                for column in source_columns
            }

            record["_bronze_source_file"] = filename
            record["_bronze_ingested_at_utc"] = ingestion_time
            record["_bronze_batch_id"] = ingestion_batch
            record["_bronze_source_batch_number"] = (
                batch_number
            )

            records.append(record)

            if len(records) >= BATCH_SIZE:
                total_written += write_arrow_batch(
                    writer,
                    records,
                    arrow_schema,
                )
                records.clear()

        if records:
            total_written += write_arrow_batch(
                writer,
                records,
                arrow_schema,
            )
            records.clear()

    finally:
        if writer is not None:
            writer.close()

    validation = validate_parquet_part(
        path=parquet_path,
        schema=arrow_schema,
        expected_rows=total_written,
        source_file=filename,
        ingestion_time=ingestion_time,
        ingestion_batch=ingestion_batch,
        batch_number=batch_number,
    )

    source_hash_after = calculate_sha256(source_path)

    if source_hash_before != source_hash_after:
        raise RuntimeError(
            f"Source checksum changed: {filename}"
        )

    return {
        "source_file": relative_path(source_path),
        "source_filename": filename,
        "source_batch_number": batch_number,
        "source_sha256": source_hash_before,
        "source_size_bytes": source_path.stat().st_size,
        "parquet_filename": parquet_filename,
        "records": total_written,
        "parquet_row_groups": validation["row_groups"],
        "parquet_size_bytes": validation["bytes"],
        "status": "PASS",
    }


# ============================================================
# 9. GLOBAL BRONZE VALIDATION
# ============================================================

def validate_complete_bronze(results, arrow_schema):
    print("\nVALIDATING COMPLETE AMPS LABOR BRONZE")
    print("-" * 65)

    actual_files = sorted(STAGING_DIR.glob("*.parquet"))

    expected_names = {
        item["parquet_filename"]
        for item in results
    }

    if (
        len(actual_files) != EXPECTED_FILES
        or {p.name for p in actual_files} != expected_names
    ):
        raise ValueError(
            "Bronze Parquet batch inventory mismatch."
        )

    total_rows = 0
    total_row_groups = 0
    total_bytes = 0

    for item in results:
        path = STAGING_DIR / item["parquet_filename"]

        metadata = pq.read_metadata(path)
        schema = pq.read_schema(path)

        if not schema.equals(
            arrow_schema,
            check_metadata=False,
        ):
            raise ValueError(
                f"Global schema mismatch: {path.name}"
            )

        if metadata.num_rows != item["records"]:
            raise ValueError(
                f"Global row mismatch: {path.name}"
            )

        total_rows += metadata.num_rows
        total_row_groups += metadata.num_row_groups
        total_bytes += path.stat().st_size

    if total_rows != EXPECTED_ROWS:
        raise ValueError(
            "Bronze record reconciliation failed.\n"
            f"Expected: {EXPECTED_ROWS:,}\n"
            f"Actual: {total_rows:,}\n"
            "Staging output remains unpublished."
        )

    print("Parquet files:", len(actual_files))
    print("Bronze records:", f"{total_rows:,}")
    print("Bronze columns:", len(arrow_schema))
    print("Parquet row groups:", f"{total_row_groups:,}")
    print("Parquet bytes:", f"{total_bytes:,}")
    print("Global row reconciliation: PASS")
    print("Global Parquet schema: PASS")
    print("Global batch inventory: PASS")

    return {
        "records": total_rows,
        "files": len(actual_files),
        "columns": len(arrow_schema),
        "row_groups": total_row_groups,
        "bytes": total_bytes,
    }


# ============================================================
# 10. MAIN KEDRO INGESTION NODE
# ============================================================

def ingest_amps_labor_bronze():
    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 5.4D")
    print("AMPS LABOR BRONZE INGESTION")
    print("KEDRO + PYSPARK + PYARROW")
    print("=" * 70)

    validate_output_safety()

    source_files, source_columns = discover_sources()

    spark_schema = create_spark_schema(source_columns)
    arrow_schema = create_arrow_schema(source_columns)

    ingestion_time = utc_now()
    ingestion_batch = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%dT%H%M%S%fZ")
    )

    results = []
    total_written = 0
    spark = None
    spark_version = None

    try:
        spark = create_spark()
        spark_version = spark.version

        STAGING_DIR.mkdir(
            parents=False,
            exist_ok=False,
        )

        print("\nPROCESSING AMPS LABOR BATCHES")
        print("-" * 65)

        for index, source_path in enumerate(
            source_files,
            start=1,
        ):
            item = process_batch(
                spark=spark,
                source_path=source_path,
                source_columns=source_columns,
                spark_schema=spark_schema,
                arrow_schema=arrow_schema,
                ingestion_time=ingestion_time,
                ingestion_batch=ingestion_batch,
            )

            results.append(item)
            total_written += item["records"]

            print(
                f"[{index:03d}/{EXPECTED_FILES}] "
                f"{source_path.name}: "
                f"{item['records']:,} rows | "
                f"Cumulative: {total_written:,}"
            )

    finally:
        if spark is not None:
            spark.stop()

    validation = validate_complete_bronze(
        results,
        arrow_schema,
    )

    # Verify the input inventory did not change.
    final_files, final_columns = discover_sources()

    if (
        [p.name for p in final_files]
        != [p.name for p in source_files]
    ):
        raise RuntimeError(
            "Source inventory changed during ingestion."
        )

    if final_columns != source_columns:
        raise RuntimeError(
            "Source schema changed during ingestion."
        )

    print("\nPUBLISHING VERIFIED LABOR BRONZE")
    print("-" * 65)

    if OUTPUT_DIR.exists():
        raise FileExistsError(
            "Final Labor Bronze output appeared. "
            "Refusing to overwrite."
        )

    STAGING_DIR.rename(OUTPUT_DIR)

    manifest = {
        "project": "AssetFusion360",
        "phase": "Bronze",
        "step": "5.4D",
        "dataset": "AMPS Labor",
        "status": "COMPLETED",
        "orchestration": "Kedro SequentialRunner",
        "ingestion_engine": "PySpark",
        "parquet_writer": "PyArrow",
        "spark_version": spark_version,
        "pyarrow_version": pa.__version__,
        "source_directory": relative_path(SOURCE_DIR),
        "bronze_directory": relative_path(OUTPUT_DIR),
        "expected_source_batches": EXPECTED_FILES,
        "processed_source_batches": len(results),
        "expected_records": EXPECTED_ROWS,
        "bronze_records": validation["records"],
        "original_source_columns": len(source_columns),
        "bronze_columns": validation["columns"],
        "source_column_names": source_columns,
        "metadata_columns": METADATA_COLUMNS,
        "ingestion_timestamp_utc": ingestion_time,
        "ingestion_batch_id": ingestion_batch,
        "arrow_batch_size": BATCH_SIZE,
        "parquet_files": validation["files"],
        "parquet_row_groups": validation["row_groups"],
        "parquet_size_bytes": validation["bytes"],
        "validation": {
            "all_batches_present": True,
            "source_headers_match": True,
            "source_columns_preserved": True,
            "original_fields_stored_as_strings": True,
            "parquet_part_counts_verified": True,
            "lineage_metadata_verified": True,
            "source_checksums_unchanged": True,
            "total_record_count_matches_profile": True,
        },
        "processing_rules": [
            "No record filtering.",
            "No deduplication.",
            "No missing-value imputation.",
            "No business transformation.",
            "Original fields preserved as strings.",
            "Four source-lineage fields appended.",
            "One Parquet part per CSV source batch.",
        ],
        "source_batch_manifest": results,
        "created_utc": utc_now(),
    }

    save_manifest(MANIFEST_FILE, manifest)

    print("\n" + "=" * 70)
    print("AMPS LABOR BRONZE INGESTION SUMMARY")
    print("=" * 70)

    print("Source CSV batches:", len(results))
    print("Bronze Parquet files:", validation["files"])
    print("Bronze records:", f"{validation['records']:,}")
    print("Original columns:", len(source_columns))
    print("Bronze columns:", validation["columns"])
    print("Parquet row groups:", validation["row_groups"])

    print("\nBronze output:")
    print(OUTPUT_DIR)

    print("\nManifest:")
    print(MANIFEST_FILE)

    print("\nLABOR BRONZE INGESTION: SUCCESS")

    return manifest


# ============================================================
# 11. KEDRO PIPELINE
# ============================================================

def create_bronze_labor_pipeline():
    return pipeline([
        node(
            func=ingest_amps_labor_bronze,
            inputs=None,
            outputs="amps_labor_bronze_manifest",
            name="ingest_amps_labor_bronze",
            tags=[
                "bronze",
                "amps",
                "labor",
            ],
        )
    ])


# ============================================================
# 12. KEDRO RUNNER
# ============================================================

def run_kedro_pipeline():
    labor_pipeline = create_bronze_labor_pipeline()

    catalog = DataCatalog({
        "amps_labor_bronze_manifest": MemoryDataset(),
    })

    runner = SequentialRunner()

    runner.run(
        labor_pipeline,
        catalog,
    )

    # Read the actual saved manifest from Kedro.
    manifest = catalog.load(
        "amps_labor_bronze_manifest"
    )

    if not isinstance(manifest, dict):
        raise RuntimeError(
            "Kedro manifest is not a dictionary."
        )

    if manifest.get("status") != "COMPLETED":
        raise RuntimeError(
            "Kedro Labor ingestion did not complete."
        )

    print("\n" + "=" * 70)
    print("KEDRO LABOR PIPELINE: SUCCESS")
    print("=" * 70)


# ============================================================
# 13. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_kedro_pipeline()
