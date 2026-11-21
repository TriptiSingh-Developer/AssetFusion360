
"""
AssetFusion360 Capstone
Step 5.4E - AMPS Transactions Bronze Ingestion

Kedro + PySpark + PyArrow
152 CSV batches, 755,122 expected records.
Preserves 14 source fields and adds 4 lineage fields.

Does not modify raw files or existing Bronze datasets.
Refuses to overwrite any Transactions output.
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
from pyspark.sql.types import StringType, StructField, StructType


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    ROOT / "data" / "01_raw_source_data"
    / "nyc_parks_asset_management" / "transactions_batches"
)

BRONZE_ROOT = ROOT / "data" / "02_bronze_ingested_data"
OUTPUT_DIR = BRONZE_ROOT / "amps_transactions"
STAGING_DIR = BRONZE_ROOT / "_staging_amps_transactions"

MANIFEST_FILE = (
    ROOT / "reports" / "data_profiling_reports"
    / "amps_transactions_bronze_ingestion_manifest.json"
)

EXPECTED_FILES = 152
EXPECTED_ROWS = 755_122
EXPECTED_COLUMNS = 14
ARROW_BATCH_SIZE = 5000

REQUIRED_COLUMNS = [
    "tra_code",
    "tra_type",
    "tra_date",
    "tra_status",
    "tra_sqlidentity",
]

LINEAGE_COLUMNS = [
    "_bronze_source_file",
    "_bronze_ingested_at_utc",
    "_bronze_batch_id",
    "_bronze_source_batch_number",
]


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def relative(path):
    return path.relative_to(ROOT).as_posix()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024), b""
        ):
            digest.update(chunk)
    return digest.hexdigest()


def read_header(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        return next(csv.reader(handle), [])


def expected_source_paths():
    return [
        SOURCE_DIR / f"transactions_batch_{i:05d}.csv"
        for i in range(1, EXPECTED_FILES + 1)
    ]


def ensure_safe_output():
    protected = [
        OUTPUT_DIR,
        STAGING_DIR,
        MANIFEST_FILE,
        MANIFEST_FILE.with_suffix(".json.tmp"),
    ]

    for path in protected:
        if path.exists():
            raise FileExistsError(
                f"Existing output must not be overwritten: {path}"
            )

    if not BRONZE_ROOT.is_dir():
        raise FileNotFoundError(
            f"Bronze parent directory missing: {BRONZE_ROOT}"
        )

    print("OUTPUT SAFETY CHECK: PASS")


# ------------------------------------------------------------
# Source inventory and schema validation
# ------------------------------------------------------------

def validate_sources():
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(SOURCE_DIR)

    files = expected_source_paths()

    actual_names = {
        p.name for p in SOURCE_DIR.iterdir()
        if p.is_file() and p.suffix.lower() == ".csv"
    }
    expected_names = {p.name for p in files}

    if actual_names != expected_names:
        raise ValueError(
            "Source inventory mismatch. "
            f"Missing: {sorted(expected_names - actual_names)[:10]}; "
            f"Unexpected: {sorted(actual_names - expected_names)[:10]}"
        )

    reference = read_header(files[0])

    if len(reference) != EXPECTED_COLUMNS:
        raise ValueError("Incorrect source column count.")

    if len(set(reference)) != len(reference):
        raise ValueError("Duplicate source column names.")

    if any(col not in reference for col in REQUIRED_COLUMNS):
        raise ValueError("Required transaction fields missing.")

    if any(col in reference for col in LINEAGE_COLUMNS):
        raise ValueError("Source and lineage column conflict.")

    for path in files:
        if path.stat().st_size == 0:
            raise ValueError(f"Empty source file: {path.name}")

        if read_header(path) != reference:
            raise ValueError(
                f"Source schema mismatch: {path.name}"
            )

    print(f"Validated CSV batches: {len(files)}")
    print(f"Original columns: {len(reference)}")
    print("SOURCE INVENTORY AND SCHEMA: PASS")

    return files, reference


# ------------------------------------------------------------
# Spark and Arrow setup
# ------------------------------------------------------------

def make_spark():
    spark = (
        SparkSession.builder
        .appName("AssetFusion360_AMPS_Transactions_Bronze")
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


def schemas(columns):
    spark_schema = StructType([
        StructField(c, StringType(), True)
        for c in columns
    ])

    arrow_schema = pa.schema([
        pa.field(c, pa.string(), nullable=True)
        for c in columns + LINEAGE_COLUMNS
    ])

    return spark_schema, arrow_schema


# ------------------------------------------------------------
# Validate each Parquet part
# ------------------------------------------------------------

def verify_part(
    path, arrow_schema, row_count, expected_lineage
):
    parquet = pq.ParquetFile(str(path))

    if parquet.metadata.num_rows != row_count:
        raise ValueError(
            f"Row count mismatch: {path.name}"
        )

    if not parquet.schema_arrow.equals(
        arrow_schema, check_metadata=False
    ):
        raise ValueError(
            f"Parquet schema mismatch: {path.name}"
        )

    checked = 0

    for batch in parquet.iter_batches(
        batch_size=ARROW_BATCH_SIZE,
        columns=LINEAGE_COLUMNS,
    ):
        for index, col in enumerate(LINEAGE_COLUMNS):
            values = batch.column(index).to_pylist()

            if any(
                value != expected_lineage[col]
                for value in values
            ):
                raise ValueError(
                    f"Lineage mismatch in {path.name}: {col}"
                )

        checked += batch.num_rows

    if checked != row_count:
        raise ValueError(
            f"Lineage row scan failed: {path.name}"
        )

    return parquet.metadata.num_row_groups


# ------------------------------------------------------------
# Process one source batch
# ------------------------------------------------------------

def process_batch(
    spark, csv_path, index, columns,
    spark_schema, arrow_schema, timestamp, run_id
):
    source_hash_before = sha256(csv_path)
    batch_number = f"{index:05d}"

    parquet_name = (
        f"transactions_batch_{batch_number}.parquet"
    )
    parquet_path = STAGING_DIR / parquet_name

    if parquet_path.exists():
        raise FileExistsError(parquet_path)

    df = (
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

    if df.columns != columns:
        raise ValueError(
            f"Spark schema mismatch: {csv_path.name}"
        )

    lineage = {
        "_bronze_source_file": csv_path.name,
        "_bronze_ingested_at_utc": timestamp,
        "_bronze_batch_id": run_id,
        "_bronze_source_batch_number": batch_number,
    }

    records = []
    written = 0

    with pq.ParquetWriter(
        str(parquet_path),
        arrow_schema,
        compression="snappy",
        version="2.6",
    ) as writer:

        for row in df.toLocalIterator():
            values = row.asDict(recursive=False)

            record = {
                name: values.get(name)
                for name in columns
            }
            record.update(lineage)
            records.append(record)

            if len(records) == ARROW_BATCH_SIZE:
                table = pa.Table.from_pylist(
                    records, schema=arrow_schema
                )
                writer.write_table(table)
                written += table.num_rows
                records.clear()

        if records:
            table = pa.Table.from_pylist(
                records, schema=arrow_schema
            )
            writer.write_table(table)
            written += table.num_rows

    row_groups = verify_part(
        parquet_path, arrow_schema, written, lineage
    )

    if sha256(csv_path) != source_hash_before:
        raise RuntimeError(
            f"Source checksum changed: {csv_path.name}"
        )

    return {
        "source_file": relative(csv_path),
        "source_filename": csv_path.name,
        "source_batch_number": batch_number,
        "source_sha256": source_hash_before,
        "source_size_bytes": csv_path.stat().st_size,
        "parquet_filename": parquet_name,
        "records": written,
        "parquet_row_groups": row_groups,
        "parquet_size_bytes": parquet_path.stat().st_size,
        "status": "PASS",
    }


# ------------------------------------------------------------
# Validate complete staged dataset
# ------------------------------------------------------------

def validate_complete(results, arrow_schema):
    expected_names = {
        x["parquet_filename"] for x in results
    }
    actual_files = list(STAGING_DIR.glob("*.parquet"))
    actual_names = {p.name for p in actual_files}

    if (
        len(results) != EXPECTED_FILES
        or len(actual_files) != EXPECTED_FILES
        or actual_names != expected_names
    ):
        raise ValueError("Parquet batch inventory mismatch.")

    total_rows = 0
    total_groups = 0
    total_bytes = 0

    for item in results:
        path = STAGING_DIR / item["parquet_filename"]
        metadata = pq.read_metadata(path)
        schema = pq.read_schema(path)

        if not schema.equals(
            arrow_schema, check_metadata=False
        ):
            raise ValueError(
                f"Schema mismatch: {path.name}"
            )

        if metadata.num_rows != item["records"]:
            raise ValueError(
                f"Record mismatch: {path.name}"
            )

        total_rows += metadata.num_rows
        total_groups += metadata.num_row_groups
        total_bytes += path.stat().st_size

    if total_rows != EXPECTED_ROWS:
        raise ValueError(
            "Transaction record reconciliation failed. "
            f"Expected {EXPECTED_ROWS:,}; got {total_rows:,}. "
            "Staging will remain unpublished."
        )

    print("\nGLOBAL TRANSACTIONS VALIDATION")
    print("-" * 60)
    print(f"Parquet files: {len(actual_files)}")
    print(f"Bronze records: {total_rows:,}")
    print(f"Bronze columns: {len(arrow_schema)}")
    print(f"Parquet row groups: {total_groups}")
    print(f"Total Parquet bytes: {total_bytes:,}")
    print("GLOBAL VALIDATION: PASS")

    return {
        "records": total_rows,
        "files": len(actual_files),
        "columns": len(arrow_schema),
        "row_groups": total_groups,
        "parquet_bytes": total_bytes,
    }


# ------------------------------------------------------------
# Kedro ingestion node
# ------------------------------------------------------------

def ingest_amps_transactions_bronze():
    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 5.4E")
    print("AMPS TRANSACTIONS BRONZE INGESTION")
    print("=" * 70)

    ensure_safe_output()
    files, columns = validate_sources()
    spark_schema, arrow_schema = schemas(columns)

    timestamp = utc_now()
    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    results = []
    cumulative = 0
    spark = None
    spark_version = None

    try:
        spark = make_spark()
        spark_version = spark.version

        STAGING_DIR.mkdir(
            parents=False, exist_ok=False
        )

        for index, path in enumerate(files, 1):
            item = process_batch(
                spark, path, index, columns,
                spark_schema, arrow_schema, timestamp, run_id
            )
            results.append(item)
            cumulative += item["records"]

            print(
                f"[{index:03d}/{EXPECTED_FILES}] "
                f"{path.name}: {item['records']:,} rows | "
                f"Cumulative: {cumulative:,}",
                flush=True,
            )
    finally:
        if spark is not None:
            spark.stop()

    validation = validate_complete(
        results, arrow_schema
    )

    # Confirm source names and headers remain unchanged.
    final_files, final_columns = validate_sources()
    if (
        [p.name for p in final_files]
        != [p.name for p in files]
        or final_columns != columns
    ):
        raise RuntimeError(
            "Source inventory changed during processing."
        )

    # Independently verify source hashes once more
    # before publishing the staged Parquet dataset.
    for item, source_path in zip(results, files):
        if sha256(source_path) != item["source_sha256"]:
            raise RuntimeError(
                f"Source changed: {source_path.name}"
            )

    if OUTPUT_DIR.exists() or MANIFEST_FILE.exists():
        raise FileExistsError(
            "Final output appeared during processing."
        )

    STAGING_DIR.rename(OUTPUT_DIR)

    manifest = {
        "project": "AssetFusion360",
        "phase": "Bronze",
        "step": "5.4E",
        "dataset": "AMPS Transactions",
        "status": "COMPLETED",
        "orchestration": "Kedro SequentialRunner",
        "ingestion_engine": "PySpark",
        "parquet_writer": "PyArrow",
        "spark_version": spark_version,
        "pyarrow_version": pa.__version__,
        "source_directory": relative(SOURCE_DIR),
        "bronze_directory": relative(OUTPUT_DIR),
        "expected_source_batches": EXPECTED_FILES,
        "processed_source_batches": len(results),
        "expected_records": EXPECTED_ROWS,
        "bronze_records": validation["records"],
        "original_source_columns": len(columns),
        "bronze_columns": validation["columns"],
        "source_column_names": columns,
        "metadata_columns": LINEAGE_COLUMNS,
        "ingestion_timestamp_utc": timestamp,
        "ingestion_batch_id": run_id,
        "arrow_batch_size": ARROW_BATCH_SIZE,
        "parquet_files": validation["files"],
        "parquet_row_groups": validation["row_groups"],
        "parquet_size_bytes": validation["parquet_bytes"],
        "validation": {
            "batch_inventory": "PASS",
            "source_schema": "PASS",
            "parquet_schema": "PASS",
            "parquet_record_counts": "PASS",
            "lineage_metadata": "PASS",
            "source_checksums": "PASS",
            "total_row_reconciliation": "PASS",
        },
        "processing_rules": [
            "No filtering, deduplication or imputation",
            "All original fields retained as strings",
            "Four source-lineage metadata fields appended",
            "One Parquet file per source CSV batch",
        ],
        "source_batch_manifest": results,
        "created_utc": utc_now(),
    }

    MANIFEST_FILE.parent.mkdir(
        parents=True, exist_ok=True
    )

    with MANIFEST_FILE.open(
        "x", encoding="utf-8"
    ) as handle:
        json.dump(
            manifest, handle, indent=2,
            ensure_ascii=False,
        )

    print("\n" + "=" * 70)
    print("AMPS TRANSACTIONS BRONZE INGESTION SUMMARY")
    print("=" * 70)
    print("Source CSV batches:", len(results))
    print("Bronze Parquet files:", validation["files"])
    print("Bronze records:", f"{validation['records']:,}")
    print("Original columns:", len(columns))
    print("Bronze columns:", validation["columns"])
    print("Parquet row groups:", validation["row_groups"])
    print("Bronze output:", OUTPUT_DIR)
    print("Manifest:", MANIFEST_FILE)
    print("\nTRANSACTIONS BRONZE INGESTION: SUCCESS")

    return manifest


# ------------------------------------------------------------
# Kedro pipeline and runner
# ------------------------------------------------------------

def create_bronze_transactions_pipeline():
    return pipeline([
        node(
            func=ingest_amps_transactions_bronze,
            inputs=None,
            outputs="amps_transactions_bronze_manifest",
            name="ingest_amps_transactions_bronze",
            tags=["bronze", "amps", "transactions"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "amps_transactions_bronze_manifest": MemoryDataset()
    })

    runner = SequentialRunner()
    runner.run(
        create_bronze_transactions_pipeline(),
        catalog,
    )

    manifest = catalog.load(
        "amps_transactions_bronze_manifest"
    )

    if (
        not isinstance(manifest, dict)
        or manifest.get("status") != "COMPLETED"
    ):
        raise RuntimeError(
            "Kedro Transactions manifest validation failed."
        )

    print("\nKEDRO TRANSACTIONS PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
