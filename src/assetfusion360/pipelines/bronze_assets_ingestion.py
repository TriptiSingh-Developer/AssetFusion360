
"""
AssetFusion360 Capstone Project
PHASE 5 - STEP 5.2

Kedro + PySpark + PyArrow
AMPS Assets Bronze Ingestion

Purpose
-------
Read the NYC Parks AMPS Assets CSV using PySpark,
then write local Bronze Parquet using PyArrow.

This avoids Hadoop's Windows filesystem writer.

Source
------
data/01_raw_source_data/
    nyc_parks_asset_management/AMPS_Assets.csv

Bronze output
-------------
data/02_bronze_ingested_data/amps_assets/

Manifest
--------
reports/data_profiling_reports/
    amps_assets_bronze_ingestion_manifest.json

Rules
-----
1. Keep all source records.
2. Preserve all 29 source columns as strings.
3. Do not clean or deduplicate source data.
4. Add three Bronze metadata fields.
5. Use incremental Arrow batches.
6. Validate source and Parquet record counts.
7. Validate the complete Parquet schema.
8. Verify source checksum after processing.
9. Never overwrite an existing Bronze dataset.
10. Run the ingestion through Kedro.

Python: 3.12
Kedro: 1.8.0
PySpark: 3.5.7
PyArrow: 26.0.1
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
    StructField,
    StringType,
    StructType,
)


# ============================================================
# 1. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_FILE = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
    / "AMPS_Assets.csv"
)

BRONZE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "02_bronze_ingested_data"
)

BRONZE_OUTPUT = (
    BRONZE_ROOT
    / "amps_assets"
)

STAGING_OUTPUT = (
    BRONZE_ROOT
    / "_staging_amps_assets"
)

PARQUET_FILENAME = "part-00000.parquet"

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

MANIFEST_FILE = (
    REPORT_DIR
    / "amps_assets_bronze_ingestion_manifest.json"
)


# ============================================================
# 2. CONSTANTS
# ============================================================

EXPECTED_SOURCE_ROWS = 148522
EXPECTED_SOURCE_COLUMNS = 29

BATCH_SIZE = 5000

METADATA_COLUMNS = [
    "_bronze_source_file",
    "_bronze_ingested_at_utc",
    "_bronze_batch_id",
]

REQUIRED_SOURCE_COLUMNS = [
    "OBJ_CODE",
    "OBJ_SQLIDENTITY",
]


# ============================================================
# 3. COMMON HELPERS
# ============================================================

def relative_path(path):

    return path.relative_to(
        PROJECT_ROOT
    ).as_posix()


def calculate_sha256(path):

    digest = hashlib.sha256()

    with path.open("rb") as handle:

        while True:

            block = handle.read(
                1024 * 1024
            )

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def write_json_atomic(path, data):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8"
    ) as handle:

        json.dump(
            data,
            handle,
            indent=2,
            ensure_ascii=False
        )

    os.replace(
        temporary,
        path
    )


# ============================================================
# 4. VALIDATE RAW SOURCE
# ============================================================

def validate_source():

    print("\nVALIDATING RAW SOURCE")
    print("-" * 60)

    if not SOURCE_FILE.is_file():

        raise FileNotFoundError(
            f"AMPS Assets source missing:\n{SOURCE_FILE}"
        )

    if SOURCE_FILE.stat().st_size == 0:

        raise ValueError(
            "Source CSV file is empty."
        )

    with SOURCE_FILE.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as handle:

        header = next(
            csv.reader(handle),
            None
        )

    if not header:

        raise ValueError(
            "Source CSV header not found."
        )

    if len(header) != EXPECTED_SOURCE_COLUMNS:

        raise ValueError(
            "Unexpected source column count. "
            f"Expected {EXPECTED_SOURCE_COLUMNS}, "
            f"found {len(header)}."
        )

    if (
        len(set(header)) != len(header)
        or any(not name.strip() for name in header)
    ):

        raise ValueError(
            "Duplicate or empty source column names."
        )

    for required in REQUIRED_SOURCE_COLUMNS:

        if required not in header:

            raise ValueError(
                f"Required source field missing: {required}"
            )

    for metadata_name in METADATA_COLUMNS:

        if metadata_name in header:

            raise ValueError(
                "Reserved metadata column already "
                f"exists: {metadata_name}"
            )

    source_hash = calculate_sha256(
        SOURCE_FILE
    )

    print(
        f"Source: {SOURCE_FILE.name}"
    )

    print(
        f"Columns: {len(header)}"
    )

    print(
        "Source bytes: "
        f"{SOURCE_FILE.stat().st_size:,}"
    )

    print(
        f"SHA256: {source_hash}"
    )

    return header, source_hash


# ============================================================
# 5. ENSURE EXISTING OUTPUTS ARE PROTECTED
# ============================================================

def validate_destinations():

    if BRONZE_OUTPUT.exists():

        raise FileExistsError(
            "Final Bronze dataset already exists. "
            "Refusing to overwrite:\n"
            f"{BRONZE_OUTPUT}"
        )

    if STAGING_OUTPUT.exists():

        raise FileExistsError(
            "A staging directory already exists. "
            "Inspect it before attempting another run:\n"
            f"{STAGING_OUTPUT}"
        )

    if MANIFEST_FILE.exists():

        raise FileExistsError(
            "The Bronze ingestion manifest already exists. "
            "Refusing to overwrite:\n"
            f"{MANIFEST_FILE}"
        )

    BRONZE_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )


# ============================================================
# 6. CREATE PYSPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_Bronze_AMPS_Assets_PyArrow"
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
# 7. READ CSV USING PYSPARK
# ============================================================

def load_source_with_spark(
    spark,
    header
):

    schema = StructType([
        StructField(
            column_name,
            StringType(),
            True
        )
        for column_name in header
    ])

    source_df = (
        spark.read
        .option(
            "header",
            "true"
        )
        .option(
            "sep",
            ","
        )
        .option(
            "quote",
            '"'
        )
        .option(
            "escape",
            '"'
        )
        .option(
            "multiLine",
            "true"
        )
        .option(
            "enforceSchema",
            "false"
        )
        .option(
            "inferSchema",
            "false"
        )
        .option(
            "mode",
            "FAILFAST"
        )
        .option(
            "encoding",
            "UTF-8"
        )
        .schema(schema)
        .csv(
            str(SOURCE_FILE.resolve())
        )
    )

    if source_df.columns != header:

        raise RuntimeError(
            "Spark source schema mismatch."
        )

    return source_df


# ============================================================
# 8. BUILD ARROW SCHEMA
# ============================================================

def build_arrow_schema(header):

    all_columns = (
        header
        + METADATA_COLUMNS
    )

    schema = pa.schema([
        pa.field(
            name,
            pa.string(),
            nullable=True
        )
        for name in all_columns
    ])

    return schema


# ============================================================
# 9. WRITE INCREMENTAL ARROW BATCH
# ============================================================

def write_batch(
    writer,
    records,
    arrow_schema
):

    if not records:
        return 0

    table = pa.Table.from_pylist(
        records,
        schema=arrow_schema
    )

    writer.write_table(
        table
    )

    return table.num_rows


# ============================================================
# 10. WRITE BRONZE PARQUET THROUGH PYARROW
# ============================================================

def write_bronze_parquet(
    source_df,
    header,
    ingestion_timestamp,
    batch_id
):

    print("\nWRITING BRONZE PARQUET USING PYARROW")
    print("-" * 60)

    STAGING_OUTPUT.mkdir(
        parents=False,
        exist_ok=False
    )

    parquet_path = (
        STAGING_OUTPUT
        / PARQUET_FILENAME
    )

    arrow_schema = build_arrow_schema(
        header
    )

    writer = None

    batch_records = []

    written_records = 0
    number_of_batches = 0

    try:

        writer = pq.ParquetWriter(
            where=str(parquet_path),
            schema=arrow_schema,
            compression="snappy",
            version="2.6"
        )

        # Stream source records from Spark to Python.
        # Do not collect the entire dataset into memory.

        for row in source_df.toLocalIterator():

            values = row.asDict(
                recursive=False
            )

            record = {
                column_name: values.get(
                    column_name
                )
                for column_name in header
            }

            record["_bronze_source_file"] = (
                SOURCE_FILE.name
            )

            record["_bronze_ingested_at_utc"] = (
                ingestion_timestamp
            )

            record["_bronze_batch_id"] = (
                batch_id
            )

            batch_records.append(
                record
            )

            if len(batch_records) >= BATCH_SIZE:

                written_records += write_batch(
                    writer,
                    batch_records,
                    arrow_schema
                )

                number_of_batches += 1

                print(
                    "Written: "
                    f"{written_records:,} records"
                )

                batch_records.clear()

        if batch_records:

            written_records += write_batch(
                writer,
                batch_records,
                arrow_schema
            )

            number_of_batches += 1

            batch_records.clear()

    finally:

        if writer is not None:
            writer.close()

    if not parquet_path.is_file():

        raise RuntimeError(
            "Parquet file was not created."
        )

    print("\nPYARROW WRITE COMPLETED")

    print(
        f"Records written: {written_records:,}"
    )

    print(
        f"Arrow batches: {number_of_batches}"
    )

    print(
        "Parquet file size: "
        f"{parquet_path.stat().st_size:,} bytes"
    )

    return {
        "staged_file": parquet_path,
        "records_written": written_records,
        "arrow_batches": number_of_batches,
        "arrow_schema": arrow_schema,
    }


# ============================================================
# 11. VALIDATE PARQUET CONTENT
# ============================================================

def validate_parquet(
    write_result,
    header,
    expected_count,
    ingestion_timestamp,
    batch_id
):

    print("\nVALIDATING BRONZE PARQUET")
    print("-" * 60)

    parquet_path = write_result[
        "staged_file"
    ]

    parquet_file = pq.ParquetFile(
        str(parquet_path)
    )

    actual_rows = (
        parquet_file.metadata.num_rows
    )

    actual_schema = (
        parquet_file.schema_arrow
    )

    expected_schema = build_arrow_schema(
        header
    )

    if actual_rows != expected_count:

        raise ValueError(
            "Bronze row count mismatch. "
            f"Expected {expected_count:,}, "
            f"found {actual_rows:,}."
        )

    if not actual_schema.equals(
        expected_schema,
        check_metadata=False
    ):

        raise ValueError(
            "Bronze schema does not match "
            "the expected schema."
        )

    if write_result[
        "records_written"
    ] != actual_rows:

        raise ValueError(
            "Written row count differs from "
            "Parquet metadata row count."
        )

    total_scanned = 0

    # Re-read the three ingestion metadata columns.
    # This checks every row without loading the
    # full Parquet dataset into memory.

    metadata_batches = parquet_file.iter_batches(
        batch_size=BATCH_SIZE,
        columns=METADATA_COLUMNS
    )

    for batch in metadata_batches:

        metadata_table = pa.Table.from_batches(
            [batch]
        )

        source_values = (
            metadata_table[
                "_bronze_source_file"
            ].to_pylist()
        )

        timestamp_values = (
            metadata_table[
                "_bronze_ingested_at_utc"
            ].to_pylist()
        )

        batch_values = (
            metadata_table[
                "_bronze_batch_id"
            ].to_pylist()
        )

        for source, timestamp, record_batch in zip(
            source_values,
            timestamp_values,
            batch_values
        ):

            if source != SOURCE_FILE.name:

                raise ValueError(
                    "Unexpected Bronze source filename."
                )

            if timestamp != ingestion_timestamp:

                raise ValueError(
                    "Unexpected ingestion timestamp."
                )

            if record_batch != batch_id:

                raise ValueError(
                    "Unexpected ingestion batch identifier."
                )

        total_scanned += batch.num_rows

    if total_scanned != actual_rows:

        raise ValueError(
            "Metadata scan did not cover all "
            "Bronze records."
        )

    # Validate that original source fields remain strings.

    for column_name in header:

        actual_type = actual_schema.field(
            column_name
        ).type

        if not pa.types.is_string(
            actual_type
        ):

            raise ValueError(
                f"Unexpected type for {column_name}"
            )

    print(
        f"Verified records: {actual_rows:,}"
    )

    print(
        f"Original source columns: {len(header)}"
    )

    print(
        f"Metadata columns: {len(METADATA_COLUMNS)}"
    )

    print(
        f"Total columns: {len(actual_schema)}"
    )

    print(
        "Record count validation: PASS"
    )

    print(
        "Parquet schema validation: PASS"
    )

    print(
        "Ingestion metadata validation: PASS"
    )

    return {
        "parquet_records": actual_rows,
        "parquet_columns": len(
            actual_schema
        ),
        "parquet_metadata_records_scanned": (
            total_scanned
        ),
        "parquet_row_groups": (
            parquet_file.metadata.num_row_groups
        ),
    }


# ============================================================
# 12. KEDRO BRONZE INGESTION NODE
# ============================================================

def ingest_amps_assets_bronze():

    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 5.2")
    print("KEDRO + PYSPARK + PYARROW BRONZE INGESTION")
    print("=" * 70)

    header, source_hash_before = (
        validate_source()
    )

    validate_destinations()

    ingestion_timestamp = (
        datetime.now(timezone.utc)
        .isoformat()
    )

    batch_id = (
        datetime.now(timezone.utc)
        .strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
    )

    spark = None
    spark_version = None
    source_count = None
    write_result = None
    validation_result = None

    try:

        spark = create_spark()

        spark_version = spark.version

        print("\nREADING SOURCE WITH PYSPARK")
        print("-" * 60)

        source_df = load_source_with_spark(
            spark,
            header
        )

        source_count = (
            source_df.count()
        )

        print(
            f"Source records: {source_count:,}"
        )

        if source_count != EXPECTED_SOURCE_ROWS:

            raise ValueError(
                "Source count does not match previous "
                "profiling. "
                f"Expected {EXPECTED_SOURCE_ROWS:,}, "
                f"found {source_count:,}."
            )

        write_result = write_bronze_parquet(
            source_df,
            header,
            ingestion_timestamp,
            batch_id
        )

        validation_result = validate_parquet(
            write_result,
            header,
            source_count,
            ingestion_timestamp,
            batch_id
        )

    finally:

        if spark is not None:
            spark.stop()

    # --------------------------------------------------------
    # Verify immutable source after ingestion
    # --------------------------------------------------------

    source_hash_after = calculate_sha256(
        SOURCE_FILE
    )

    if source_hash_before != source_hash_after:

        raise RuntimeError(
            "Source checksum changed during ingestion. "
            "Do not publish the staged Bronze output."
        )

    print("\nSOURCE INTEGRITY")
    print("-" * 60)

    print(
        "Source SHA256 unchanged: PASS"
    )

    # --------------------------------------------------------
    # Publish validated staged data
    # --------------------------------------------------------

    if BRONZE_OUTPUT.exists():

        raise FileExistsError(
            "Final Bronze destination appeared "
            "during processing. Refusing to overwrite."
        )

    STAGING_OUTPUT.rename(
        BRONZE_OUTPUT
    )

    final_parquet = (
        BRONZE_OUTPUT
        / PARQUET_FILENAME
    )

    manifest = {
        "project": "AssetFusion360",
        "phase": "Bronze",
        "step": "5.2",
        "dataset": "AMPS Assets",
        "status": "COMPLETED",
        "orchestration": (
            "Kedro SequentialRunner"
        ),
        "ingestion_engine": "PySpark",
        "local_parquet_writer": "PyArrow",
        "spark_version": spark_version,
        "pyarrow_version": pa.__version__,
        "source_file": relative_path(
            SOURCE_FILE
        ),
        "bronze_directory": relative_path(
            BRONZE_OUTPUT
        ),
        "bronze_parquet_file": relative_path(
            final_parquet
        ),
        "source_sha256": source_hash_before,
        "source_records": source_count,
        "bronze_records": validation_result[
            "parquet_records"
        ],
        "original_source_columns": len(
            header
        ),
        "bronze_columns": validation_result[
            "parquet_columns"
        ],
        "source_column_names": header,
        "bronze_metadata_columns": (
            METADATA_COLUMNS
        ),
        "ingestion_timestamp_utc": (
            ingestion_timestamp
        ),
        "batch_id": batch_id,
        "batch_size": BATCH_SIZE,
        "arrow_batches_written": (
            write_result["arrow_batches"]
        ),
        "parquet_row_groups": (
            validation_result[
                "parquet_row_groups"
            ]
        ),
        "parquet_file_size_bytes": (
            final_parquet.stat().st_size
        ),
        "validation_results": {
            "source_count_matches_profile": True,
            "bronze_count_matches_source": True,
            "all_source_columns_retained": True,
            "original_columns_are_strings": True,
            "ingestion_metadata_valid": True,
            "source_checksum_unchanged": True,
        },
        "processing_rules": [
            "No source record filtering.",
            "No business-key deduplication.",
            "No source column removal.",
            "No missing-value imputation.",
            "No numeric or date type conversions.",
            "Source values follow Spark CSV parsing semantics.",
            "Original source columns stored as strings.",
            "Three ingestion metadata columns appended.",
        ],
        "created_utc": (
            datetime.now(timezone.utc)
            .isoformat()
        ),
    }

    write_json_atomic(
        MANIFEST_FILE,
        manifest
    )

    print("\n" + "=" * 70)
    print("AMPS ASSETS BRONZE INGESTION SUMMARY")
    print("=" * 70)

    print(
        f"Source records: {source_count:,}"
    )

    print(
        "Bronze records: "
        f"{validation_result['parquet_records']:,}"
    )

    print(
        f"Source columns: {len(header)}"
    )

    print(
        "Bronze columns: "
        f"{validation_result['parquet_columns']}"
    )

    print(
        "Arrow batches written: "
        f"{write_result['arrow_batches']}"
    )

    print(
        "Parquet row groups: "
        f"{validation_result['parquet_row_groups']}"
    )

    print("\nBronze output:")
    print(BRONZE_OUTPUT)

    print("\nManifest:")
    print(MANIFEST_FILE)

    print("\n" + "=" * 70)
    print("BRONZE INGESTION COMPLETED SUCCESSFULLY")
    print("=" * 70)

    return manifest


# ============================================================
# 13. KEDRO PIPELINE
# ============================================================

def create_bronze_assets_pipeline():

    return pipeline([
        node(
            func=ingest_amps_assets_bronze,
            inputs=None,
            outputs="amps_assets_bronze_manifest",
            name="ingest_amps_assets_bronze",
            tags=[
                "bronze",
                "amps",
                "assets"
            ],
        )
    ])


# ============================================================
# 14. KEDRO RUNNER
# ============================================================

def run_kedro_pipeline():

    bronze_pipeline = (
        create_bronze_assets_pipeline()
    )

    catalog = DataCatalog({
        "amps_assets_bronze_manifest": (
            MemoryDataset()
        )
    })

    runner = SequentialRunner()

    result = runner.run(
        bronze_pipeline,
        catalog
    )

    manifest = catalog.load(
    "amps_assets_bronze_manifest"
    )

    if not manifest:

        raise RuntimeError(
            "Kedro did not return "
            "the ingestion manifest."
        )

    if manifest.get("status") != "COMPLETED":

        raise RuntimeError(
            "Kedro ingestion status was not completed."
        )

    print("\n" + "=" * 70)
    print("KEDRO PIPELINE EXECUTION: SUCCESS")
    print("=" * 70)


# ============================================================
# 15. MAIN
# ============================================================

if __name__ == "__main__":

    run_kedro_pipeline()
