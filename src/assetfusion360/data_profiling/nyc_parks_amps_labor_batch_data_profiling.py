
"""
AssetFusion360 Capstone Project

STEP 4.6C
NYC Parks AMPS - Labor Batch Schema and Data Quality Profiling

Purpose:
    1. Validate all 647 labor CSV batches.
    2. Check source schema consistency.
    3. Load labor data with PySpark.
    4. Calculate records and missing values.
    5. Count placeholder '-' values separately.
    6. Discover potential identifier and relationship columns.
    7. Check candidate identifier uniqueness.
    8. Inspect sample labor records.
    9. Save a complete JSON profiling report.

Technology:
    Python 3.12
    PySpark 3.5.7
    No pandas

Source:
    data/01_raw_source_data/
        nyc_parks_asset_management/
            labor_batches/

Report:
    reports/data_profiling_reports/
        nyc_parks_amps_labor_batch_data_profile.json

Original source files are never modified.
"""

import csv
import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

# Configure the current Python interpreter for Spark.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
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
    / "labor_batches"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_parks_amps_labor_batch_data_profile.json"
)

EXPECTED_FILE_COUNT = 647
EXPECTED_COLUMN_COUNT = 18


# ============================================================
# 2. DISCOVER AND VALIDATE LABOR CSV FILES
# ============================================================

def validate_source_files():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source directory not found: {SOURCE_DIR}"
        )

    files = sorted(
        SOURCE_DIR.glob("*.csv"),
        key=lambda path: path.name.lower()
    )

    if len(files) != EXPECTED_FILE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_FILE_COUNT} CSV files, "
            f"found {len(files)}."
        )

    empty_files = [
        str(file_path)
        for file_path in files
        if file_path.stat().st_size == 0
    ]

    if empty_files:
        raise ValueError(
            f"Empty CSV files detected: {empty_files[:10]}"
        )

    print("\n" + "=" * 65)
    print("NYC PARKS AMPS - LABOR SOURCE INVENTORY")
    print("=" * 65)

    print(f"CSV batch files: {len(files):,}")

    return files


# ============================================================
# 3. READ AND VALIDATE ALL CSV HEADERS
# ============================================================

def read_csv_header(file_path):

    with file_path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as source:

        header = next(
            csv.reader(source),
            None
        )

    if not header:
        raise ValueError(
            f"Missing CSV header: {file_path}"
        )

    return header


def validate_source_headers(files):

    expected_header = read_csv_header(files[0])

    if len(expected_header) != EXPECTED_COLUMN_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_COLUMN_COUNT} columns, "
            f"found {len(expected_header)}."
        )

    if len(set(expected_header)) != len(expected_header):
        raise ValueError(
            "Duplicate column names detected."
        )

    for file_path in files[1:]:

        actual_header = read_csv_header(file_path)

        if actual_header != expected_header:
            raise ValueError(
                f"Schema mismatch in: {file_path}"
            )

    print("\nCSV HEADER VALIDATION")
    print("-" * 55)

    print("All labor batch headers are consistent.")
    print(f"Source columns: {len(expected_header)}")

    print("\nLABOR DATASET COLUMN NAMES:")

    for index, column_name in enumerate(
        expected_header, start=1
    ):
        print(f"{index:02d}. {column_name}")

    return expected_header


# ============================================================
# 4. CREATE LOCAL PYSPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_NYC_Parks_AMPS_Labor_Profiling"
        )
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.csv.parser.columnPruning.enabled",
            "false"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 5. LOAD ALL LABOR BATCHES WITH PYSPARK
# ============================================================

def load_labor_data(spark, files, expected_header):

    file_paths = [
        str(file_path.resolve())
        for file_path in files
    ]

    schema = StructType([
        StructField(
            column_name,
            StringType(),
            True
        )
        for column_name in expected_header
    ])

    print("\nLOADING LABOR DATA WITH PYSPARK")
    print("-" * 55)

    print(
        f"Loading {len(file_paths):,} "
        "CSV files using explicit paths."
    )

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
        .schema(schema)
        .csv(file_paths)
    )

    if df.columns != expected_header:
        raise ValueError(
            "PySpark schema differs from the "
            "validated source schema."
        )

    print("\nPYSPARK LABOR DATA SCHEMA:")
    df.printSchema()

    return df


# ============================================================
# 6. PROFILE RECORD COUNT AND COLUMN QUALITY
# ============================================================

def profile_labor_columns(df):
    """
    Missing:
        NULL or whitespace-only value.

    Placeholder:
        Literal '-' after trimming.

    Placeholder counts are separate from missing
    counts because their business meaning is
    not yet verified.
    """

    expressions = [
        F.count(F.lit(1)).alias("__total_rows")
    ]

    for index, column_name in enumerate(df.columns):

        safe_column_name = (
            column_name.replace("`", "``")
        )

        column = F.col(f"`{safe_column_name}`")

        missing_expression = F.sum(
            F.when(
                column.isNull()
                | (F.trim(column) == ""),
                1
            ).otherwise(0)
        ).alias(f"missing_{index}")

        placeholder_expression = F.sum(
            F.when(
                F.trim(column) == "-",
                1
            ).otherwise(0)
        ).alias(f"placeholder_{index}")

        expressions.append(missing_expression)
        expressions.append(placeholder_expression)

    summary = df.agg(*expressions).first()

    total_rows = int(summary["__total_rows"])

    column_profiles = []

    for index, column_name in enumerate(df.columns):

        missing_count = int(
            summary[f"missing_{index}"] or 0
        )

        placeholder_count = int(
            summary[f"placeholder_{index}"] or 0
        )

        column_profiles.append({
            "column_name": column_name,
            "source_data_type": "string",
            "missing_count": missing_count,
            "non_missing_count": (
                total_rows - missing_count
            ),
            "missing_percentage": (
                round(
                    100 * missing_count / total_rows,
                    2
                )
                if total_rows else 0.0
            ),
            "dash_placeholder_count": (
                placeholder_count
            ),
            "dash_placeholder_percentage": (
                round(
                    100 * placeholder_count / total_rows,
                    2
                )
                if total_rows else 0.0
            ),
        })

    return total_rows, column_profiles


# ============================================================
# 7. DISCOVER POSSIBLE IDENTIFIER COLUMNS
# ============================================================

def discover_identifier_columns(columns):
    """
    Discover likely identifiers based on source names.

    A matching name does not prove that a column
    is unique or is a foreign key.
    """

    identifier_keywords = [
        "sqlidentity",
        "identity",
        "_id",
        "lab_code",
        "labor_code",
        "employee",
        "person",
        "technician",
        "evt_code",
        "event",
        "workorder",
        "work_order",
    ]

    candidates = []

    for column_name in columns:

        normalized = column_name.lower()

        if any(
            keyword in normalized
            for keyword in identifier_keywords
        ):
            candidates.append(column_name)

    return candidates


# ============================================================
# 8. DISCOVER POSSIBLE RELATIONSHIP COLUMNS
# ============================================================

def discover_relationship_columns(columns):

    keywords = [
        "event",
        "evt",
        "workorder",
        "work_order",
        "employee",
        "person",
        "technician",
        "trade",
        "asset",
        "object",
        "code",
        "identity",
    ]

    return [
        column_name
        for column_name in columns
        if any(
            keyword in column_name.lower()
            for keyword in keywords
        )
    ]


# ============================================================
# 9. VALIDATE CANDIDATE IDENTIFIER UNIQUENESS
# ============================================================

def validate_candidate_identifiers(
    df,
    total_rows,
    candidates
):
    """
    Check up to six discovered candidate fields.

    Not every identifier should be unique:
    employee IDs and work-order references may
    legitimately repeat across labor records.
    """

    results = {}

    for column_name in candidates[:6]:

        normalized = F.trim(
            F.col(column_name)
        )

        valid = df.filter(
            F.col(column_name).isNotNull()
            & (normalized != "")
        )

        non_missing_count = valid.count()

        distinct_count = (
            valid.select(
                normalized.alias("identifier")
            )
            .distinct()
            .count()
        )

        duplicate_excess = (
            non_missing_count - distinct_count
        )

        results[column_name] = {
            "total_records": total_rows,
            "non_missing_records": non_missing_count,
            "missing_records": (
                total_rows - non_missing_count
            ),
            "distinct_identifiers": distinct_count,
            "duplicate_excess_rows": duplicate_excess,
            "is_complete": (
                non_missing_count == total_rows
            ),
            "is_unique": (
                duplicate_excess == 0
            ),
            "is_candidate_record_key": (
                non_missing_count == total_rows
                and duplicate_excess == 0
            ),
            "note": (
                "Uniqueness alone does not establish "
                "the business meaning of this identifier."
            ),
        }

    return results


# ============================================================
# 10. INSPECT SOURCE RECORD SAMPLES
# ============================================================

def inspect_sample_records(df):

    print("\nSAMPLE LABOR RECORDS")
    print("-" * 55)

    selected_columns = df.columns[:8]

    df.select(
        *selected_columns
    ).show(
        5,
        truncate=40
    )

    return {
        "sample_count_requested": 5,
        "sample_columns": selected_columns,
    }


# ============================================================
# 11. EXECUTE PROFILING AND SAVE JSON
# ============================================================

def run_profiling():

    files = validate_source_files()

    expected_header = validate_source_headers(files)

    spark = create_spark_session()

    try:

        df = load_labor_data(
            spark,
            files,
            expected_header
        )

        print("\n" + "=" * 65)
        print("NYC PARKS AMPS - LABOR DATA PROFILING")
        print("=" * 65)

        # Profile all records and column completeness.
        total_rows, column_quality = (
            profile_labor_columns(df)
        )

        # Discover source identifiers.
        identifier_columns = (
            discover_identifier_columns(df.columns)
        )

        relationship_columns = (
            discover_relationship_columns(df.columns)
        )

        # Test discovered identifier candidates.
        identifier_validation = (
            validate_candidate_identifiers(
                df,
                total_rows,
                identifier_columns
            )
        )

        # Display samples.
        sample_inspection = (
            inspect_sample_records(df)
        )

        # Build the permanent profiling report.
        report = {
            "project": "AssetFusion360",
            "profiling_step": "4.6C",
            "dataset": (
                "NYC Parks AMPS Labor - CSV Batches"
            ),
            "source_directory": str(
                SOURCE_DIR.relative_to(PROJECT_ROOT)
            ),
            "profile_timestamp_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "spark_version": spark.version,
            "total_source_files": len(files),
            "total_records": total_rows,
            "total_columns": len(df.columns),
            "csv_reader_configuration": {
                "header": True,
                "quote": '"',
                "escape": '"',
                "multiLine": True,
                "inferSchema": False,
                "mode": "FAILFAST",
            },
            "source_schema": [
                {
                    "column_name": field.name,
                    "data_type": (
                        field.dataType.simpleString()
                    ),
                    "nullable": field.nullable,
                }
                for field in df.schema.fields
            ],
            "column_quality": column_quality,
            "potential_identifier_columns": (
                identifier_columns
            ),
            "potential_relationship_columns": (
                relationship_columns
            ),
            "identifier_validation": (
                identifier_validation
            ),
            "sample_inspection": sample_inspection,
            "notes": [
                "All 647 source headers are validated.",
                "All fields are initially read as strings.",
                "Source files are not modified.",
                "Dash placeholders are counted separately.",
                "Candidate keys need business validation.",
                "Cross-dataset relationships are not yet tested.",
            ],
        }

        REPORT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        with REPORT_FILE.open(
            "w",
            encoding="utf-8"
        ) as output_file:

            json.dump(
                report,
                output_file,
                indent=2,
                ensure_ascii=False
            )

        # Print results.
        print("\n" + "=" * 65)
        print("LABOR DATA PROFILING SUMMARY")
        print("=" * 65)

        print(f"Files processed: {len(files):,}")
        print(f"Total records: {total_rows:,}")
        print(f"Total columns: {len(df.columns)}")

        print("\nPOTENTIAL IDENTIFIER COLUMNS:")
        print(
            json.dumps(
                identifier_columns,
                indent=2
            )
        )

        print("\nPOTENTIAL RELATIONSHIP COLUMNS:")
        print(
            json.dumps(
                relationship_columns,
                indent=2
            )
        )

        print("\nIDENTIFIER VALIDATION:")
        print(
            json.dumps(
                identifier_validation,
                indent=2
            )
        )

        print("\nCOLUMN QUALITY:")
        for column in column_quality:

            print(
                f"{column['column_name']}: "
                f"{column['missing_count']:,} missing "
                f"({column['missing_percentage']}%), "
                f"{column['dash_placeholder_count']:,} "
                "dash placeholders"
            )

        print("\n" + "=" * 65)
        print("LABOR PROFILING COMPLETED SUCCESSFULLY")
        print("=" * 65)

        print("\nReport saved to:")
        print(REPORT_FILE)

    finally:
        spark.stop()


# ============================================================
# 12. MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    run_profiling()
