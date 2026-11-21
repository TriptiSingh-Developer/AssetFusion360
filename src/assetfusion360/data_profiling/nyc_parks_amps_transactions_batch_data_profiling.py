
"""
AssetFusion360 Capstone Project

STEP 4.6D
NYC Parks AMPS - Transactions Batch Data Profiling

Purpose:
    1. Validate all 152 transactions CSV files.
    2. Verify the expected 14-column schema.
    3. Load source records using PySpark.
    4. Count records and profile column completeness.
    5. Count dash placeholders separately.
    6. Discover potential transaction identifiers.
    7. Validate candidate identifier uniqueness.
    8. Discover possible relationships.
    9. Display sample records.
   10. Save a permanent JSON profiling report.

Technology:
    Python 3.12
    PySpark 3.5.7
    No pandas

Source:
    data/01_raw_source_data/
        nyc_parks_asset_management/
            transactions_batches/

Report:
    reports/data_profiling_reports/
        nyc_parks_amps_transactions_batch_data_profile.json

Original CSV files are never modified.
"""

import csv
import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

# Configure the active Python interpreter before Spark starts.
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
    / "transactions_batches"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_parks_amps_transactions_batch_data_profile.json"
)

EXPECTED_FILE_COUNT = 152
EXPECTED_COLUMN_COUNT = 14


# ============================================================
# 2. VALIDATE SOURCE FILES
# ============================================================

def validate_source_files():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source directory does not exist: {SOURCE_DIR}"
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
            f"Empty source files found: {empty_files[:10]}"
        )

    print("\n" + "=" * 65)
    print("NYC PARKS AMPS - TRANSACTIONS SOURCE INVENTORY")
    print("=" * 65)

    print(f"CSV batch files: {len(files):,}")

    return files


# ============================================================
# 3. READ AND VALIDATE CSV HEADERS
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

    if any(not column.strip() for column in expected_header):
        raise ValueError(
            "Blank column name detected."
        )

    for file_path in files[1:]:

        actual_header = read_csv_header(file_path)

        if actual_header != expected_header:
            raise ValueError(
                f"CSV schema mismatch: {file_path}"
            )

    print("\nCSV HEADER VALIDATION")
    print("-" * 55)

    print("All 152 CSV headers are consistent.")
    print(f"Total columns: {len(expected_header)}")

    print("\nTRANSACTION DATASET COLUMN NAMES:")

    for index, column_name in enumerate(
        expected_header,
        start=1
    ):
        print(f"{index:02d}. {column_name}")

    return expected_header


# ============================================================
# 4. INITIALIZE PYSPARK
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_NYC_Parks_AMPS_Transactions_Profiling"
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
# 5. LOAD TRANSACTION BATCHES WITH PYSPARK
# ============================================================

def load_transactions(spark, files, expected_header):

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

    print("\nLOADING TRANSACTION DATA WITH PYSPARK")
    print("-" * 55)

    print(
        f"Loading {len(file_paths):,} CSV files "
        "using explicit file paths."
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
            "Loaded Spark schema differs from "
            "the validated source header."
        )

    print("\nPYSPARK TRANSACTIONS SCHEMA:")
    df.printSchema()

    return df


# ============================================================
# 6. PROFILE RECORD COUNTS AND COLUMN QUALITY
# ============================================================

def profile_transaction_columns(df):
    """
    Missing values:
        NULL or whitespace-only strings.

    Placeholder values:
        Literal '-' after trimming.

    Dash placeholders are counted separately.
    """

    expressions = [
        F.count(F.lit(1)).alias("__total_rows")
    ]

    for index, column_name in enumerate(df.columns):

        safe_column = column_name.replace("`", "``")
        column = F.col(f"`{safe_column}`")

        expressions.append(
            F.sum(
                F.when(
                    column.isNull()
                    | (F.trim(column) == ""),
                    1
                ).otherwise(0)
            ).alias(f"missing_{index}")
        )

        expressions.append(
            F.sum(
                F.when(
                    F.trim(column) == "-",
                    1
                ).otherwise(0)
            ).alias(f"placeholder_{index}")
        )

    summary = df.agg(*expressions).first()

    total_rows = int(summary["__total_rows"])

    column_profiles = []

    for index, column_name in enumerate(df.columns):

        missing = int(
            summary[f"missing_{index}"] or 0
        )

        placeholders = int(
            summary[f"placeholder_{index}"] or 0
        )

        column_profiles.append({
            "column_name": column_name,
            "source_data_type": "string",
            "missing_count": missing,
            "non_missing_count": total_rows - missing,
            "missing_percentage": (
                round(
                    100 * missing / total_rows,
                    2
                )
                if total_rows
                else 0.0
            ),
            "dash_placeholder_count": placeholders,
            "dash_placeholder_percentage": (
                round(
                    100 * placeholders / total_rows,
                    2
                )
                if total_rows
                else 0.0
            ),
        })

    return total_rows, column_profiles


# ============================================================
# 7. DISCOVER POTENTIAL IDENTIFIER COLUMNS
# ============================================================

def discover_identifier_columns(columns):
    """
    Discover source columns that may contain
    transaction or record identifiers.

    Actual business meaning must be validated.
    """

    identifier_keywords = [
        "sqlidentity",
        "identity",
        "_id",
        "transaction",
        "trans_code",
        "tra_code",
        "trn_code",
        "evt_code",
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
# 8. DISCOVER POTENTIAL RELATIONSHIP COLUMNS
# ============================================================

def discover_relationship_columns(columns):

    relationship_keywords = [
        "event",
        "evt",
        "workorder",
        "work_order",
        "object",
        "asset",
        "order",
        "stock",
        "store",
        "supplier",
        "identity",
        "code",
    ]

    return [
        column_name
        for column_name in columns
        if any(
            keyword in column_name.lower()
            for keyword in relationship_keywords
        )
    ]


# ============================================================
# 9. VALIDATE CANDIDATE IDENTIFIERS
# ============================================================

def validate_candidate_identifiers(
    df,
    total_rows,
    candidates
):
    """
    Check discovered identifier columns for
    completeness and uniqueness.

    Repeating values may be valid references
    to a parent transaction or work order.
    """

    results = {}

    for column_name in candidates:

        normalized = F.trim(
            F.col(column_name)
        )

        valid = df.filter(
            F.col(column_name).isNotNull()
            & (normalized != "")
        )

        non_missing = valid.count()

        distinct_values = (
            valid
            .select(
                normalized.alias("normalized_identifier")
            )
            .distinct()
            .count()
        )

        duplicate_excess = (
            non_missing - distinct_values
        )

        results[column_name] = {
            "total_records": total_rows,
            "non_missing_records": non_missing,
            "missing_records": (
                total_rows - non_missing
            ),
            "distinct_identifier_count": distinct_values,
            "duplicate_excess_rows": duplicate_excess,
            "is_complete": non_missing == total_rows,
            "is_unique": duplicate_excess == 0,
            "is_candidate_record_key": (
                non_missing == total_rows
                and duplicate_excess == 0
            ),
            "note": (
                "A complete and unique identifier is only "
                "a structurally valid candidate key. "
                "Business and relationship validation "
                "are still required."
            ),
        }

    return results


# ============================================================
# 10. INSPECT SAMPLE TRANSACTION RECORDS
# ============================================================

def inspect_sample_records(df):

    print("\nSAMPLE TRANSACTION RECORDS")
    print("-" * 55)

    # Dataset-specific names will be confirmed
    # from the actual source header.
    selected_columns = df.columns[:8]

    df.select(
        *selected_columns
    ).show(
        5,
        truncate=45
    )

    return {
        "sample_count_requested": 5,
        "sample_columns": selected_columns,
    }


# ============================================================
# 11. GENERATE COMPLETE PROFILING REPORT
# ============================================================

def run_profiling():

    files = validate_source_files()

    expected_header = validate_source_headers(files)

    spark = create_spark_session()

    try:

        df = load_transactions(
            spark,
            files,
            expected_header
        )

        print("\n" + "=" * 65)
        print("NYC PARKS AMPS - TRANSACTIONS DATA PROFILING")
        print("=" * 65)

        # ----------------------------------------------------
        # Profile records and column quality
        # ----------------------------------------------------

        total_rows, column_quality = (
            profile_transaction_columns(df)
        )

        # ----------------------------------------------------
        # Discover potential identifiers and relationships
        # ----------------------------------------------------

        identifier_columns = (
            discover_identifier_columns(df.columns)
        )

        relationship_columns = (
            discover_relationship_columns(df.columns)
        )

        # ----------------------------------------------------
        # Validate identifier candidates
        # ----------------------------------------------------

        identifier_validation = (
            validate_candidate_identifiers(
                df,
                total_rows,
                identifier_columns
            )
        )

        # ----------------------------------------------------
        # Display sample source records
        # ----------------------------------------------------

        sample_inspection = (
            inspect_sample_records(df)
        )

        # ----------------------------------------------------
        # Build permanent report
        # ----------------------------------------------------

        report = {
            "project": "AssetFusion360",
            "profiling_step": "4.6D",
            "dataset": (
                "NYC Parks AMPS Transactions - CSV Batches"
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
                "All CSV headers validated before loading.",
                "Source columns initially read as strings.",
                "Embedded double quotes handled explicitly.",
                "Multiline quoted text supported.",
                "Dash placeholders counted separately.",
                "Original source CSV files preserved.",
                "Cross-dataset joins are not yet validated.",
            ],
        }

        # ----------------------------------------------------
        # Save JSON report
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Print profiling results
        # ----------------------------------------------------

        print("\n" + "=" * 65)
        print("TRANSACTIONS DATA PROFILING SUMMARY")
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
        print("TRANSACTIONS PROFILING COMPLETED SUCCESSFULLY")
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
