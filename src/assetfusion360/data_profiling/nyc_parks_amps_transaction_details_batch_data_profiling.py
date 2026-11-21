
"""
AssetFusion360 Capstone Project

STEP 4.6E
NYC Parks AMPS - Transaction Details Batch Profiling

Purpose:
    1. Validate all 461 transaction-detail CSV files.
    2. Validate their 31-column headers.
    3. Load source records using PySpark.
    4. Count records and profile missing values.
    5. Count dash placeholders separately.
    6. Discover candidate record identifiers.
    7. Validate candidate identifier uniqueness.
    8. Discover possible foreign-key columns.
    9. Display sample source records.
   10. Save a permanent JSON profiling report.

Technology:
    Python 3.12
    PySpark 3.5.7
    No pandas

Important:
    - Source files are read-only.
    - No source values are cleaned or changed.
    - Candidate relationships are not assumed to be valid.
    - CSV fields containing quoted text are supported.
"""

import csv
import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

# Configure Python before starting PySpark.
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
    / "transaction_details_batches"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_parks_amps_transaction_details_batch_data_profile.json"
)

EXPECTED_FILE_COUNT = 461
EXPECTED_COLUMN_COUNT = 31


# ============================================================
# 2. VALIDATE SOURCE CSV FILES
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
            f"Expected {EXPECTED_FILE_COUNT} files, "
            f"found {len(files)}."
        )

    empty_files = [
        str(path)
        for path in files
        if path.stat().st_size == 0
    ]

    if empty_files:
        raise ValueError(
            f"Empty CSV files found: {empty_files[:10]}"
        )

    print("\n" + "=" * 65)
    print("TRANSACTION DETAILS - SOURCE FILE INVENTORY")
    print("=" * 65)

    print(f"CSV batch files: {len(files):,}")

    return files


# ============================================================
# 3. VALIDATE SOURCE CSV HEADERS
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
            "Duplicate source column names detected."
        )

    if any(
        not name.strip()
        for name in expected_header
    ):
        raise ValueError(
            "Blank source column name detected."
        )

    for file_path in files[1:]:

        actual_header = read_csv_header(file_path)

        if actual_header != expected_header:
            raise ValueError(
                f"Header mismatch detected: {file_path}"
            )

    print("\nCSV HEADER VALIDATION")
    print("-" * 55)

    print("All 461 CSV headers are consistent.")
    print(f"Total columns: {len(expected_header)}")

    print("\nTRANSACTION DETAIL SOURCE COLUMNS:")

    for index, name in enumerate(
        expected_header,
        start=1
    ):
        print(f"{index:02d}. {name}")

    return expected_header


# ============================================================
# 4. CREATE LOCAL PYSPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_AMPS_Transaction_Details_Profiling"
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

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 5. LOAD ALL TRANSACTION DETAIL CSV FILES
# ============================================================

def load_transaction_details(
    spark,
    files,
    expected_header
):

    # Explicit paths avoid the Hadoop wildcard
    # issue encountered during Work Orders profiling.
    file_paths = [
        str(path.resolve())
        for path in files
    ]

    # Preserve every source field as a string.
    schema = StructType([
        StructField(
            name,
            StringType(),
            True
        )
        for name in expected_header
    ])

    print("\nLOADING TRANSACTION DETAILS WITH PYSPARK")
    print("-" * 55)

    print(
        f"Passing {len(file_paths):,} "
        "explicit CSV paths to Spark."
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
            "Loaded schema does not match "
            "the validated source schema."
        )

    print("\nPYSPARK TRANSACTION DETAILS SCHEMA:")
    df.printSchema()

    return df


# ============================================================
# 6. PROFILE ROW COUNTS AND COLUMN QUALITY
# ============================================================

def profile_columns(df):
    """
    Missing values:
        NULL or whitespace-only source values.

    Dash placeholders:
        Values equal to '-' after trimming.

    Placeholder values are not automatically
    converted to NULL.
    """

    expressions = [
        F.count(F.lit(1)).alias("__total_rows")
    ]

    for index, name in enumerate(df.columns):

        safe_name = name.replace("`", "``")
        column = F.col(f"`{safe_name}`")

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
            ).alias(f"dash_{index}")
        )

    # Perform the 31-column quality scan
    # in a single Spark aggregation.
    summary = df.agg(*expressions).first()

    total_rows = int(
        summary["__total_rows"]
    )

    profiles = []

    for index, name in enumerate(df.columns):

        missing = int(
            summary[f"missing_{index}"] or 0
        )

        dash_count = int(
            summary[f"dash_{index}"] or 0
        )

        profiles.append({
            "column_name": name,
            "source_data_type": "string",
            "missing_count": missing,
            "non_missing_count": (
                total_rows - missing
            ),
            "missing_percentage": (
                round(
                    100 * missing / total_rows,
                    2
                )
                if total_rows else 0.0
            ),
            "dash_placeholder_count": dash_count,
            "dash_placeholder_percentage": (
                round(
                    100 * dash_count / total_rows,
                    2
                )
                if total_rows else 0.0
            ),
        })

    return total_rows, profiles


# ============================================================
# 7. DISCOVER CANDIDATE RECORD IDENTIFIERS
# ============================================================

def discover_identifier_columns(columns):
    """
    Identify likely record-level keys using names.

    Parent transaction references should not
    automatically be treated as primary keys.
    """

    explicit_names = {
        "trd_code",
        "trd_sqlidentity",
        "trd_id",
        "trd_lineid",
        "trd_line_id",
        "trd_detailid",
        "trd_detail_id",
        "trl_sqlidentity",
        "trl_id",
        "trd_number",
    }

    candidates = []

    for name in columns:

        normalized = name.lower()

        if (
            normalized in explicit_names
            or normalized.endswith("_sqlidentity")
        ):
            candidates.append(name)

    return candidates


# ============================================================
# 8. DISCOVER POTENTIAL RELATIONSHIP COLUMNS
# ============================================================

def discover_relationship_columns(columns):
    """
    Find possible links to transactions,
    work orders, assets, stores and inventory.

    Name-based discovery only.
    """

    keywords = [
        "transaction",
        "trans",
        "tra_",
        "event",
        "evt",
        "workorder",
        "work_order",
        "object",
        "asset",
        "item",
        "part",
        "stock",
        "store",
        "supplier",
        "request",
        "req",
        "code",
        "identity",
    ]

    return [
        name
        for name in columns
        if any(
            keyword in name.lower()
            for keyword in keywords
        )
    ]


# ============================================================
# 9. VALIDATE CANDIDATE RECORD KEYS
# ============================================================

def validate_candidate_identifiers(
    df,
    total_rows,
    candidates
):
    """
    Test completeness and uniqueness.

    This does not verify foreign keys.
    """

    results = {}

    for name in candidates:

        normalized = F.trim(F.col(name))

        valid = df.filter(
            F.col(name).isNotNull()
            & (normalized != "")
        )

        non_missing = valid.count()

        distinct_count = (
            valid
            .select(
                normalized.alias("__identifier")
            )
            .distinct()
            .count()
        )

        duplicate_excess = (
            non_missing - distinct_count
        )

        results[name] = {
            "total_records": total_rows,
            "non_missing_records": non_missing,
            "missing_records": (
                total_rows - non_missing
            ),
            "distinct_identifiers": distinct_count,
            "duplicate_excess_rows": duplicate_excess,
            "is_complete": (
                non_missing == total_rows
            ),
            "is_unique": (
                duplicate_excess == 0
            ),
            "is_candidate_record_key": (
                non_missing == total_rows
                and duplicate_excess == 0
            ),
        }

    return results


# ============================================================
# 10. INSPECT TRANSACTION DETAIL SAMPLES
# ============================================================

def inspect_sample_records(df):

    print("\nSAMPLE TRANSACTION DETAIL RECORDS")
    print("-" * 55)

    sample_columns = df.columns[:10]

    df.select(
        *sample_columns
    ).show(
        5,
        truncate=40
    )

    return {
        "sample_count_requested": 5,
        "sample_columns": sample_columns,
    }


# ============================================================
# 11. EXECUTE FULL PROFILING
# ============================================================

def run_profiling():

    files = validate_source_files()

    expected_header = validate_source_headers(files)

    spark = create_spark_session()

    try:

        df = load_transaction_details(
            spark,
            files,
            expected_header
        )

        print("\n" + "=" * 65)
        print("TRANSACTION DETAILS - DATA PROFILING")
        print("=" * 65)

        total_rows, column_quality = (
            profile_columns(df)
        )

        identifier_columns = (
            discover_identifier_columns(
                df.columns
            )
        )

        relationship_columns = (
            discover_relationship_columns(
                df.columns
            )
        )

        identifier_validation = (
            validate_candidate_identifiers(
                df,
                total_rows,
                identifier_columns
            )
        )

        sample_inspection = (
            inspect_sample_records(df)
        )

        # ----------------------------------------------------
        # Build permanent JSON report
        # ----------------------------------------------------

        report = {
            "project": "AssetFusion360",
            "profiling_step": "4.6E",
            "dataset": (
                "NYC Parks AMPS Transaction Details"
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
                "enforceSchema": False,
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
            "candidate_identifier_columns": (
                identifier_columns
            ),
            "candidate_identifier_validation": (
                identifier_validation
            ),
            "potential_relationship_columns": (
                relationship_columns
            ),
            "sample_inspection": sample_inspection,
            "notes": [
                "All 461 CSV headers are validated.",
                "Original source data is unchanged.",
                "All fields are read as strings.",
                "Embedded CSV quotes are supported.",
                "Dash placeholders are counted separately.",
                "Candidate keys require business validation.",
                "Parent transaction references require FK checks.",
                "No source rows are deliberately excluded.",
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

        # ----------------------------------------------------
        # Print final results
        # ----------------------------------------------------

        print("\n" + "=" * 65)
        print("TRANSACTION DETAILS PROFILING SUMMARY")
        print("=" * 65)

        print(
            f"Files processed: {len(files):,}"
        )

        print(
            f"Total records: {total_rows:,}"
        )

        print(
            f"Total columns: {len(df.columns)}"
        )

        print("\nCANDIDATE IDENTIFIER COLUMNS:")

        print(
            json.dumps(
                identifier_columns,
                indent=2
            )
        )

        print("\nCANDIDATE IDENTIFIER VALIDATION:")

        print(
            json.dumps(
                identifier_validation,
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
        print("TRANSACTION DETAILS PROFILING COMPLETED")
        print("=" * 65)

        print("\nJSON report saved to:")
        print(REPORT_FILE)

    finally:
        spark.stop()


# ============================================================
# 12. MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    run_profiling()
