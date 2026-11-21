
"""
AssetFusion360 Capstone Project

STEP 4.6B
NYC Parks AMPS - Work Order Batch Data Profiling

Checks:
    1. Validate 447 CSV batch files
    2. Validate 55 source columns
    3. Correctly parse CSV escaped quotes
    4. Support multiline quoted descriptions
    5. Count source records
    6. Profile missing and blank values
    7. Identify candidate identifier columns
    8. Validate candidate identifier uniqueness
    9. Save a comprehensive JSON profiling report

Technology:
    PySpark 3.5.7
    Python 3.12
    No pandas

Input:
    data/01_raw_source_data/
        nyc_parks_asset_management/
            workorders_batches/

Output:
    reports/data_profiling_reports/
        nyc_parks_amps_workorders_batch_data_profile.json

Original source files are never modified.
"""

import csv
import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

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
# 1. PROJECT PATHS AND CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
    / "workorders_batches"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_parks_amps_workorders_batch_data_profile.json"
)

EXPECTED_FILE_COUNT = 447
EXPECTED_COLUMN_COUNT = 55

CANDIDATE_IDENTIFIER_COLUMNS = [
    "evt_code",
    "evt_sqlidentity",
]

ASSET_REFERENCE_COLUMNS = [
    "evt_object",
    "evt_obtype",
    "evt_parent",
]


# ============================================================
# 2. VALIDATE CSV FILE INVENTORY
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

    for file_path in files:
        if file_path.stat().st_size == 0:
            raise ValueError(
                f"Empty CSV file: {file_path}"
            )

    print("\nSOURCE FILE INVENTORY")
    print("-" * 55)
    print(f"CSV batch files: {len(files):,}")

    return files


# ============================================================
# 3. VALIDATE ALL SOURCE HEADERS
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

    if len(expected_header) != len(set(expected_header)):
        raise ValueError(
            "Duplicate source column names detected."
        )

    for file_path in files[1:]:

        actual_header = read_csv_header(file_path)

        if actual_header != expected_header:
            raise ValueError(
                f"CSV header mismatch: {file_path}"
            )

    print("\nHEADER VALIDATION")
    print("-" * 55)
    print("All 447 CSV headers are consistent.")
    print(f"Columns: {len(expected_header)}")

    print("\nSOURCE COLUMN NAMES:")

    for index, column_name in enumerate(
        expected_header, start=1
    ):
        print(f"{index:02d}. {column_name}")

    return expected_header


# ============================================================
# 4. CREATE LOCAL SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_AMPS_WorkOrders_Profiling"
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
# 5. READ CSV WITH CORRECT QUOTATION SETTINGS
# ============================================================

def load_workorders(
    spark,
    files,
    expected_header
):

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

    print("\nLOADING WORK-ORDER CSV FILES")
    print("-" * 55)

    print(
        f"Passing {len(file_paths):,} "
        "explicit file paths to PySpark."
    )

    df = (
        spark.read

        # CSV structure
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')

        # IMPORTANT:
        # Two double quotes inside a quoted field
        # represent one literal double quote.
        .option("escape", '"')

        # Handles quoted text containing line breaks.
        .option("multiLine", "true")

        # Validate headers instead of ignoring them.
        .option("enforceSchema", "false")

        # Keep original source values as strings.
        .option("inferSchema", "false")

        # Do not silently skip corrupted records.
        .option("mode", "FAILFAST")

        .option("encoding", "UTF-8")

        .schema(schema)
        .csv(file_paths)
    )

    if df.columns != expected_header:
        raise ValueError(
            "Loaded PySpark schema does not match "
            "the validated source header."
        )

    print("\nPYSPARK DATAFRAME SCHEMA:")
    df.printSchema()

    return df


# ============================================================
# 6. PROFILE TOTAL RECORDS AND MISSING VALUES
# ============================================================

def profile_columns(df):

    expressions = [
        F.count(F.lit(1)).alias("__total_rows")
    ]

    for index, column_name in enumerate(df.columns):

        safe_name = column_name.replace("`", "``")

        column = F.col(f"`{safe_name}`")

        missing_expression = F.sum(
            F.when(
                column.isNull()
                | (F.trim(column) == ""),
                1
            ).otherwise(0)
        ).alias(f"missing_{index}")

        expressions.append(missing_expression)

    summary = df.agg(*expressions).first()

    total_rows = int(
        summary["__total_rows"]
    )

    column_quality = []

    for index, column_name in enumerate(df.columns):

        missing = int(
            summary[f"missing_{index}"] or 0
        )

        column_quality.append({
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
        })

    return total_rows, column_quality


# ============================================================
# 7. VALIDATE CANDIDATE WORK-ORDER IDENTIFIERS
# ============================================================

def validate_candidate_identifiers(df, total_rows):

    results = {}

    for column_name in CANDIDATE_IDENTIFIER_COLUMNS:

        if column_name not in df.columns:
            results[column_name] = {
                "error": "Column not found"
            }
            continue

        normalized = F.trim(
            F.col(column_name)
        )

        valid = df.filter(
            F.col(column_name).isNotNull()
            & (normalized != "")
        )

        non_missing_count = valid.count()

        distinct_count = (
            valid
            .select(
                normalized.alias("identifier")
            )
            .distinct()
            .count()
        )

        duplicate_excess = (
            non_missing_count - distinct_count
        )

        result = {
            "column_name": column_name,
            "total_records": total_rows,
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
        }

        results[column_name] = result

    return results


# ============================================================
# 8. INSPECT SOURCE SAMPLE RECORDS
# ============================================================

def inspect_sample_records(df):

    selected_columns = [
        column_name
        for column_name in [
            "evt_code",
            "evt_desc",
            "evt_type",
            "evt_object",
            "evt_priority",
            "evt_sqlidentity",
        ]
        if column_name in df.columns
    ]

    print("\nSAMPLE WORK-ORDER RECORDS")
    print("-" * 55)

    df.select(
        *selected_columns
    ).show(
        5,
        truncate=60
    )

    return selected_columns


# ============================================================
# 9. GENERATE COMPLETE JSON PROFILING REPORT
# ============================================================

def run_profiling():

    files = validate_source_files()

    expected_header = validate_source_headers(files)

    spark = create_spark_session()

    try:

        df = load_workorders(
            spark,
            files,
            expected_header
        )

        print("\n" + "=" * 65)
        print("WORK-ORDER BATCH DATA PROFILING")
        print("=" * 65)

        total_rows, column_quality = (
            profile_columns(df)
        )

        identifier_validation = (
            validate_candidate_identifiers(
                df,
                total_rows
            )
        )

        sample_columns = (
            inspect_sample_records(df)
        )

        report = {
            "project": "AssetFusion360",
            "profiling_step": "4.6B",
            "dataset": (
                "NYC Parks AMPS Work Orders - Batches"
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
                "mode": "FAILFAST",
                "inferSchema": False,
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
            "candidate_identifier_validation": (
                identifier_validation
            ),
            "asset_reference_columns": (
                ASSET_REFERENCE_COLUMNS
            ),
            "sample_columns": sample_columns,
            "notes": [
                "All 447 CSV headers were validated.",
                "Doubled CSV quotes are handled explicitly.",
                "Multiline quoted text is supported.",
                "Standalone exports and test CSVs excluded.",
                "All original columns remain strings.",
                "No source records were modified.",
                "Cross-dataset relationships are pending.",
            ],
        }

        REPORT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        with REPORT_FILE.open(
            "w",
            encoding="utf-8"
        ) as output:

            json.dump(
                report,
                output,
                indent=2,
                ensure_ascii=False
            )

        print("\n" + "=" * 65)
        print("WORK-ORDER PROFILING SUMMARY")
        print("=" * 65)

        print(f"Files processed: {len(files):,}")
        print(f"Total records: {total_rows:,}")
        print(f"Total columns: {len(df.columns)}")

        print("\nCANDIDATE IDENTIFIER RESULTS:")

        print(
            json.dumps(
                identifier_validation,
                indent=2
            )
        )

        print("\nCOLUMN MISSING VALUES:")

        for column in column_quality:

            print(
                f"{column['column_name']}: "
                f"{column['missing_count']:,} missing "
                f"({column['missing_percentage']}%)"
            )

        print("\n" + "=" * 65)
        print("WORK-ORDER PROFILING COMPLETED SUCCESSFULLY")
        print("=" * 65)

        print("\nJSON report saved to:")
        print(REPORT_FILE)

    finally:
        spark.stop()


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    run_profiling()
