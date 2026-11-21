
"""
AssetFusion360 Capstone Project

STEP 4.7B
NYC 311 Service Requests - Source Data Profiling

Purpose:
    1. Discover all NYC 311 CSV batches.
    2. Validate file counts and schema consistency.
    3. Read files using PySpark in manageable groups.
    4. Calculate accurate source record counts.
    5. Measure missing and blank values.
    6. Identify dash and literal NULL placeholders.
    7. Discover service-request, date, agency and location fields.
    8. Save a permanent JSON data quality report.

Technology:
    Python 3.12
    PySpark 3.5.7
    No pandas

Source:
    data/01_raw_source_data/
        nyc_311_service_requests/

Output:
    reports/data_profiling_reports/
        nyc_311_service_requests_data_profile.json

Original source CSV files are never modified.

Note:
    This is descriptive source profiling.
    Exact ID uniqueness and relationship checks
    will follow once actual columns are confirmed.
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
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_311_service_requests"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_311_service_requests_data_profile.json"
)

EXPECTED_FILE_COUNT = 176

# Read only this many CSV files per Spark group.
FILES_PER_GROUP = 12


# ============================================================
# 2. DISCOVER SOURCE FILES
# ============================================================

def discover_csv_files():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source folder not found: {SOURCE_DIR}"
        )

    files = sorted(
        (
            path
            for path in SOURCE_DIR.rglob("*")
            if path.is_file()
            and path.suffix.lower() == ".csv"
        ),
        key=lambda path: str(path).lower()
    )

    if len(files) != EXPECTED_FILE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_FILE_COUNT} CSV files, "
            f"found {len(files)}."
        )

    empty_files = [
        str(path)
        for path in files
        if path.stat().st_size == 0
    ]

    if empty_files:
        raise ValueError(
            f"Empty CSV files: {empty_files[:10]}"
        )

    print("\n" + "=" * 65)
    print("NYC 311 - SOURCE FILE INVENTORY")
    print("=" * 65)

    print(f"CSV files discovered: {len(files):,}")
    print(f"Files per processing group: {FILES_PER_GROUP}")

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

        reader = csv.reader(source)
        header = next(reader, None)

    if not header:
        raise ValueError(
            f"Missing CSV header: {file_path}"
        )

    return header


def validate_csv_headers(files):

    expected_header = read_csv_header(files[0])

    if any(not name.strip() for name in expected_header):
        raise ValueError(
            "Blank source column name detected."
        )

    if len(expected_header) != len(set(expected_header)):
        raise ValueError(
            "Duplicate source column names detected."
        )

    mismatches = []

    for path in files[1:]:

        header = read_csv_header(path)

        if header != expected_header:

            mismatches.append({
                "file": str(
                    path.relative_to(SOURCE_DIR)
                ),
                "column_count": len(header),
                "columns": header,
            })

    if mismatches:

        REPORT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        mismatch_report = {
            "project": "AssetFusion360",
            "profiling_step": "4.7B",
            "status": "SCHEMA_MISMATCH",
            "expected_columns": expected_header,
            "mismatched_files": mismatches,
        }

        with REPORT_FILE.open(
            "w",
            encoding="utf-8"
        ) as output:

            json.dump(
                mismatch_report,
                output,
                indent=2,
                ensure_ascii=False
            )

        raise ValueError(
            f"Found {len(mismatches)} CSV files "
            "with inconsistent schemas. "
            f"Review report: {REPORT_FILE}"
        )

    print("\nCSV HEADER VALIDATION")
    print("-" * 55)

    print("All source CSV headers are consistent.")
    print(f"Total source columns: {len(expected_header)}")

    print("\nNYC 311 SOURCE COLUMN NAMES:")

    for index, name in enumerate(
        expected_header,
        start=1
    ):
        print(f"{index:02d}. {name}")

    return expected_header


# ============================================================
# 4. CREATE LOCAL SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_NYC311_Source_Profiling"
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
# 5. LOAD ONE FILE GROUP WITH PYSPARK
# ============================================================

def load_file_group(
    spark,
    file_group,
    source_header
):

    explicit_paths = [
        str(path.resolve())
        for path in file_group
    ]

    schema = StructType([
        StructField(
            name,
            StringType(),
            True
        )
        for name in source_header
    ])

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
        .csv(explicit_paths)
    )

    if df.columns != source_header:
        raise ValueError(
            "Loaded Spark columns differ "
            "from source schema."
        )

    return df


# ============================================================
# 6. PROFILE EACH GROUP
# ============================================================

def profile_file_group(df):
    """
    Missing = NULL or empty/whitespace string.

    Dash placeholder = exactly '-'.

    Literal NULL placeholder = text 'NULL',
    ignoring capitalization.

    These categories remain separate so
    we do not silently change source meanings.
    """

    expressions = [
        F.count(F.lit(1)).alias("row_count")
    ]

    for index, name in enumerate(df.columns):

        safe_name = name.replace("`", "``")

        column = F.col(f"`{safe_name}`")
        trimmed = F.trim(column)

        expressions.extend([
            F.sum(
                F.when(
                    column.isNull() | (trimmed == ""),
                    1
                ).otherwise(0)
            ).alias(f"missing_{index}"),

            F.sum(
                F.when(
                    trimmed == "-",
                    1
                ).otherwise(0)
            ).alias(f"dash_{index}"),

            F.sum(
                F.when(
                    F.upper(trimmed) == "NULL",
                    1
                ).otherwise(0)
            ).alias(f"literal_null_{index}"),
        ])

    row = df.agg(*expressions).first()

    result = {
        "record_count": int(row["row_count"]),
        "column_counts": [],
    }

    for index, name in enumerate(df.columns):

        result["column_counts"].append({
            "column_name": name,
            "missing_count": int(
                row[f"missing_{index}"] or 0
            ),
            "dash_placeholder_count": int(
                row[f"dash_{index}"] or 0
            ),
            "literal_null_count": int(
                row[f"literal_null_{index}"] or 0
            ),
        })

    return result


# ============================================================
# 7. DISCOVER POTENTIAL BUSINESS FIELDS
# ============================================================

def discover_business_fields(columns):

    categories = {
        "service_request_identifiers": [
            "unique key",
            "unique_key",
            "request id",
            "request_id",
            "complaint id",
            "complaint_id",
            "sr number",
        ],
        "date_fields": [
            "date",
            "time",
            "created",
            "closed",
            "updated",
            "due",
        ],
        "agency_fields": [
            "agency",
            "department",
        ],
        "service_category_fields": [
            "complaint",
            "descriptor",
            "problem",
            "status",
            "resolution",
            "location type",
        ],
        "geographic_fields": [
            "borough",
            "latitude",
            "longitude",
            "zip",
            "street",
            "address",
            "city",
            "community board",
            "park",
            "location",
        ],
    }

    results = {}

    for category, keywords in categories.items():

        results[category] = [
            name
            for name in columns
            if any(
                keyword in name.lower()
                for keyword in keywords
            )
        ]

    return results


# ============================================================
# 8. SAVE A CHECKPOINT REPORT
# ============================================================

def save_report(report):

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    report["last_updated_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    temporary_path = REPORT_FILE.with_suffix(
        ".json.tmp"
    )

    with temporary_path.open(
        "w",
        encoding="utf-8"
    ) as output:

        json.dump(
            report,
            output,
            indent=2,
            ensure_ascii=False
        )

    temporary_path.replace(REPORT_FILE)


# ============================================================
# 9. PROCESS ALL FILE GROUPS
# ============================================================

def run_profiling():

    files = discover_csv_files()

    source_header = validate_csv_headers(files)

    file_groups = [
        files[index:index + FILES_PER_GROUP]
        for index in range(
            0,
            len(files),
            FILES_PER_GROUP
        )
    ]

    report = {
        "project": "AssetFusion360",
        "profiling_step": "4.7B",
        "dataset": "NYC 311 Service Requests",
        "status": "IN_PROGRESS",
        "source_directory": str(
            SOURCE_DIR.relative_to(PROJECT_ROOT)
        ),
        "total_source_files": len(files),
        "total_columns": len(source_header),
        "total_records": 0,
        "source_columns": source_header,
        "spark_version": None,
        "csv_reader_configuration": {
            "header": True,
            "quote": '"',
            "escape": '"',
            "multiLine": True,
            "mode": "FAILFAST",
            "inferSchema": False,
        },
        "business_field_discovery": (
            discover_business_fields(source_header)
        ),
        "file_group_results": [],
        "notes": [
            "Source CSV files remain unchanged.",
            "All columns are read as strings.",
            "Records are profiled in file groups.",
            "Blank values and placeholders are separate.",
            "Identifier uniqueness has not yet been tested.",
            "NYC 311 and AMPS are independent source systems.",
        ],
    }

    totals = [
        {
            "column_name": name,
            "missing_count": 0,
            "dash_placeholder_count": 0,
            "literal_null_count": 0,
        }
        for name in source_header
    ]

    spark = create_spark_session()

    try:

        report["spark_version"] = spark.version

        print("\n" + "=" * 65)
        print("NYC 311 - PYSPARK SOURCE DATA PROFILING")
        print("=" * 65)

        print(
            f"Processing {len(file_groups)} "
            "file groups."
        )

        for group_index, file_group in enumerate(
            file_groups,
            start=1
        ):

            print(
                f"\nGroup {group_index}/"
                f"{len(file_groups)}: "
                f"{len(file_group)} CSV files"
            )

            df = load_file_group(
                spark,
                file_group,
                source_header
            )

            group_result = profile_file_group(df)

            group_rows = group_result["record_count"]

            report["total_records"] += group_rows

            for index, column_count in enumerate(
                group_result["column_counts"]
            ):

                for field in [
                    "missing_count",
                    "dash_placeholder_count",
                    "literal_null_count",
                ]:

                    totals[index][field] += (
                        column_count[field]
                    )

            report["file_group_results"].append({
                "group_number": group_index,
                "file_count": len(file_group),
                "record_count": group_rows,
                "first_file": (
                    file_group[0].relative_to(
                        SOURCE_DIR
                    ).as_posix()
                ),
                "last_file": (
                    file_group[-1].relative_to(
                        SOURCE_DIR
                    ).as_posix()
                ),
            })

            # Save findings after each successful group.
            save_report(report)

            print(
                f"  Group rows: {group_rows:,}"
            )

            print(
                "  Cumulative rows: "
                f"{report['total_records']:,}"
            )

        total_records = report["total_records"]

        report["column_quality"] = []

        for column in totals:

            missing = column["missing_count"]

            report["column_quality"].append({
                **column,
                "non_missing_count": (
                    total_records - missing
                ),
                "missing_percentage": (
                    round(
                        100 * missing / total_records,
                        2
                    )
                    if total_records else 0.0
                ),
                "dash_placeholder_percentage": (
                    round(
                        100 * column[
                            "dash_placeholder_count"
                        ] / total_records,
                        2
                    )
                    if total_records else 0.0
                ),
                "literal_null_percentage": (
                    round(
                        100 * column[
                            "literal_null_count"
                        ] / total_records,
                        2
                    )
                    if total_records else 0.0
                ),
            })

        report["status"] = "COMPLETED"

        save_report(report)

        print("\n" + "=" * 65)
        print("NYC 311 DATA PROFILING SUMMARY")
        print("=" * 65)

        print(
            f"Files processed: {len(files):,}"
        )

        print(
            f"Processing groups: {len(file_groups)}"
        )

        print(
            f"Total records: {total_records:,}"
        )

        print(
            f"Total columns: {len(source_header)}"
        )

        print("\nDISCOVERED BUSINESS FIELDS:")

        print(
            json.dumps(
                report["business_field_discovery"],
                indent=2
            )
        )

        print("\nCOLUMN QUALITY:")

        for column in report["column_quality"]:

            print(
                f"{column['column_name']}: "
                f"{column['missing_count']:,} missing "
                f"({column['missing_percentage']}%), "
                f"{column['dash_placeholder_count']:,} "
                "dash placeholders, "
                f"{column['literal_null_count']:,} "
                "literal NULL placeholders"
            )

        print("\n" + "=" * 65)
        print("NYC 311 PROFILING COMPLETED SUCCESSFULLY")
        print("=" * 65)

        print("\nJSON report saved to:")
        print(REPORT_FILE)

    except Exception as error:

        report["status"] = "FAILED"

        report["error"] = (
            f"{type(error).__name__}: {error}"
        )

        save_report(report)

        raise

    finally:
        spark.stop()


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    run_profiling()
