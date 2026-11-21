
"""
AssetFusion360 Capstone Project

STEP 4.7C - Supplementary Inspection Data Profiling

Purpose:
  1. Discover the supplementary inspection CSV.
  2. Validate source schema and record count.
  3. Profile missing and placeholder values.
  4. Test candidate identifier completeness and uniqueness.
  5. Identify numeric columns and their ranges.
  6. Discover date columns and validate supported formats.
  7. Identify inspection, condition, status and asset fields.
  8. Save results to a permanent JSON report.

Technology:
  Python 3.12
  PySpark 3.5.7
  No pandas

Source files are read-only. No transformations are persisted.
"""

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "supplementary_inspection_data"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "supplementary_inspection_data_profile.json"
)

EXPECTED_FILE_COUNT = 1
MAX_IDENTIFIER_CANDIDATES = 6
MAX_DATE_CANDIDATES = 10

NUMERIC_REGEX = (
    r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"
    r"(?:[eE][+-]?[0-9]+)?$"
)

DATE_FORMATS = [
    "yyyy-MM-dd",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "MM/dd/yyyy",
    "MM/dd/yyyy HH:mm:ss",
    "M/d/yyyy",
    "M/d/yyyy h:mm:ss a",
    "MMM d yyyy h:mma",
    "MMM d yyyy h:mm a",
]


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def safe_column(name):
    """Escape column names for Spark SQL expressions."""
    return F.col(f"`{name.replace('`', '``')}`")


def percent(numerator, denominator):
    if not denominator:
        return 0.0
    return round(100.0 * numerator / denominator, 2)


def normalized_value(name):
    value = F.trim(safe_column(name))

    return F.when(
        value.isNull() | (value == ""),
        F.lit(None).cast("string")
    ).otherwise(value)


def normalized_name(name):
    return re.sub(
        r"[^a-z0-9]+",
        "_",
        name.strip().lower()
    ).strip("_")


def save_report(report):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    report["last_updated_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    temporary_file = REPORT_FILE.with_suffix(".json.tmp")

    with temporary_file.open(
        "w",
        encoding="utf-8"
    ) as output:
        json.dump(
            report,
            output,
            indent=2,
            ensure_ascii=False
        )

    temporary_file.replace(REPORT_FILE)


# ============================================================
# 3. SOURCE FILE DISCOVERY
# ============================================================

def discover_source():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source folder does not exist: {SOURCE_DIR}"
        )

    files = sorted(
        [
            path
            for path in SOURCE_DIR.iterdir()
            if path.is_file()
            and path.suffix.lower() == ".csv"
        ],
        key=lambda path: path.name.lower()
    )

    if len(files) != EXPECTED_FILE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_FILE_COUNT} CSV file; "
            f"found {len(files)}. "
            f"Files: {[p.name for p in files]}"
        )

    file_path = files[0]

    if file_path.stat().st_size == 0:
        raise ValueError(
            f"CSV file is empty: {file_path}"
        )

    with file_path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as source:
        header = next(csv.reader(source), None)

    if not header:
        raise ValueError("CSV header is missing.")

    if len(set(header)) != len(header):
        raise ValueError("Duplicate column names found.")

    if any(not name.strip() for name in header):
        raise ValueError("Blank column name found.")

    print("\n" + "=" * 68)
    print("SUPPLEMENTARY INSPECTION - SOURCE INVENTORY")
    print("=" * 68)

    print(f"Source file: {file_path.name}")
    print(f"File size: {file_path.stat().st_size:,} bytes")
    print(f"Source columns: {len(header)}")

    print("\nCOLUMN NAMES:")
    for index, name in enumerate(header, start=1):
        print(f"{index:02d}. {name}")

    return file_path, header


# ============================================================
# 4. SPARK SESSION AND CSV READER
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_Supplementary_Inspection_Profiling"
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


def load_source(spark, file_path, header):

    schema = StructType([
        StructField(name, StringType(), True)
        for name in header
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
        .csv(str(file_path.resolve()))
    )

    if df.columns != header:
        raise ValueError(
            "Loaded Spark schema differs from CSV header."
        )

    return df


# ============================================================
# 5. COLUMN QUALITY PROFILING
# ============================================================

def profile_columns(df):

    expressions = [
        F.count(F.lit(1)).alias("total_rows")
    ]

    for index, name in enumerate(df.columns):

        column = safe_column(name)
        trimmed = F.trim(column)

        expressions.extend([
            F.sum(
                F.when(
                    column.isNull() | (trimmed == ""),
                    1
                ).otherwise(0)
            ).alias(f"missing_{index}"),

            F.sum(
                F.when(trimmed == "-", 1).otherwise(0)
            ).alias(f"dash_{index}"),

            F.sum(
                F.when(
                    F.upper(trimmed) == "NULL",
                    1
                ).otherwise(0)
            ).alias(f"literal_null_{index}"),

            F.sum(
                F.when(
                    F.upper(trimmed) == "N/A",
                    1
                ).otherwise(0)
            ).alias(f"na_{index}"),
        ])

    summary = df.agg(*expressions).first()

    total_rows = int(summary["total_rows"] or 0)
    results = []

    for index, name in enumerate(df.columns):

        missing = int(
            summary[f"missing_{index}"] or 0
        )

        results.append({
            "column_name": name,
            "missing_count": missing,
            "missing_percentage": percent(
                missing, total_rows
            ),
            "non_missing_count": total_rows - missing,
            "dash_placeholder_count": int(
                summary[f"dash_{index}"] or 0
            ),
            "literal_null_count": int(
                summary[f"literal_null_{index}"] or 0
            ),
            "na_placeholder_count": int(
                summary[f"na_{index}"] or 0
            ),
        })

    return total_rows, results


# ============================================================
# 6. DISCOVER INSPECTION-RELATED BUSINESS FIELDS
# ============================================================

def discover_business_fields(columns):

    categories = {
        "inspection_identifiers": [
            "inspection_id",
            "inspectionid",
            "inspection_number",
            "inspection_no",
            "record_id",
            "recordid",
        ],
        "asset_references": [
            "asset",
            "equipment",
            "object",
            "facility",
            "machine",
        ],
        "inspection_dates": [
            "date",
            "timestamp",
            "inspected_on",
            "inspected_at",
        ],
        "inspection_outcomes": [
            "result",
            "status",
            "condition",
            "rating",
            "score",
            "finding",
            "severity",
            "defect",
            "pass",
            "fail",
        ],
        "maintenance_fields": [
            "maintenance",
            "work_order",
            "workorder",
            "repair",
            "recommendation",
            "action",
        ],
        "geographic_fields": [
            "borough",
            "latitude",
            "longitude",
            "location",
            "address",
            "park",
        ],
    }

    discovered = {}

    for category, keywords in categories.items():
        discovered[category] = [
            name
            for name in columns
            if any(
                keyword in normalized_name(name)
                for keyword in keywords
            )
        ]

    return discovered


# ============================================================
# 7. IDENTIFIER UNIQUENESS VALIDATION
# ============================================================

def discover_identifier_candidates(columns):

    exact_names = {
        "inspection_id",
        "inspectionid",
        "inspection_number",
        "inspection_no",
        "record_id",
        "recordid",
        "inspection_key",
        "unique_key",
        "row_id",
        "id",
    }

    candidates = []

    for name in columns:

        normalized = normalized_name(name)

        if (
            normalized in exact_names
            or normalized.endswith("_sqlidentity")
        ):
            candidates.append(name)

    return candidates[:MAX_IDENTIFIER_CANDIDATES]


def validate_identifiers(df, total_rows, columns):

    results = {}

    for name in columns:

        key = normalized_value(name)

        summary = (
            df.select(key.alias("__key"))
            .agg(
                F.count("__key").alias("non_missing"),
                F.countDistinct("__key").alias("distinct_count")
            )
            .first()
        )

        non_missing = int(
            summary["non_missing"] or 0
        )

        distinct_count = int(
            summary["distinct_count"] or 0
        )

        results[name] = {
            "non_missing_records": non_missing,
            "missing_records": total_rows - non_missing,
            "distinct_identifiers": distinct_count,
            "duplicate_excess_rows": (
                non_missing - distinct_count
            ),
            "is_candidate_record_key": (
                non_missing == total_rows
                and distinct_count == total_rows
            ),
        }

    return results


# ============================================================
# 8. NUMERIC COLUMN PROFILING
# ============================================================

def profile_numeric_columns(df):

    results = {}

    for name in df.columns:

        value = normalized_value(name)

        # Validate numeric format before conversion.
        numeric_format = value.rlike(NUMERIC_REGEX)

        numeric_value = F.when(
            numeric_format,
            value.cast("double")
        )

        summary = df.agg(
            F.count(value).alias("populated"),

            F.sum(
                F.when(numeric_format, 1).otherwise(0)
            ).alias("numeric_count"),

            F.min(numeric_value).alias("minimum"),
            F.max(numeric_value).alias("maximum"),
        ).first()

        populated = int(summary["populated"] or 0)
        numeric_count = int(
            summary["numeric_count"] or 0
        )

        coverage = percent(
            numeric_count,
            populated
        )

        # Report columns with predominantly numeric data.
        if populated and coverage >= 95:

            results[name] = {
                "populated_records": populated,
                "numeric_format_records": numeric_count,
                "non_numeric_records": (
                    populated - numeric_count
                ),
                "numeric_coverage_percentage": coverage,
                "minimum": summary["minimum"],
                "maximum": summary["maximum"],
                "is_numeric_candidate": True,
            }

    return results


# ============================================================
# 9. DATE FIELD PROFILING
# ============================================================

def discover_date_candidates(columns):

    keywords = [
        "date",
        "timestamp",
        "inspected_on",
        "inspected_at",
    ]

    return [
        name
        for name in columns
        if any(
            keyword in normalized_name(name)
            for keyword in keywords
        )
    ][:MAX_DATE_CANDIDATES]


def profile_dates(df, date_columns):

    results = {}

    for name in date_columns:

        value = normalized_value(name)

        # Probe common date and timestamp representations.
        # Date validation is format-based, not a guarantee
        # of business-level chronological correctness.
        parsed_options = [
            F.to_timestamp(value, pattern)
            for pattern in DATE_FORMATS
        ]

        parsed = F.coalesce(*parsed_options)

        summary = df.agg(
            F.count(value).alias("populated"),

            F.sum(
                F.when(
                    value.isNotNull()
                    & parsed.isNotNull(),
                    1
                ).otherwise(0)
            ).alias("parseable"),

            F.min(parsed).alias("earliest"),
            F.max(parsed).alias("latest"),
        ).first()

        populated = int(
            summary["populated"] or 0
        )

        parseable = int(
            summary["parseable"] or 0
        )

        results[name] = {
            "populated_records": populated,
            "parseable_records": parseable,
            "unparseable_records": populated - parseable,
            "parseable_percentage": percent(
                parseable, populated
            ),
            "earliest_parsed_value": (
                str(summary["earliest"])
                if summary["earliest"] is not None
                else None
            ),
            "latest_parsed_value": (
                str(summary["latest"])
                if summary["latest"] is not None
                else None
            ),
            "note": (
                "Unparseable means no configured date "
                "format matched; other valid formats may exist."
            ),
        }

    return results


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

def run_profiling():

    file_path, header = discover_source()
    spark = create_spark()

    try:

        df = load_source(
            spark, file_path, header
        )

        print("\nPYSPARK SOURCE SCHEMA:")
        df.printSchema()

        total_rows, column_quality = profile_columns(df)

        business_fields = discover_business_fields(
            df.columns
        )

        identifier_columns = discover_identifier_candidates(
            df.columns
        )

        identifiers = validate_identifiers(
            df, total_rows, identifier_columns
        )

        numeric_columns = profile_numeric_columns(df)

        date_columns = discover_date_candidates(
            df.columns
        )

        date_quality = profile_dates(
            df, date_columns
        )

        print("\nSAMPLE SOURCE RECORDS")
        df.select(
            *df.columns[:10]
        ).show(5, truncate=35)

        report = {
            "project": "AssetFusion360",
            "profiling_step": "4.7C",
            "dataset": "Supplementary Inspection Data",
            "status": "COMPLETED",
            "profile_timestamp_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "source_file": str(
                file_path.relative_to(PROJECT_ROOT)
            ),
            "spark_version": spark.version,
            "total_source_files": 1,
            "total_records": total_rows,
            "total_columns": len(header),
            "source_columns": header,
            "source_schema": [
                {
                    "column_name": field.name,
                    "data_type": field.dataType.simpleString(),
                    "nullable": field.nullable,
                }
                for field in df.schema.fields
            ],
            "column_quality": column_quality,
            "discovered_business_fields": business_fields,
            "candidate_identifier_columns": identifier_columns,
            "identifier_validation": identifiers,
            "numeric_quality": numeric_columns,
            "date_quality": date_quality,
            "notes": [
                "Original CSV remains unchanged.",
                "All source fields were read as strings.",
                "Missing and placeholder values are separate.",
                "No identifiers were fabricated.",
                "No inspection-to-AMPS join was performed.",
                "Numeric and date conversions were tested only.",
                "Source provenance and business meanings "
                "require separate validation.",
            ],
        }

        save_report(report)

        print("\n" + "=" * 68)
        print("SUPPLEMENTARY INSPECTION PROFILING SUMMARY")
        print("=" * 68)

        print(f"Source file: {file_path.name}")
        print(f"Total records: {total_rows:,}")
        print(f"Total columns: {len(header)}")

        print("\nDISCOVERED BUSINESS FIELDS:")
        print(
            json.dumps(
                business_fields,
                indent=2
            )
        )

        print("\nIDENTIFIER VALIDATION:")
        print(
            json.dumps(
                identifiers,
                indent=2
            )
        )

        print("\nCOLUMN QUALITY:")
        for item in column_quality:
            print(
                f"{item['column_name']}: "
                f"{item['missing_count']:,} missing "
                f"({item['missing_percentage']}%), "
                f"{item['dash_placeholder_count']:,} dashes, "
                f"{item['literal_null_count']:,} literal NULL, "
                f"{item['na_placeholder_count']:,} N/A"
            )

        print("\nNUMERIC CANDIDATES:")
        print(
            json.dumps(
                numeric_columns,
                indent=2
            )
        )

        print("\nDATE QUALITY:")
        print(
            json.dumps(
                date_quality,
                indent=2
            )
        )

        print("\n" + "=" * 68)
        print("SUPPLEMENTARY INSPECTION PROFILING COMPLETED")
        print("=" * 68)

        print("\nJSON report saved to:")
        print(REPORT_FILE)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_profiling()
