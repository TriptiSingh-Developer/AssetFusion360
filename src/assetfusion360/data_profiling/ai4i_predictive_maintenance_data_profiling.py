
"""
AssetFusion360 Capstone Project

STEP 4.7A
AI4I Predictive Maintenance - Source Data Profiling

Checks:
    1. Source file discovery
    2. CSV header and schema validation
    3. Record counts
    4. Missing and blank values
    5. Dash placeholders
    6. Candidate identifier uniqueness
    7. Numeric format and range analysis
    8. Failure target and failure-mode distributions
    9. Source record samples
   10. Permanent JSON profiling report

Technology:
    Python 3.12
    PySpark 3.5.7
    No pandas

Original source files are never modified.
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
    / "ai4i_predictive_maintenance"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "ai4i_predictive_maintenance_data_profile.json"
)

EXPECTED_CSV_FILE_COUNT = 1

NUMERIC_PATTERN = (
    r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)"
    r"([eE][+-]?[0-9]+)?$"
)

# Known possible AI4I failure-related column names.
# Only columns actually present in the source are used.
FAILURE_COLUMN_NAMES = {
    "machine failure",
    "machine_failure",
    "target",
    "twf",
    "hdf",
    "pwf",
    "osf",
    "rnf",
    "tool wear failure",
    "heat dissipation failure",
    "power failure",
    "overstrain failure",
    "random failures",
}


# ============================================================
# 2. DISCOVER SOURCE CSV
# ============================================================

def discover_source_file():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source folder not found: {SOURCE_DIR}"
        )

    csv_files = sorted(
        path
        for path in SOURCE_DIR.iterdir()
        if path.is_file()
        and path.suffix.lower() == ".csv"
    )

    if len(csv_files) != EXPECTED_CSV_FILE_COUNT:
        raise ValueError(
            "Expected exactly one AI4I CSV file. "
            f"Found {len(csv_files)}. "
            f"Files: {[p.name for p in csv_files]}"
        )

    file_path = csv_files[0]

    if file_path.stat().st_size == 0:
        raise ValueError(
            f"Source file is empty: {file_path}"
        )

    print("\n" + "=" * 65)
    print("AI4I - SOURCE FILE INVENTORY")
    print("=" * 65)

    print(f"Filename: {file_path.name}")
    print(
        f"Size: {file_path.stat().st_size:,} bytes"
    )

    return file_path


# ============================================================
# 3. READ AND VALIDATE CSV HEADER
# ============================================================

def read_source_header(file_path):

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
            "The source CSV does not have a valid header."
        )

    if len(header) != len(set(header)):
        raise ValueError(
            "Duplicate column names detected."
        )

    if any(not name.strip() for name in header):
        raise ValueError(
            "Blank source column names detected."
        )

    print("\nAI4I SOURCE COLUMNS")
    print("-" * 55)

    for index, column_name in enumerate(
        header,
        start=1
    ):
        print(
            f"{index:02d}. {column_name}"
        )

    print(
        f"\nTotal columns: {len(header)}"
    )

    return header


# ============================================================
# 4. CREATE PYSPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_AI4I_Source_Profiling"
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
# 5. LOAD SOURCE FILE
# ============================================================

def load_source(spark, file_path, header):

    schema = StructType([
        StructField(
            name,
            StringType(),
            True
        )
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
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(file_path.resolve()))
    )

    if df.columns != header:
        raise ValueError(
            "Spark schema differs from the source header."
        )

    print("\nPYSPARK SCHEMA")
    df.printSchema()

    return df


# ============================================================
# 6. MISSING AND PLACEHOLDER VALUE PROFILING
# ============================================================

def profile_columns(df):

    expressions = [
        F.count(F.lit(1)).alias("__total_rows")
    ]

    for index, name in enumerate(df.columns):

        col = F.col(name)
        trimmed = F.trim(col)

        expressions.extend([
            F.sum(
                F.when(
                    col.isNull() | (trimmed == ""),
                    1
                ).otherwise(0)
            ).alias(f"missing_{index}"),

            F.sum(
                F.when(
                    trimmed == "-",
                    1
                ).otherwise(0)
            ).alias(f"dash_{index}"),
        ])

    summary = df.agg(*expressions).first()

    total_rows = int(summary["__total_rows"])

    results = []

    for index, name in enumerate(df.columns):

        missing = int(
            summary[f"missing_{index}"] or 0
        )

        dash = int(
            summary[f"dash_{index}"] or 0
        )

        results.append({
            "column_name": name,
            "missing_count": missing,
            "missing_percentage": (
                round(100 * missing / total_rows, 2)
                if total_rows else 0.0
            ),
            "non_missing_count": total_rows - missing,
            "dash_placeholder_count": dash,
        })

    return total_rows, results


# ============================================================
# 7. IDENTIFIER VALIDATION
# ============================================================

def discover_identifier_columns(columns):

    explicit_names = {
        "udi",
        "product id",
        "product_id",
        "machine id",
        "machine_id",
        "record id",
        "record_id",
    }

    results = []

    for name in columns:

        normalized = name.strip().lower()

        if (
            normalized in explicit_names
            or normalized.endswith("_id")
        ):
            results.append(name)

    return results


def validate_identifiers(
    df,
    total_rows,
    identifier_columns
):

    results = {}

    for name in identifier_columns:

        value = F.trim(F.col(name))

        valid = df.filter(
            F.col(name).isNotNull()
            & (value != "")
        )

        non_missing = valid.count()

        distinct_count = (
            valid
            .select(value.alias("__identifier"))
            .distinct()
            .count()
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
# 8. NUMERIC FORMAT AND RANGE ANALYSIS
# ============================================================

def profile_numeric_columns(df, total_rows):
    """
    Inspect all source fields without assuming types.

    A column is a numeric candidate when at least
    95% of its populated values are numeric.

    Actual type conversion is deferred to Silver.
    """

    results = {}

    for name in df.columns:

        value = F.trim(F.col(name))

        populated = (
            F.col(name).isNotNull()
            & (value != "")
        )

        numeric_format = (
            value.rlike(NUMERIC_PATTERN)
        )

        number = F.expr(
            "try_cast("
            f"trim(`{name.replace('`', '``')}`)"
            " AS DOUBLE)"
        )

        summary = df.agg(
            F.sum(
                F.when(populated, 1).otherwise(0)
            ).alias("populated"),

            F.sum(
                F.when(
                    populated & numeric_format,
                    1
                ).otherwise(0)
            ).alias("numeric_format"),

            F.sum(
                F.when(
                    populated
                    & numeric_format
                    & number.isNotNull()
                    & (~F.isnan(number)),
                    1
                ).otherwise(0)
            ).alias("valid_numeric"),

            F.min(
                F.when(
                    numeric_format
                    & number.isNotNull()
                    & (~F.isnan(number)),
                    number
                )
            ).alias("minimum"),

            F.max(
                F.when(
                    numeric_format
                    & number.isNotNull()
                    & (~F.isnan(number)),
                    number
                )
            ).alias("maximum"),
        ).first()

        populated_count = int(
            summary["populated"] or 0
        )

        format_count = int(
            summary["numeric_format"] or 0
        )

        valid_count = int(
            summary["valid_numeric"] or 0
        )

        numeric_ratio = (
            valid_count / populated_count
            if populated_count else 0.0
        )

        if populated_count and numeric_ratio >= 0.95:

            results[name] = {
                "non_missing_records": populated_count,
                "valid_numeric_records": valid_count,
                "invalid_or_non_numeric_records": (
                    populated_count - valid_count
                ),
                "minimum": summary["minimum"],
                "maximum": summary["maximum"],
                "numeric_coverage_percentage": round(
                    100 * numeric_ratio,
                    2
                ),
                "is_numeric_candidate": True,
                "note": (
                    "Numeric formatting does not establish "
                    "the business meaning of the field."
                ),
            }

    return results


# ============================================================
# 9. FAILURE TARGET DISTRIBUTIONS
# ============================================================

def profile_failure_columns(df):

    results = {}

    for name in df.columns:

        normalized_name = (
            re.sub(
                r"\s+",
                " ",
                name.strip().lower()
            )
        )

        if normalized_name not in FAILURE_COLUMN_NAMES:
            continue

        counts = (
            df.groupBy(name)
            .count()
            .orderBy(F.desc("count"))
            .limit(20)
            .collect()
        )

        results[name] = [
            {
                "value": row[name],
                "record_count": int(row["count"]),
            }
            for row in counts
        ]

    return results


# ============================================================
# 10. MAIN PROFILING EXECUTION
# ============================================================

def run_profiling():

    file_path = discover_source_file()

    header = read_source_header(file_path)

    spark = create_spark_session()

    try:

        df = load_source(
            spark,
            file_path,
            header
        )

        print("\n" + "=" * 65)
        print("AI4I PREDICTIVE MAINTENANCE - DATA PROFILING")
        print("=" * 65)

        total_rows, column_quality = (
            profile_columns(df)
        )

        identifier_columns = (
            discover_identifier_columns(df.columns)
        )

        identifier_validation = (
            validate_identifiers(
                df,
                total_rows,
                identifier_columns
            )
        )

        numeric_quality = (
            profile_numeric_columns(
                df,
                total_rows
            )
        )

        failure_distributions = (
            profile_failure_columns(df)
        )

        print("\nSAMPLE SOURCE RECORDS")
        df.show(5, truncate=35)

        report = {
            "project": "AssetFusion360",
            "profiling_step": "4.7A",
            "dataset": "AI4I Predictive Maintenance",
            "source_file": str(
                file_path.relative_to(PROJECT_ROOT)
            ),
            "profile_timestamp_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "spark_version": spark.version,
            "total_source_files": 1,
            "total_records": total_rows,
            "total_columns": len(df.columns),
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
            "identifier_validation": (
                identifier_validation
            ),
            "numeric_quality": numeric_quality,
            "failure_target_distributions": (
                failure_distributions
            ),
            "notes": [
                "Original source CSV remains unchanged.",
                "All source columns were read as strings.",
                "Numeric candidates are identified from values.",
                "No missing values were imputed.",
                "Failure label semantics require verification.",
                "AI4I records are not assumed to match AMPS assets.",
                "Model training and validation are not yet performed.",
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
        print("AI4I DATA PROFILING SUMMARY")
        print("=" * 65)

        print(f"Source file: {file_path.name}")
        print(f"Total records: {total_rows:,}")
        print(f"Total columns: {len(df.columns)}")

        print("\nIDENTIFIER VALIDATION:")
        print(json.dumps(
            identifier_validation,
            indent=2
        ))

        print("\nCOLUMN MISSING VALUES:")
        for column in column_quality:
            print(
                f"{column['column_name']}: "
                f"{column['missing_count']:,} missing, "
                f"{column['dash_placeholder_count']:,} "
                "dash placeholders"
            )

        print("\nNUMERIC CANDIDATE COLUMNS:")
        print(json.dumps(
            numeric_quality,
            indent=2
        ))

        print("\nFAILURE TARGET DISTRIBUTIONS:")
        print(json.dumps(
            failure_distributions,
            indent=2
        ))

        print("\n" + "=" * 65)
        print("AI4I PROFILING COMPLETED SUCCESSFULLY")
        print("=" * 65)

        print("\nJSON report saved to:")
        print(REPORT_FILE)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_profiling()
