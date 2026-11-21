
"""
AssetFusion360 Capstone Project
NYC Parks AMPS Assets - Comprehensive Source Data Profiling

STEP 4.5:
    Schema inspection and source data quality analysis.

Checks:
    1. Schema and record counts
    2. Missing and blank values
    3. OBJ_CODE identifier uniqueness
    4. Duplicate asset-code records
    5. OBJ_SQLIDENTITY primary-key validation
    6. Asset status distribution
    7. Numeric format and conversion validation
    8. Date and timestamp validation
    9. Exact full-row duplicates
   10. Negative, zero, and unusual numeric values
   11. Coordinate completeness and outlier analysis
   12. Commission and withdrawal date consistency
   13. Placeholder and suspicious asset records

Technology:
    Python 3.12
    PySpark 3.5.7
    No pandas

Input:
    data/01_raw_source_data/
        nyc_parks_asset_management/
        AMPS_Assets.csv

Output:
    reports/data_profiling_reports/
        nyc_parks_amps_assets_profile.json

The original source data is never modified.
"""

import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. PROJECT PATHS AND CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
    / "AMPS_Assets.csv"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

OUTPUT_FILE = (
    OUTPUT_DIR
    / "nyc_parks_amps_assets_profile.json"
)

EXPECTED_COLUMN_COUNT = 29

NUMERIC_COLUMNS = {
    "OBJ_SQLIDENTITY": "BIGINT",
    "OBJ_VALUE": "DOUBLE",
    "OBJ_XLOCATION": "DOUBLE",
    "OBJ_YLOCATION": "DOUBLE",
    "OBJ_UPDATECOUNT": "BIGINT",
    "OBJ_GISOBJID": "BIGINT",
}

DATE_COLUMNS = [
    "OBJ_COMMISS",
    "OBJ_WITHDRAW",
    "OBJ_RECORD",
    "OBJ_UPDATED",
    "OBJ_CREATED",
]

DATE_FORMATS = [
    "MMM d yyyy h:mma",
    "MMM d yyyy h:mm a",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "M/d/yyyy H:mm:ss",
    "M/d/yyyy H:mm",
    "M/d/yyyy h:mm a",
    "M/d/yyyy",
]

INTEGER_PATTERN = r"^[+-]?[0-9]+$"

DECIMAL_PATTERN = (
    r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)"
    r"([eE][+-]?[0-9]+)?$"
)


# ============================================================
# 2. PYSPARK INITIALIZATION
# ============================================================

def create_spark_session():

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName("AssetFusion360_AMPS_Assets_Profiling")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 3. LOAD ORIGINAL SOURCE DATA
# ============================================================

def load_assets(spark):

    if not INPUT_FILE.is_file():
        raise FileNotFoundError(
            f"Source file not found: {INPUT_FILE}"
        )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .csv(str(INPUT_FILE))
    )

    if len(df.columns) != EXPECTED_COLUMN_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_COLUMN_COUNT} columns, "
            f"found {len(df.columns)}."
        )

    return df


# ============================================================
# 4. SHARED HELPERS
# ============================================================

def non_missing_condition(column_name):

    column = F.col(column_name)

    return (
        column.isNotNull()
        & (F.trim(column) != "")
    )


def normalized_datetime(column_name):

    return F.regexp_replace(
        F.trim(F.col(column_name)),
        r"\s+",
        " "
    )


def parsed_timestamp(column_name):
    """
    Parse the source value using validated candidate formats.

    NULL source values remain NULL.
    Invalid values produce NULL in the temporary expression.
    """

    normalized = normalized_datetime(column_name)

    expressions = [
        F.expr(
            "try_to_timestamp("
            f"regexp_replace(trim(`{column_name}`), "
            "'\\\\s+', ' '), "
            f"'{date_format}')"
        )
        for date_format in DATE_FORMATS
    ]

    return F.coalesce(*expressions)


def limited_values(df, column_name, limit=5):

    rows = (
        df.filter(non_missing_condition(column_name))
        .select(column_name)
        .distinct()
        .limit(limit)
        .collect()
    )

    return [row[column_name] for row in rows]


def print_json(title, result):

    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

    print(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False
        )
    )


# ============================================================
# 5. SCHEMA AND MISSING VALUE PROFILING
# ============================================================

def profile_columns(df):

    total_rows = df.count()

    expressions = []

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

    missing_counts = df.agg(*expressions).first()

    profiles = []

    for index, field in enumerate(df.schema.fields):

        missing = int(missing_counts[index] or 0)

        profiles.append({
            "column_name": field.name,
            "source_spark_type": (
                field.dataType.simpleString()
            ),
            "nullable": field.nullable,
            "missing_count": missing,
            "missing_percentage": (
                round(100 * missing / total_rows, 2)
                if total_rows else 0.0
            ),
            "non_missing_count": total_rows - missing,
        })

    return total_rows, profiles


# ============================================================
# 6. OBJ_CODE IDENTIFIER UNIQUENESS
# ============================================================

def profile_asset_identifiers(df):

    valid = (
        df.filter(non_missing_condition("OBJ_CODE"))
        .select(
            F.trim(F.col("OBJ_CODE")).alias("asset_code")
        )
    )

    non_empty_count = valid.count()

    distinct_count = (
        valid.distinct().count()
    )

    duplicate_excess = (
        non_empty_count - distinct_count
    )

    return {
        "candidate_identifier": "OBJ_CODE",
        "non_empty_records": non_empty_count,
        "distinct_identifiers": distinct_count,
        "duplicate_excess_rows": duplicate_excess,
        "is_unique": duplicate_excess == 0,
    }


# ============================================================
# 7. DUPLICATE OBJ_CODE RECORD INVESTIGATION
# ============================================================

def inspect_duplicate_asset_codes(df):

    valid = (
        df.filter(non_missing_condition("OBJ_CODE"))
        .withColumn(
            "__normalized_code",
            F.trim(F.col("OBJ_CODE"))
        )
    )

    duplicates = (
        valid.groupBy("__normalized_code")
        .count()
        .filter(F.col("count") > 1)
        .orderBy(
            F.desc("count"),
            F.asc("__normalized_code")
        )
    )

    duplicate_codes = [
        {
            "asset_code": row["__normalized_code"],
            "occurrences": int(row["count"]),
        }
        for row in duplicates.collect()
    ]

    duplicate_rows_df = valid.join(
        duplicates.select("__normalized_code"),
        on="__normalized_code",
        how="inner"
    )

    selected_columns = [
        "OBJ_CODE",
        "OBJ_OBTYPE",
        "OBJ_DESC",
        "OBJ_CLASS",
        "OBJ_STATUS",
        "OBJ_SQLIDENTITY",
    ]

    duplicate_rows = [
        row.asDict(recursive=True)
        for row in (
            duplicate_rows_df
            .select(*selected_columns)
            .limit(100)
            .collect()
        )
    ]

    result = {
        "duplicate_identifier_groups": len(duplicate_codes),
        "duplicate_codes": duplicate_codes,
        "duplicate_records_sample": duplicate_rows,
        "sample_limit": 100,
    }

    print_json(
        "DUPLICATE ASSET CODE INVESTIGATION",
        result
    )

    return result


# ============================================================
# 8. OBJ_SQLIDENTITY PRIMARY KEY VALIDATION
# ============================================================

def validate_asset_primary_key(df):

    column = "OBJ_SQLIDENTITY"

    total_rows = df.count()

    valid = df.filter(
        non_missing_condition(column)
    )

    non_missing = valid.count()

    normalized = F.trim(F.col(column))

    distinct_strings = (
        valid.select(
            normalized.alias("identifier")
        )
        .distinct()
        .count()
    )

    invalid_format = valid.filter(
        ~normalized.rlike(INTEGER_PATTERN)
    ).count()

    converted = valid.withColumn(
        "__integer_id",
        F.expr(
            "try_cast(trim(`OBJ_SQLIDENTITY`) AS BIGINT)"
        )
    )

    invalid_conversion = converted.filter(
        F.col("__integer_id").isNull()
    ).count()

    valid_converted = converted.filter(
        F.col("__integer_id").isNotNull()
    )

    converted_rows = valid_converted.count()

    distinct_converted = (
        valid_converted
        .select("__integer_id")
        .distinct()
        .count()
    )

    result = {
        "candidate_primary_key": column,
        "total_records": total_rows,
        "missing_records": total_rows - non_missing,
        "distinct_string_identifiers": distinct_strings,
        "duplicate_string_identifier_excess": (
            non_missing - distinct_strings
        ),
        "invalid_integer_format_records": invalid_format,
        "invalid_bigint_conversion_records": (
            invalid_conversion
        ),
        "distinct_converted_bigint_identifiers": (
            distinct_converted
        ),
        "duplicate_converted_bigint_excess": (
            converted_rows - distinct_converted
        ),
        "is_candidate_primary_key": (
            non_missing == total_rows
            and distinct_strings == total_rows
            and invalid_format == 0
            and invalid_conversion == 0
            and distinct_converted == total_rows
        ),
    }

    return result


# ============================================================
# 9. ASSET STATUS DISTRIBUTION
# ============================================================

def profile_asset_status(df):

    rows = (
        df.groupBy("OBJ_STATUS")
        .count()
        .orderBy(
            F.desc("count"),
            F.asc("OBJ_STATUS")
        )
        .collect()
    )

    return [
        {
            "status": row["OBJ_STATUS"],
            "record_count": int(row["count"]),
        }
        for row in rows
    ]


# ============================================================
# 10. NUMERIC DATA TYPE VALIDATION
# ============================================================

def validate_numeric_columns(df):

    results = {}

    for column_name, target_type in NUMERIC_COLUMNS.items():

        valid = df.filter(
            non_missing_condition(column_name)
        )

        non_missing = valid.count()

        missing = (
            df.count() - non_missing
        )

        normalized = F.trim(
            F.col(column_name)
        )

        pattern = (
            INTEGER_PATTERN
            if target_type == "BIGINT"
            else DECIMAL_PATTERN
        )

        invalid_format_df = valid.filter(
            ~normalized.rlike(pattern)
        )

        invalid_format = invalid_format_df.count()

        converted = valid.withColumn(
            "__parsed_number",
            F.expr(
                f"try_cast(trim(`{column_name}`) "
                f"AS {target_type})"
            )
        )

        invalid_conversion = converted.filter(
            F.col("__parsed_number").isNull()
        ).count()

        result = {
            "target_spark_type": target_type,
            "non_missing_records": non_missing,
            "missing_records": missing,
            "invalid_format_records": invalid_format,
            "invalid_conversion_records": invalid_conversion,
            "sample_source_values": limited_values(
                df, column_name
            ),
            "conversion_eligible": (
                invalid_format == 0
                and invalid_conversion == 0
            ),
        }

        results[column_name] = result

    return results


# ============================================================
# 11. DATE AND TIMESTAMP VALIDATION
# ============================================================

def validate_date_columns(df):

    results = {}

    total_rows = df.count()

    for column_name in DATE_COLUMNS:

        valid = df.filter(
            non_missing_condition(column_name)
        )

        non_missing = valid.count()

        parsed = valid.withColumn(
            "__parsed_datetime",
            parsed_timestamp(column_name)
        )

        unparseable_df = parsed.filter(
            F.col("__parsed_datetime").isNull()
        )

        unparseable = unparseable_df.count()

        parseable = non_missing - unparseable

        invalid_examples = [
            row[column_name]
            for row in (
                unparseable_df
                .select(column_name)
                .distinct()
                .limit(10)
                .collect()
            )
        ]

        results[column_name] = {
            "target_spark_type": "timestamp",
            "non_missing_records": non_missing,
            "missing_records": total_rows - non_missing,
            "parseable_records": parseable,
            "unparseable_records": unparseable,
            "parse_success_percentage": (
                round(100 * parseable / non_missing, 2)
                if non_missing else 0.0
            ),
            "tested_formats": DATE_FORMATS,
            "sample_source_values": limited_values(
                df, column_name
            ),
            "unparseable_examples": invalid_examples,
            "all_non_missing_values_parseable": (
                unparseable == 0
            ),
        }

    return results


# ============================================================
# 12. NEW: EXACT FULL-ROW DUPLICATE ANALYSIS
# ============================================================

def profile_full_row_duplicates(df):
    """
    Compare all source columns.

    Exact duplicates mean every column contains the
    same source value, including NULL positions.

    No records are removed.
    """

    total_records = df.count()

    unique_records = (
        df.dropDuplicates().count()
    )

    duplicate_excess = (
        total_records - unique_records
    )

    result = {
        "total_records": total_records,
        "distinct_full_rows": unique_records,
        "exact_duplicate_excess_rows": duplicate_excess,
        "all_full_rows_unique": duplicate_excess == 0,
        "note": (
            "This check compares all original columns. "
            "Duplicate OBJ_CODE values do not necessarily "
            "represent exact duplicate records."
        ),
    }

    print_json(
        "EXACT FULL-ROW DUPLICATE ANALYSIS",
        result
    )

    return result


# ============================================================
# 13. NEW: ASSET VALUE QUALITY ANALYSIS
# ============================================================

def profile_asset_values(df):
    """
    Profile non-missing asset values.

    Negative and zero values are flagged for review,
    not automatically classified as erroneous.
    """

    valid = (
        df.filter(non_missing_condition("OBJ_VALUE"))
        .withColumn(
            "__asset_value",
            F.expr(
                "try_cast(trim(`OBJ_VALUE`) AS DOUBLE)"
            )
        )
    )

    numeric = valid.filter(
        F.col("__asset_value").isNotNull()
        & (~F.isnan(F.col("__asset_value")))
    )

    non_missing = valid.count()

    valid_numeric = numeric.count()

    summary = numeric.agg(
        F.min("__asset_value").alias("minimum"),
        F.max("__asset_value").alias("maximum"),
        F.avg("__asset_value").alias("average"),
        F.sum(
            F.when(
                F.col("__asset_value") < 0, 1
            ).otherwise(0)
        ).alias("negative_count"),
        F.sum(
            F.when(
                F.col("__asset_value") == 0, 1
            ).otherwise(0)
        ).alias("zero_count"),
    ).first()

    largest_values = [
        {
            "asset_code": row["OBJ_CODE"],
            "asset_description": row["OBJ_DESC"],
            "asset_value": row["__asset_value"],
        }
        for row in (
            numeric.orderBy(
                F.desc("__asset_value")
            )
            .select(
                "OBJ_CODE",
                "OBJ_DESC",
                "__asset_value"
            )
            .limit(10)
            .collect()
        )
    ]

    result = {
        "source_column": "OBJ_VALUE",
        "non_missing_records": non_missing,
        "valid_numeric_records": valid_numeric,
        "minimum_value": summary["minimum"],
        "maximum_value": summary["maximum"],
        "average_value": summary["average"],
        "negative_value_records": int(
            summary["negative_count"] or 0
        ),
        "zero_value_records": int(
            summary["zero_count"] or 0
        ),
        "largest_values_sample": largest_values,
        "note": (
            "Missing, zero, and negative values are "
            "preserved. Their business meanings must "
            "be verified before cleaning."
        ),
    }

    print_json(
        "ASSET VALUE QUALITY ANALYSIS",
        result
    )

    return result


# ============================================================
# 14. NEW: COORDINATE QUALITY AND OUTLIER ANALYSIS
# ============================================================

def profile_asset_coordinates(df):
    """
    Check coordinate completeness, numeric validity,
    zero values, and statistical outliers.

    IQR outliers are review candidates, not confirmed
    invalid coordinates.

    Coordinate reference system remains unverified.
    """

    total_records = df.count()

    x_present = non_missing_condition(
        "OBJ_XLOCATION"
    )

    y_present = non_missing_condition(
        "OBJ_YLOCATION"
    )

    x_only = df.filter(
        x_present & (~y_present)
    ).count()

    y_only = df.filter(
        y_present & (~x_present)
    ).count()

    both_missing = df.filter(
        (~x_present) & (~y_present)
    ).count()

    both_present = df.filter(
        x_present & y_present
    ).count()

    coordinates = (
        df.filter(x_present & y_present)
        .withColumn(
            "__x",
            F.expr(
                "try_cast(trim(`OBJ_XLOCATION`) AS DOUBLE)"
            )
        )
        .withColumn(
            "__y",
            F.expr(
                "try_cast(trim(`OBJ_YLOCATION`) AS DOUBLE)"
            )
        )
    )

    valid_coordinates = coordinates.filter(
        F.col("__x").isNotNull()
        & F.col("__y").isNotNull()
        & (~F.isnan(F.col("__x")))
        & (~F.isnan(F.col("__y")))
    )

    valid_count = valid_coordinates.count()

    summary = valid_coordinates.agg(
        F.min("__x").alias("min_x"),
        F.max("__x").alias("max_x"),
        F.min("__y").alias("min_y"),
        F.max("__y").alias("max_y"),
        F.sum(
            F.when(
                (F.col("__x") == 0)
                | (F.col("__y") == 0),
                1
            ).otherwise(0)
        ).alias("zero_coordinate_count"),
    ).first()

    bounds = {}
    outlier_conditions = []

    for axis in ["x", "y"]:

        q1, q3 = valid_coordinates.approxQuantile(
            f"__{axis}",
            [0.25, 0.75],
            0.01
        )

        iqr = q3 - q1

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        bounds[axis] = {
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_review_boundary": lower,
            "upper_review_boundary": upper,
        }

        outlier_conditions.append(
            (F.col(f"__{axis}") < lower)
            | (F.col(f"__{axis}") > upper)
        )

    possible_outliers = valid_coordinates.filter(
        outlier_conditions[0]
        | outlier_conditions[1]
    )

    outlier_count = possible_outliers.count()

    outlier_samples = [
        {
            "asset_code": row["OBJ_CODE"],
            "x": row["__x"],
            "y": row["__y"],
        }
        for row in (
            possible_outliers
            .select("OBJ_CODE", "__x", "__y")
            .limit(10)
            .collect()
        )
    ]

    result = {
        "total_records": total_records,
        "both_coordinates_present": both_present,
        "both_coordinates_missing": both_missing,
        "only_x_present": x_only,
        "only_y_present": y_only,
        "valid_numeric_coordinate_pairs": valid_count,
        "invalid_numeric_coordinate_pairs": (
            both_present - valid_count
        ),
        "minimum_x": summary["min_x"],
        "maximum_x": summary["max_x"],
        "minimum_y": summary["min_y"],
        "maximum_y": summary["max_y"],
        "zero_coordinate_records": int(
            summary["zero_coordinate_count"] or 0
        ),
        "iqr_review_boundaries": bounds,
        "possible_statistical_outliers": outlier_count,
        "outlier_samples": outlier_samples,
        "coordinate_reference_system_verified": False,
        "note": (
            "X/Y values appear to use projected coordinates. "
            "The official coordinate reference system must "
            "be verified before geospatial conversion. "
            "IQR flags do not prove that coordinates are wrong."
        ),
    }

    print_json(
        "ASSET COORDINATE QUALITY ANALYSIS",
        result
    )

    return result


# ============================================================
# 15. NEW: DATE RELATIONSHIP QUALITY CHECKS
# ============================================================

def profile_date_relationships(df):
    """
    Compare commission and withdrawal timestamps.

    Withdrawal earlier than commission is a potential
    inconsistency requiring business review.

    Missing dates remain missing.
    """

    working = (
        df.withColumn(
            "__commission",
            parsed_timestamp("OBJ_COMMISS")
        )
        .withColumn(
            "__withdrawal",
            parsed_timestamp("OBJ_WITHDRAW")
        )
    )

    both_dates = working.filter(
        F.col("__commission").isNotNull()
        & F.col("__withdrawal").isNotNull()
    )

    comparable_count = both_dates.count()

    suspicious = both_dates.filter(
        F.col("__withdrawal") < F.col("__commission")
    )

    suspicious_count = suspicious.count()

    suspicious_samples = [
        {
            "asset_code": row["OBJ_CODE"],
            "commission_source": row["OBJ_COMMISS"],
            "withdrawal_source": row["OBJ_WITHDRAW"],
            "asset_status": row["OBJ_STATUS"],
        }
        for row in (
            suspicious
            .select(
                "OBJ_CODE",
                "OBJ_COMMISS",
                "OBJ_WITHDRAW",
                "OBJ_STATUS"
            )
            .limit(10)
            .collect()
        )
    ]

    result = {
        "records_with_both_parseable_dates": comparable_count,
        "withdrawal_before_commission_count": suspicious_count,
        "suspicious_records_sample": suspicious_samples,
        "note": (
            "Flagged records require validation of "
            "the source system's date semantics. "
            "No date values have been altered."
        ),
    }

    print_json(
        "DATE RELATIONSHIP QUALITY ANALYSIS",
        result
    )

    return result


# ============================================================
# 16. NEW: PLACEHOLDER AND SUSPICIOUS RECORDS
# ============================================================

def profile_placeholder_records(df):
    """
    Identify obvious placeholders and records whose
    descriptions explicitly mention duplication.

    Flags may overlap; counts are not additive.
    """

    placeholder_code = df.filter(
        F.trim(F.col("OBJ_CODE")) == "*"
    )

    duplicate_description = df.filter(
        F.lower(
            F.coalesce(
                F.col("OBJ_DESC"),
                F.lit("")
            )
        ).contains("duplicat")
    )

    retired_description = df.filter(
        F.lower(
            F.coalesce(
                F.col("OBJ_DESC"),
                F.lit("")
            )
        ).contains("retired")
    )

    placeholder_samples = [
        row.asDict(recursive=True)
        for row in (
            placeholder_code
            .select(
                "OBJ_CODE",
                "OBJ_DESC",
                "OBJ_STATUS",
                "OBJ_SQLIDENTITY"
            )
            .limit(10)
            .collect()
        )
    ]

    result = {
        "placeholder_asset_code_records": (
            placeholder_code.count()
        ),
        "description_mentions_duplicate_records": (
            duplicate_description.count()
        ),
        "description_mentions_retired_records": (
            retired_description.count()
        ),
        "placeholder_samples": placeholder_samples,
        "note": (
            "These are review flags, not confirmed "
            "errors. All records remain in raw data "
            "and will be preserved in Bronze."
        ),
    }

    print_json(
        "PLACEHOLDER AND SUSPICIOUS RECORD ANALYSIS",
        result
    )

    return result


# ============================================================
# 17. EXECUTE ALL PROFILING CHECKS
# ============================================================

def run_profiling():

    spark = create_spark_session()

    try:

        print("\n" + "=" * 65)
        print("ASSETFUSION360 - NYC PARKS AMPS ASSETS")
        print("COMPREHENSIVE SOURCE DATA PROFILING")
        print("=" * 65)

        df = load_assets(spark)

        # Cache the original source DataFrame because
        # several profiling checks reuse it.
        df = df.cache()

        total_rows, column_quality = (
            profile_columns(df)
        )

        identifier_profile = (
            profile_asset_identifiers(df)
        )

        duplicate_identifier_details = (
            inspect_duplicate_asset_codes(df)
        )

        primary_key_validation = (
            validate_asset_primary_key(df)
        )

        status_distribution = (
            profile_asset_status(df)
        )

        numeric_validation = (
            validate_numeric_columns(df)
        )

        date_validation = (
            validate_date_columns(df)
        )

        # NEW CHECKS IN STEP 4.5

        full_row_duplicates = (
            profile_full_row_duplicates(df)
        )

        asset_value_quality = (
            profile_asset_values(df)
        )

        coordinate_quality = (
            profile_asset_coordinates(df)
        )

        date_relationship_quality = (
            profile_date_relationships(df)
        )

        placeholder_quality = (
            profile_placeholder_records(df)
        )

        # ----------------------------------------------------
        # Construct the complete report
        # ----------------------------------------------------

        report = {
            "project": "AssetFusion360",
            "dataset": "NYC Parks AMPS Assets",
            "profiling_step": "4.5",
            "source_file": str(
                INPUT_FILE.relative_to(PROJECT_ROOT)
            ),
            "profile_timestamp_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "spark_version": spark.version,
            "total_rows": total_rows,
            "total_columns": len(df.columns),
            "schema": [
                {
                    "name": field.name,
                    "type": field.dataType.simpleString(),
                    "nullable": field.nullable,
                }
                for field in df.schema.fields
            ],
            "column_quality": column_quality,
            "asset_identifier_profile": identifier_profile,
            "duplicate_identifier_details": (
                duplicate_identifier_details
            ),
            "primary_key_validation": (
                primary_key_validation
            ),
            "asset_status_distribution": (
                status_distribution
            ),
            "numeric_type_validation": (
                numeric_validation
            ),
            "date_timestamp_validation": (
                date_validation
            ),
            "full_row_duplicate_analysis": (
                full_row_duplicates
            ),
            "asset_value_quality_analysis": (
                asset_value_quality
            ),
            "coordinate_quality_analysis": (
                coordinate_quality
            ),
            "date_relationship_quality_analysis": (
                date_relationship_quality
            ),
            "placeholder_record_analysis": (
                placeholder_quality
            ),
        }

        # ----------------------------------------------------
        # Save profiling report
        # ----------------------------------------------------

        OUTPUT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        with OUTPUT_FILE.open(
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
        # Final execution summary
        # ----------------------------------------------------

        print("\n" + "=" * 65)
        print("FINAL SOURCE PROFILING SUMMARY")
        print("=" * 65)

        print(f"Total records: {total_rows:,}")
        print(f"Total columns: {len(df.columns)}")

        print(
            "OBJ_SQLIDENTITY candidate key: "
            f"{primary_key_validation['is_candidate_primary_key']}"
        )

        print(
            "Exact duplicate excess rows: "
            f"{full_row_duplicates['exact_duplicate_excess_rows']}"
        )

        print(
            "Negative asset values: "
            f"{asset_value_quality['negative_value_records']}"
        )

        print(
            "Possible coordinate outliers: "
            f"{coordinate_quality['possible_statistical_outliers']}"
        )

        print(
            "Withdrawal before commission: "
            f"{date_relationship_quality['withdrawal_before_commission_count']}"
        )

        print("\n" + "=" * 65)
        print("PROFILING COMPLETED SUCCESSFULLY")
        print("=" * 65)

        print("\nUpdated JSON report:")
        print(OUTPUT_FILE)

        df.unpersist()

    finally:
        spark.stop()


# ============================================================
# 18. MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    run_profiling()
