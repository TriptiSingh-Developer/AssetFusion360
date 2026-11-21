
"""
AssetFusion360 Capstone Project

STEP 4.7D
Inspection Date and AMPS Asset Relationship Validation

Sources:
  1. supplementary_inspection_data/Parks_Inspections.csv
  2. nyc_parks_asset_management/AMPS_Assets.csv

Checks:
  - Inspection date parsing
  - Added-date parsing
  - Unparseable date examples
  - Date ranges
  - Prop ID -> OBJ_CODE relationship
  - Exact and case-normalized reference matching
  - Duplicate AMPS business keys
  - Matched / unmatched inspection records
  - Unmatched property examples

Technology:
  Python 3.12
  PySpark 3.5.7
  No pandas

Original source CSV files remain unchanged.
No rows are removed or merged.
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

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
)

INSPECTION_FILE = (
    RAW_DIR
    / "supplementary_inspection_data"
    / "Parks_Inspections.csv"
)

ASSETS_FILE = (
    RAW_DIR
    / "nyc_parks_asset_management"
    / "AMPS_Assets.csv"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_parks_inspection_date_and_asset_relationship_report.json"
)

EXPECTED_INSPECTIONS = 153093
EXPECTED_ASSETS = 148522

# Actual inspection example:
# 2020 Jul 15 12:00:00 AM
#
# Other formats are tested because inspAddedDate
# may use a different representation.

DATE_FORMATS = [
    "yyyy MMM dd hh:mm:ss a",
    "yyyy MMM d hh:mm:ss a",
    "yyyy MMM dd h:mm:ss a",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd",
    "MM/dd/yyyy HH:mm:ss",
    "MM/dd/yyyy hh:mm:ss a",
    "M/d/yyyy h:mm:ss a",
    "MM/dd/yyyy",
    "M/d/yyyy",
    "MMM d yyyy h:mm:ss a",
    "MMM d yyyy",
]

DATE_COLUMNS = [
    "Date",
    "inspAddedDate",
]


# ============================================================
# 2. GENERAL HELPERS
# ============================================================

def quoted_column(name):
    return F.col(
        f"`{name.replace('`', '``')}`"
    )


def percentage(value, total):
    if not total:
        return 0.0
    return round(100.0 * value / total, 2)


def normalized_key(name, uppercase=False):

    value = F.trim(quoted_column(name))

    if uppercase:
        value = F.upper(value)

    return F.when(
        value.isNull() | (value == ""),
        F.lit(None).cast("string")
    ).otherwise(value)


def save_report(report):

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    report["last_updated_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    temporary = REPORT_FILE.with_suffix(
        ".json.tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8"
    ) as handle:
        json.dump(
            report,
            handle,
            indent=2,
            ensure_ascii=False
        )

    temporary.replace(REPORT_FILE)


# ============================================================
# 3. SOURCE CSV VALIDATION
# ============================================================

def get_csv_header(path):

    if not path.is_file():
        raise FileNotFoundError(
            f"Source file missing: {path}"
        )

    with path.open(
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
            f"Missing CSV header: {path}"
        )

    if (
        len(set(header)) != len(header)
        or any(not col.strip() for col in header)
    ):
        raise ValueError(
            f"Invalid column names: {path}"
        )

    return header


# ============================================================
# 4. CREATE PYSPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_Inspection_AMPS_Validation"
        )
        .master("local[2]")
        .config(
            "spark.sql.shuffle.partitions",
            "8"
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
# 5. LOAD SOURCE DATA AS STRINGS
# ============================================================

def load_csv(spark, path):

    header = get_csv_header(path)

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
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(str(path.resolve()))
    )

    return df


# ============================================================
# 6. SAFE DATE PARSING
# ============================================================

def parse_multiple_date_formats(column_name):
    """
    try_to_timestamp returns NULL for parsing failures
    rather than treating the invalid date as a valid one.

    The original source values are not modified.
    """

    raw_value = F.trim(
        quoted_column(column_name)
    )

    options = [
        F.try_to_timestamp(
            raw_value,
            F.lit(fmt)
        )
        for fmt in DATE_FORMATS
    ]

    return F.coalesce(*options)


def validate_dates(df):

    results = {}

    print("\n" + "=" * 68)
    print("INSPECTION DATE VALIDATION")
    print("=" * 68)

    for column_name in DATE_COLUMNS:

        if column_name not in df.columns:
            raise ValueError(
                f"Required date column missing: {column_name}"
            )

        print(
            f"\nChecking date column: {column_name}"
        )

        raw_value = F.trim(
            quoted_column(column_name)
        )

        populated = (
            raw_value.isNotNull()
            & (raw_value != "")
        )

        parsed_df = df.select(
            raw_value.alias("__raw_date"),
            parse_multiple_date_formats(
                column_name
            ).alias("__parsed_date")
        )

        summary = parsed_df.agg(
            F.count(F.lit(1)).alias("total"),

            F.count("__raw_date").alias(
                "non_null"
            ),

            F.sum(
                F.when(
                    F.col("__raw_date") == "",
                    1
                ).otherwise(0)
            ).alias("blank"),

            F.count("__parsed_date").alias(
                "parsed"
            ),

            F.min("__parsed_date").alias(
                "earliest"
            ),

            F.max("__parsed_date").alias(
                "latest"
            ),
        ).first()

        total = int(
            summary["total"] or 0
        )

        non_null = int(
            summary["non_null"] or 0
        )

        blank = int(
            summary["blank"] or 0
        )

        populated_count = non_null - blank

        parsed_count = int(
            summary["parsed"] or 0
        )

        invalid_count = (
            populated_count - parsed_count
        )

        # Display representative raw date strings.
        samples = (
            parsed_df
            .filter(
                F.col("__raw_date").isNotNull()
                & (F.col("__raw_date") != "")
            )
            .select("__raw_date")
            .distinct()
            .limit(8)
            .collect()
        )

        raw_samples = [
            row["__raw_date"]
            for row in samples
        ]

        invalid_samples = []

        if invalid_count:

            invalid_rows = (
                parsed_df
                .filter(
                    F.col("__raw_date").isNotNull()
                    & (F.col("__raw_date") != "")
                    & F.col("__parsed_date").isNull()
                )
                .groupBy("__raw_date")
                .count()
                .orderBy(
                    F.desc("count"),
                    F.asc("__raw_date")
                )
                .limit(10)
                .collect()
            )

            invalid_samples = [
                {
                    "raw_value": row["__raw_date"],
                    "records": int(row["count"]),
                }
                for row in invalid_rows
            ]

        result = {
            "column_name": column_name,
            "total_rows": total,
            "populated_records": populated_count,
            "missing_or_blank_records": (
                total - populated_count
            ),
            "parseable_records": parsed_count,
            "unparseable_records": invalid_count,
            "parseable_percentage": percentage(
                parsed_count,
                populated_count
            ),
            "earliest_parsed_date": (
                str(summary["earliest"])
                if summary["earliest"] is not None
                else None
            ),
            "latest_parsed_date": (
                str(summary["latest"])
                if summary["latest"] is not None
                else None
            ),
            "raw_date_samples": raw_samples,
            "unparseable_examples": invalid_samples,
            "tested_formats": DATE_FORMATS,
        }

        results[column_name] = result

        print(
            f"Populated: {populated_count:,}"
        )
        print(
            f"Parsed: {parsed_count:,}"
        )
        print(
            f"Unparseable: {invalid_count:,}"
        )
        print(
            "Parsing coverage: "
            f"{result['parseable_percentage']}%"
        )
        print(
            f"Earliest: {result['earliest_parsed_date']}"
        )
        print(
            f"Latest: {result['latest_parsed_date']}"
        )

        print("\nRaw date samples:")
        for value in raw_samples:
            print(f"  {value}")

        if invalid_samples:
            print("\nUnparseable examples:")
            for item in invalid_samples:
                print(
                    f"  {item['raw_value']}: "
                    f"{item['records']:,} records"
                )

    return results


# ============================================================
# 7. VALIDATE AMPS ASSET BUSINESS KEYS
# ============================================================

def validate_asset_keys(assets_df):

    parent = assets_df.select(
        normalized_key(
            "OBJ_CODE"
        ).alias("__asset_key")
    )

    frequency = (
        parent
        .filter(
            F.col("__asset_key").isNotNull()
        )
        .groupBy("__asset_key")
        .count()
    )

    duplicate_rows = (
        frequency
        .filter(F.col("count") > 1)
        .orderBy(
            F.desc("count"),
            F.asc("__asset_key")
        )
        .limit(20)
        .collect()
    )

    duplicate_examples = [
        {
            "asset_code": row["__asset_key"],
            "record_count": int(row["count"]),
        }
        for row in duplicate_rows
    ]

    summary = parent.agg(
        F.count(F.lit(1)).alias("total"),
        F.count("__asset_key").alias("populated"),
        F.countDistinct("__asset_key").alias("distinct"),
    ).first()

    total = int(summary["total"])
    populated = int(summary["populated"])
    distinct = int(summary["distinct"])

    return {
        "total_asset_records": total,
        "populated_asset_codes": populated,
        "missing_asset_codes": total - populated,
        "distinct_asset_codes": distinct,
        "duplicate_excess_rows": populated - distinct,
        "duplicate_asset_code_examples": duplicate_examples,
    }


# ============================================================
# 8. MATCH INSPECTIONS TO AMPS ASSETS
# ============================================================

def calculate_match(
    inspections_df,
    assets_df,
    uppercase=False
):

    # Deduplicate parent business keys.
    # This prevents duplicate OBJ_CODE values from
    # multiplying inspection rows during the join.

    parent_keys = (
        assets_df
        .select(
            normalized_key(
                "OBJ_CODE",
                uppercase=uppercase
            ).alias("__parent_key")
        )
        .filter(
            F.col("__parent_key").isNotNull()
        )
        .distinct()
    )

    inspection_key_counts = (
        inspections_df
        .select(
            normalized_key(
                "Prop ID",
                uppercase=uppercase
            ).alias("__inspection_key")
        )
        .groupBy("__inspection_key")
        .agg(
            F.count(F.lit(1)).alias("__records")
        )
    )

    joined = inspection_key_counts.join(
        parent_keys,
        F.col("__inspection_key")
        == F.col("__parent_key"),
        "left"
    )

    missing = F.col("__inspection_key").isNull()

    matched = (
        (~missing)
        & F.col("__parent_key").isNotNull()
    )

    unmatched = (
        (~missing)
        & F.col("__parent_key").isNull()
    )

    summary = joined.agg(
        F.sum("__records").alias("total"),

        F.sum(
            F.when(
                missing,
                F.col("__records")
            ).otherwise(0)
        ).alias("missing"),

        F.sum(
            F.when(
                matched,
                F.col("__records")
            ).otherwise(0)
        ).alias("matched"),

        F.sum(
            F.when(
                unmatched,
                F.col("__records")
            ).otherwise(0)
        ).alias("unmatched"),

        F.sum(
            F.when(matched, 1).otherwise(0)
        ).alias("matched_distinct"),

        F.sum(
            F.when(unmatched, 1).otherwise(0)
        ).alias("unmatched_distinct"),
    ).first()

    total = int(summary["total"] or 0)
    missing_count = int(summary["missing"] or 0)
    matched_count = int(summary["matched"] or 0)
    unmatched_count = int(summary["unmatched"] or 0)

    populated = total - missing_count

    if (
        missing_count
        + matched_count
        + unmatched_count
        != total
    ):
        raise RuntimeError(
            "Inspection relationship counts "
            "did not reconcile."
        )

    sample_rows = (
        joined
        .filter(unmatched)
        .select(
            "__inspection_key",
            "__records"
        )
        .orderBy(
            F.desc("__records"),
            F.asc("__inspection_key")
        )
        .limit(15)
        .collect()
    )

    samples = [
        {
            "prop_id": row["__inspection_key"],
            "inspection_records": int(row["__records"]),
        }
        for row in sample_rows
    ]

    return {
        "matching_method": (
            "Trim and uppercase"
            if uppercase
            else "Trim only; case-sensitive"
        ),
        "total_inspection_records": total,
        "missing_property_references": missing_count,
        "populated_property_references": populated,
        "matched_inspection_records": matched_count,
        "unmatched_inspection_records": unmatched_count,
        "match_percentage_of_populated": percentage(
            matched_count,
            populated
        ),
        "match_percentage_of_all_records": percentage(
            matched_count,
            total
        ),
        "distinct_matched_property_codes": int(
            summary["matched_distinct"] or 0
        ),
        "distinct_unmatched_property_codes": int(
            summary["unmatched_distinct"] or 0
        ),
        "unmatched_property_examples": samples,
    }


# ============================================================
# 9. MAIN EXECUTION
# ============================================================

def run_validation():

    print("\n" + "=" * 68)
    print("ASSETFUSION360 - STEP 4.7D")
    print("INSPECTION DATE AND AMPS RELATIONSHIP VALIDATION")
    print("=" * 68)

    spark = create_spark()

    try:

        inspections_df = load_csv(
            spark,
            INSPECTION_FILE
        )

        assets_df = load_csv(
            spark,
            ASSETS_FILE
        )

        required_inspection_columns = [
            "Inspection ID",
            "Prop ID",
            "Date",
            "inspAddedDate",
        ]

        for name in required_inspection_columns:
            if name not in inspections_df.columns:
                raise ValueError(
                    f"Missing inspection column: {name}"
                )

        if "OBJ_CODE" not in assets_df.columns:
            raise ValueError(
                "Missing AMPS column: OBJ_CODE"
            )

        # ----------------------------------------------------
        # Source count validation
        # ----------------------------------------------------

        inspection_count = inspections_df.count()
        asset_count = assets_df.count()

        print("\nSOURCE COUNTS")
        print("-" * 55)

        print(
            f"Inspection records: {inspection_count:,}"
        )
        print(
            f"AMPS asset records: {asset_count:,}"
        )

        if inspection_count != EXPECTED_INSPECTIONS:
            raise ValueError(
                "Inspection count differs from "
                f"previous profiling: {inspection_count}"
            )

        if asset_count != EXPECTED_ASSETS:
            raise ValueError(
                "Asset count differs from "
                f"previous profiling: {asset_count}"
            )

        # ----------------------------------------------------
        # Date validation
        # ----------------------------------------------------

        date_results = validate_dates(
            inspections_df
        )

        # ----------------------------------------------------
        # Parent identifier quality
        # ----------------------------------------------------

        asset_key_results = validate_asset_keys(
            assets_df
        )

        print("\n" + "=" * 68)
        print("AMPS ASSET KEY QUALITY")
        print("=" * 68)

        print(
            json.dumps(
                asset_key_results,
                indent=2
            )
        )

        # ----------------------------------------------------
        # Exact matching
        # ----------------------------------------------------

        print("\n" + "=" * 68)
        print("INSPECTION PROP ID -> AMPS OBJ_CODE")
        print("EXACT MATCHING")
        print("=" * 68)

        exact_match = calculate_match(
            inspections_df,
            assets_df,
            uppercase=False
        )

        print(
            json.dumps(
                exact_match,
                indent=2
            )
        )

        # ----------------------------------------------------
        # Case-insensitive matching
        # ----------------------------------------------------

        print("\n" + "=" * 68)
        print("CASE-NORMALIZED MATCHING")
        print("=" * 68)

        case_match = calculate_match(
            inspections_df,
            assets_df,
            uppercase=True
        )

        print(
            json.dumps(
                case_match,
                indent=2
            )
        )

        # ----------------------------------------------------
        # Build permanent report
        # ----------------------------------------------------

        report = {
            "project": "AssetFusion360",
            "validation_step": "4.7D",
            "status": "COMPLETED",
            "spark_version": spark.version,
            "source_files": {
                "inspections": str(
                    INSPECTION_FILE.relative_to(
                        PROJECT_ROOT
                    )
                ),
                "assets": str(
                    ASSETS_FILE.relative_to(
                        PROJECT_ROOT
                    )
                ),
            },
            "source_counts": {
                "inspection_records": inspection_count,
                "asset_records": asset_count,
            },
            "inspection_date_validation": date_results,
            "asset_business_key_validation": (
                asset_key_results
            ),
            "inspection_asset_relationship": {
                "parent_column": "OBJ_CODE",
                "child_column": "Prop ID",
                "exact_match": exact_match,
                "case_normalized_match": case_match,
            },
            "integration_decision": (
                "PENDING BUSINESS VALIDATION"
            ),
            "notes": [
                "Both source CSV files remain unchanged.",
                "Source identifiers were not fabricated.",
                "Duplicate parent asset codes were deduplicated.",
                "Matching is based on string identifiers.",
                "Matching does not prove equal entity grain.",
                "Property identifiers may represent park "
                "properties rather than individual assets.",
                "No joins were persisted.",
                "No missing dates were imputed.",
                "Date formats are tested using safe parsing.",
            ],
        }

        save_report(report)

        # ----------------------------------------------------
        # Final summary
        # ----------------------------------------------------

        print("\n" + "=" * 68)
        print("STEP 4.7D FINAL VALIDATION SUMMARY")
        print("=" * 68)

        print(
            f"Inspection records: {inspection_count:,}"
        )

        print(
            f"Asset records: {asset_count:,}"
        )

        for column_name, result in date_results.items():

            print(
                f"\n{column_name} date parsing:"
            )

            print(
                f"  Parseable: "
                f"{result['parseable_records']:,}"
            )

            print(
                f"  Unparseable: "
                f"{result['unparseable_records']:,}"
            )

            print(
                f"  Coverage: "
                f"{result['parseable_percentage']}%"
            )

        print("\nExact identifier matching:")

        print(
            "  Matched inspection records: "
            f"{exact_match['matched_inspection_records']:,}"
        )

        print(
            "  Unmatched inspection records: "
            f"{exact_match['unmatched_inspection_records']:,}"
        )

        print(
            "  Match percentage: "
            f"{exact_match['match_percentage_of_populated']}%"
        )

        print("\nCase-normalized matching:")

        print(
            "  Matched inspection records: "
            f"{case_match['matched_inspection_records']:,}"
        )

        print(
            "  Unmatched inspection records: "
            f"{case_match['unmatched_inspection_records']:,}"
        )

        print(
            "  Match percentage: "
            f"{case_match['match_percentage_of_populated']}%"
        )

        print("\n" + "=" * 68)
        print("STEP 4.7D COMPLETED SUCCESSFULLY")
        print("=" * 68)

        print("\nReport saved to:")
        print(REPORT_FILE)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_validation()
