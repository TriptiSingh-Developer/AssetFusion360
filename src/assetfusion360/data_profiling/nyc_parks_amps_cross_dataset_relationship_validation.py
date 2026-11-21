
"""
AssetFusion360 Capstone Project

STEP 4.6F
NYC Parks AMPS - Cross-Dataset Relationship Validation

Relationships:
    1. Assets -> Work Orders
       OBJ_CODE -> evt_object

    2. Work Orders -> Labor
       evt_code -> boo_event

    3. Transactions -> Transaction Details
       tra_code -> trl_trans

    4. Work Orders -> Transaction Details
       evt_code -> trl_event

Validation:
    - Parent and child record references
    - Missing child references
    - Matched child records
    - Unmatched child records
    - Match coverage percentages
    - Distinct matched and unmatched references
    - Unmatched reference examples

Important:
    - Parent business keys are deduplicated for validation.
    - Duplicate parent keys cannot inflate join counts.
    - NULL and blank child references are counted separately.
    - All CSV source columns remain strings.
    - The original CSV files are never modified.
    - Results are saved after each relationship.
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

SOURCE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR
    / "nyc_parks_amps_cross_dataset_relationship_report.json"
)


DATASETS = {
    "assets": {
        "path": SOURCE_ROOT / "AMPS_Assets.csv",
        "files": 1,
        "columns": 29,
    },
    "workorders": {
        "path": SOURCE_ROOT / "workorders_batches",
        "files": 447,
        "columns": 55,
    },
    "labor": {
        "path": SOURCE_ROOT / "labor_batches",
        "files": 647,
        "columns": 18,
    },
    "transactions": {
        "path": SOURCE_ROOT / "transactions_batches",
        "files": 152,
        "columns": 14,
    },
    "transaction_details": {
        "path": (
            SOURCE_ROOT
            / "transaction_details_batches"
        ),
        "files": 461,
        "columns": 31,
    },
}


RELATIONSHIPS = [
    {
        "name": "Assets_to_WorkOrders",
        "parent_dataset": "assets",
        "parent_column": "OBJ_CODE",
        "child_dataset": "workorders",
        "child_column": "evt_object",
    },
    {
        "name": "WorkOrders_to_Labor",
        "parent_dataset": "workorders",
        "parent_column": "evt_code",
        "child_dataset": "labor",
        "child_column": "boo_event",
    },
    {
        "name": "Transactions_to_TransactionDetails",
        "parent_dataset": "transactions",
        "parent_column": "tra_code",
        "child_dataset": "transaction_details",
        "child_column": "trl_trans",
    },
    {
        "name": "WorkOrders_to_TransactionDetails",
        "parent_dataset": "workorders",
        "parent_column": "evt_code",
        "child_dataset": "transaction_details",
        "child_column": "trl_event",
    },
]


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "AssetFusion360_AMPS_Relationship_Validation"
        )
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "32")
        .config(
            "spark.sql.csv.parser.columnPruning.enabled",
            "false"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 3. DISCOVER DATASET FILES
# ============================================================

def get_dataset_files(dataset_name):

    config = DATASETS[dataset_name]

    source_path = config["path"]

    if source_path.is_file():
        files = [source_path]

    elif source_path.is_dir():
        files = sorted(
            source_path.glob("*.csv"),
            key=lambda path: path.name.lower()
        )

    else:
        raise FileNotFoundError(
            f"Dataset source not found: {source_path}"
        )

    if len(files) != config["files"]:
        raise ValueError(
            f"{dataset_name}: Expected "
            f"{config['files']} files, "
            f"found {len(files)}."
        )

    return files


# ============================================================
# 4. READ AND VALIDATE SOURCE HEADER
# ============================================================

def read_header(file_path):

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
            f"Missing header: {file_path}"
        )

    return header


def get_dataset_schema(dataset_name, files):

    header = read_header(files[0])

    expected_columns = DATASETS[
        dataset_name
    ]["columns"]

    if len(header) != expected_columns:
        raise ValueError(
            f"{dataset_name}: Expected "
            f"{expected_columns} columns, "
            f"found {len(header)}."
        )

    if (
        len(header) != len(set(header))
        or any(not name.strip() for name in header)
    ):
        raise ValueError(
            f"Invalid header in {dataset_name}"
        )

    return StructType([
        StructField(
            name,
            StringType(),
            True
        )
        for name in header
    ])


# ============================================================
# 5. LOAD A DATASET USING VERIFIED CSV SETTINGS
# ============================================================

def load_dataset(spark, dataset_name):

    files = get_dataset_files(dataset_name)

    schema = get_dataset_schema(
        dataset_name,
        files
    )

    file_paths = [
        str(path.resolve())
        for path in files
    ]

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

    return df


# ============================================================
# 6. NORMALIZE KEYS FOR MATCHING
# ============================================================

def normalized_key(column_name):
    """
    Normalize surrounding whitespace.

    Blank strings become NULL.

    Exact matching is case-sensitive.
    Other transformations are deliberately avoided.
    """

    value = F.trim(
        F.col(column_name)
    )

    return F.when(
        value.isNull() | (value == ""),
        F.lit(None).cast("string")
    ).otherwise(value)


# ============================================================
# 7. VALIDATE ONE PARENT-CHILD RELATIONSHIP
# ============================================================

def validate_relationship(spark, relationship):

    name = relationship["name"]

    parent_dataset = relationship["parent_dataset"]
    child_dataset = relationship["child_dataset"]

    parent_column = relationship["parent_column"]
    child_column = relationship["child_column"]

    print("\n" + "=" * 70)
    print(f"VALIDATING: {name}")
    print("=" * 70)

    print(
        f"Parent: {parent_dataset}.{parent_column}"
    )

    print(
        f"Child:  {child_dataset}.{child_column}"
    )

    # --------------------------------------------------------
    # Load source datasets
    # --------------------------------------------------------

    parent_df = load_dataset(
        spark,
        parent_dataset
    )

    child_df = load_dataset(
        spark,
        child_dataset
    )

    if parent_column not in parent_df.columns:
        raise ValueError(
            f"Parent column missing: {parent_column}"
        )

    if child_column not in child_df.columns:
        raise ValueError(
            f"Child column missing: {child_column}"
        )

    # --------------------------------------------------------
    # Parent: distinct nonblank reference keys
    #
    # This avoids duplicated parent business keys
    # multiplying child records during the join.
    # --------------------------------------------------------

    parent_keys = (
        parent_df
        .select(
            normalized_key(
                parent_column
            ).alias("__parent_key")
        )
        .filter(
            F.col("__parent_key").isNotNull()
        )
        .distinct()
    )

    # --------------------------------------------------------
    # Child: count rows per normalized reference
    #
    # Grouping before joining reduces unnecessary
    # repeated join processing for one-to-many links.
    # --------------------------------------------------------

    child_key_counts = (
        child_df
        .select(
            normalized_key(
                child_column
            ).alias("__child_key")
        )
        .groupBy("__child_key")
        .agg(
            F.count(F.lit(1)).alias("__child_rows")
        )
    )

    # --------------------------------------------------------
    # Join distinct parent keys to child reference groups
    # --------------------------------------------------------

    joined = (
        child_key_counts
        .join(
            parent_keys,
            F.col("__child_key")
            == F.col("__parent_key"),
            "left"
        )
    )

    child_missing = (
        F.col("__child_key").isNull()
    )

    parent_found = (
        F.col("__parent_key").isNotNull()
    )

    matched = (
        (~child_missing) & parent_found
    )

    unmatched = (
        (~child_missing) & (~parent_found)
    )

    # --------------------------------------------------------
    # Aggregate record-level and distinct-key coverage
    # --------------------------------------------------------

    summary = joined.agg(
        F.sum("__child_rows").alias(
            "total_child_rows"
        ),
        F.sum(
            F.when(
                child_missing,
                F.col("__child_rows")
            ).otherwise(0)
        ).alias("missing_reference_rows"),
        F.sum(
            F.when(
                matched,
                F.col("__child_rows")
            ).otherwise(0)
        ).alias("matched_child_rows"),
        F.sum(
            F.when(
                unmatched,
                F.col("__child_rows")
            ).otherwise(0)
        ).alias("unmatched_child_rows"),
        F.sum(
            F.when(matched, 1).otherwise(0)
        ).alias("distinct_matched_references"),
        F.sum(
            F.when(unmatched, 1).otherwise(0)
        ).alias("distinct_unmatched_references"),
    ).first()

    total_rows = int(
        summary["total_child_rows"] or 0
    )

    missing_rows = int(
        summary["missing_reference_rows"] or 0
    )

    matched_rows = int(
        summary["matched_child_rows"] or 0
    )

    unmatched_rows = int(
        summary["unmatched_child_rows"] or 0
    )

    populated_rows = total_rows - missing_rows

    if (
        matched_rows + unmatched_rows + missing_rows
        != total_rows
    ):
        raise ValueError(
            f"Relationship count reconciliation failed: {name}"
        )

    # --------------------------------------------------------
    # Retrieve a few unmatched reference examples
    # --------------------------------------------------------

    examples = (
        joined
        .filter(unmatched)
        .select(
            F.col("__child_key").alias("reference"),
            F.col("__child_rows").alias("records")
        )
        .orderBy(
            F.desc("records"),
            F.asc("reference")
        )
        .limit(10)
        .collect()
    )

    unmatched_samples = [
        {
            "reference": row["reference"],
            "child_records": int(row["records"]),
        }
        for row in examples
    ]

    result = {
        "relationship_name": name,
        "parent_dataset": parent_dataset,
        "parent_column": parent_column,
        "child_dataset": child_dataset,
        "child_column": child_column,
        "matching_method": (
            "Trimmed, case-sensitive, exact string matching"
        ),
        "parent_keys_deduplicated": True,
        "total_child_rows": total_rows,
        "missing_reference_rows": missing_rows,
        "populated_reference_rows": populated_rows,
        "matched_child_rows": matched_rows,
        "unmatched_child_rows": unmatched_rows,
        "match_percentage_of_populated": (
            round(
                100 * matched_rows / populated_rows,
                2
            )
            if populated_rows else 0.0
        ),
        "match_percentage_of_all_rows": (
            round(
                100 * matched_rows / total_rows,
                2
            )
            if total_rows else 0.0
        ),
        "distinct_matched_references": int(
            summary["distinct_matched_references"] or 0
        ),
        "distinct_unmatched_references": int(
            summary["distinct_unmatched_references"] or 0
        ),
        "unmatched_reference_samples": unmatched_samples,
        "all_populated_references_match": (
            unmatched_rows == 0
        ),
        "all_child_rows_have_valid_references": (
            missing_rows == 0 and unmatched_rows == 0
        ),
        "notes": [
            "No rows were modified or removed.",
            "Blank child references are counted separately.",
            "Parent keys were deduplicated before joining.",
            "An unmatched reference is a review finding.",
            "Business semantics require separate validation.",
        ],
    }

    print("\nRELATIONSHIP RESULTS")
    print("-" * 55)

    print(f"Total child records: {total_rows:,}")
    print(f"Missing references: {missing_rows:,}")
    print(f"Matched records: {matched_rows:,}")
    print(f"Unmatched records: {unmatched_rows:,}")

    print(
        "Match rate (populated references): "
        f"{result['match_percentage_of_populated']}%"
    )

    print(
        "Match rate (all records): "
        f"{result['match_percentage_of_all_rows']}%"
    )

    print(
        "Distinct unmatched references: "
        f"{result['distinct_unmatched_references']:,}"
    )

    if unmatched_samples:
        print("\nTOP UNMATCHED REFERENCE EXAMPLES:")

        for item in unmatched_samples:
            print(
                f"  {item['reference']} "
                f"- {item['child_records']:,} records"
            )

    return result


# ============================================================
# 8. SAVE PROGRESS AFTER EACH RELATIONSHIP
# ============================================================

def save_report(report):

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    report["last_updated_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    # Write a temporary JSON file and then replace
    # the report to avoid partially written JSON.
    temporary_file = REPORT_FILE.with_suffix(
        ".json.tmp"
    )

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
# 9. MAIN EXECUTION
# ============================================================

def run_validation():

    print("\n" + "=" * 70)
    print("ASSETFUSION360 - CROSS-DATASET RELATIONSHIPS")
    print("=" * 70)

    report = {
        "project": "AssetFusion360",
        "profiling_step": "4.6F",
        "dataset_collection": "NYC Parks AMPS",
        "generated_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "spark_version": None,
        "validation_status": "IN_PROGRESS",
        "relationship_results": [],
    }

    spark = create_spark_session()

    try:

        report["spark_version"] = spark.version

        for index, relationship in enumerate(
            RELATIONSHIPS,
            start=1
        ):

            print(
                f"\nRelationship {index} "
                f"of {len(RELATIONSHIPS)}"
            )

            result = validate_relationship(
                spark,
                relationship
            )

            report["relationship_results"].append(
                result
            )

            # Preserve completed findings immediately.
            save_report(report)

        report["validation_status"] = "COMPLETED"

        save_report(report)

        print("\n" + "=" * 70)
        print("FINAL CROSS-DATASET RELATIONSHIP SUMMARY")
        print("=" * 70)

        for result in report["relationship_results"]:

            print(
                f"\n{result['relationship_name']}"
            )

            print(
                f"  Matched: "
                f"{result['matched_child_rows']:,}"
            )

            print(
                f"  Unmatched: "
                f"{result['unmatched_child_rows']:,}"
            )

            print(
                f"  Missing references: "
                f"{result['missing_reference_rows']:,}"
            )

            print(
                "  Match percentage: "
                f"{result['match_percentage_of_populated']}%"
            )

        print("\n" + "=" * 70)
        print("RELATIONSHIP VALIDATION COMPLETED")
        print("=" * 70)

        print("\nJSON report saved to:")
        print(REPORT_FILE)

    except Exception as error:

        report["validation_status"] = "FAILED"

        report["error"] = (
            f"{type(error).__name__}: {error}"
        )

        save_report(report)

        raise

    finally:
        spark.stop()


# ============================================================
# 10. EXECUTE
# ============================================================

if __name__ == "__main__":
    run_validation()
