# Databricks notebook source

# ============================================================
# STEP 9.12S — CORRECTED GOLD WORKFLOW READINESS
# AssetFusion360 | Databricks Free Edition
#
# FIX:
# Gold tables do not all begin with "gold_".
# Include dim_, fact_, bridge_, and gold_ prefixes.
#
# READ-ONLY — NO DELTA TABLES MODIFIED
# ============================================================

from pyspark.sql import types as T

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

EXPECTED_GOLD_TABLES = 17

# ------------------------------------------------------------
# 1. Discover Gold tables using actual naming conventions
# ------------------------------------------------------------

GOLD_PREFIXES = (
    "dim_",
    "fact_",
    "bridge_",
    "gold_"
)

def get_gold_tables(schema_name):

    return sorted([
        table.name
        for table in spark.catalog.listTables(schema_name)
        if (
            table.name.lower().startswith(GOLD_PREFIXES)
            and not table.isTemporary
        )
    ])

production_tables = get_gold_tables(PRODUCTION)
staging_tables = get_gold_tables(STAGING)

print("=" * 70)
print("STEP 9.12S — CORRECTED GOLD WORKFLOW READINESS")
print("=" * 70)

print("\nProduction Gold tables:", len(production_tables))
print("Staging Gold tables:", len(staging_tables))

print("\nPRODUCTION GOLD TABLE INVENTORY")
for table in production_tables:
    print(" ", table)

print("\nSTAGING GOLD TABLE INVENTORY")
for table in staging_tables:
    print(" ", table)

# ------------------------------------------------------------
# 2. Check Gold table inventory
# ------------------------------------------------------------

assert len(production_tables) == EXPECTED_GOLD_TABLES, (
    f"Expected 17 production Gold tables, "
    f"found {len(production_tables)}"
)

assert len(staging_tables) == EXPECTED_GOLD_TABLES, (
    f"Expected 17 staging Gold tables, "
    f"found {len(staging_tables)}"
)

assert production_tables == staging_tables, (
    "Production and staging inventories differ."
)

print("\nGOLD TABLE INVENTORY: PASS")

# ------------------------------------------------------------
# 3. Validate all 17 production/staging table pairs
# ------------------------------------------------------------

results = []

for table_name in production_tables:

    prod_name = f"{PRODUCTION}.{table_name}"
    stage_name = f"{STAGING}.{table_name}"

    result = {
        "table": table_name,
        "production_rows": None,
        "staging_rows": None,
        "columns": None,
        "production_only": None,
        "staging_only": None,
        "status": "FAIL",
        "error": None
    }

    print(f"\nValidating: {table_name}")

    try:

        prod = spark.table(prod_name)
        stage = spark.table(stage_name)

        # Validate row counts

        prod_rows = prod.count()
        stage_rows = stage.count()

        result["production_rows"] = prod_rows
        result["staging_rows"] = stage_rows

        assert prod_rows == stage_rows, (
            f"Row mismatch: {prod_rows} vs {stage_rows}"
        )

        # Validate schemas

        assert prod.schema.names == stage.schema.names, (
            "Column names or order mismatch"
        )

        assert len(prod.columns) == len(stage.columns)

        for name in prod.columns:
            assert (
                prod.schema[name].dataType
                == stage.schema[name].dataType
            ), f"Data type mismatch: {name}"

        result["columns"] = len(prod.columns)

        # Validate Delta format

        for full_name in [prod_name, stage_name]:

            metadata = spark.sql(
                f"DESCRIBE DETAIL {full_name}"
            ).first()

            assert metadata["format"].lower() == "delta"

        # Full bidirectional data comparison

        production_only = (
            prod.exceptAll(stage).count()
        )

        staging_only = (
            stage.exceptAll(prod).count()
        )

        result["production_only"] = production_only
        result["staging_only"] = staging_only

        assert production_only == 0
        assert staging_only == 0

        result["status"] = "PASS"

        print(
            f"PASS | {prod_rows:,} rows "
            f"| {len(prod.columns)} columns"
        )

    except Exception as error:

        result["error"] = str(error)

        print("FAIL:", str(error))

    results.append(result)

# ------------------------------------------------------------
# 4. Display consolidated validation results
# ------------------------------------------------------------

REPORT_SCHEMA = T.StructType([
    T.StructField("table", T.StringType(), False),
    T.StructField("production_rows", T.LongType(), True),
    T.StructField("staging_rows", T.LongType(), True),
    T.StructField("columns", T.IntegerType(), True),
    T.StructField("production_only", T.LongType(), True),
    T.StructField("staging_only", T.LongType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("error", T.StringType(), True)
])

report_df = spark.createDataFrame(
    results,
    schema=REPORT_SCHEMA
)

print("\nGOLD WORKFLOW VALIDATION REPORT")

display(report_df)

# ------------------------------------------------------------
# 5. Final validation summary
# ------------------------------------------------------------

passed = sum(
    result["status"] == "PASS"
    for result in results
)

failed = len(results) - passed

production_total = sum(
    result["production_rows"] or 0
    for result in results
)

staging_total = sum(
    result["staging_rows"] or 0
    for result in results
)

print("\n" + "=" * 70)
print("GOLD WORKFLOW READINESS SUMMARY")
print("=" * 70)

print("Production Gold tables:", len(production_tables))
print("Staging Gold tables:", len(staging_tables))

print("Tables passed:", passed)
print("Tables failed:", failed)

print("Production total records:", production_total)
print("Staging total records:", staging_total)

assert passed == EXPECTED_GOLD_TABLES, (
    f"{failed} Gold table(s) failed validation."
)

assert production_total == staging_total

print("\nSTEP 9.12S: PASS")
print("ALL 17 GOLD TABLE PAIRS VALIDATED")
print("NO EXISTING DELTA TABLES MODIFIED")
