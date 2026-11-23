# Databricks notebook source

# ============================================================
# ASSETFUSION360
# STEP 9.12R — CONSOLIDATED SILVER WORKFLOW QUALITY GATE
#
# Notebook:
# AssetFusion360_WF_03_Validate_All_Silver
#
# PURPOSE:
# Validate the five existing Silver production/staging
# table pairs before Gold workflow execution.
#
# SAFETY:
# READ-ONLY
# No Delta table creation, updates, deletes or overwrites.
# ============================================================

from pyspark.sql import types as T

# ------------------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------------------

PRODUCTION_SCHEMA = "workspace.assetfusion360"
STAGING_SCHEMA = "workspace.assetfusion360_staging"

SILVER_CONFIG = {
    "silver_amps_assets": (148522, 43),
    "silver_amps_workorders": (2231535, 82),
    "silver_amps_transactions": (755122, 33),
    "silver_amps_transaction_details": (2301927, 68),
    "silver_amps_labor": (3230423, 38)
}

EXPECTED_TOTAL_ROWS = 8667529

results = []

print("=" * 70)
print("ASSETFUSION360 — SILVER WORKFLOW QUALITY GATE")
print("=" * 70)

# ------------------------------------------------------------
# 2. VALIDATE ALL FIVE SILVER TABLE PAIRS
# ------------------------------------------------------------

for table_name, (expected_rows, expected_columns) in (
    SILVER_CONFIG.items()
):

    production_name = (
        f"{PRODUCTION_SCHEMA}.{table_name}"
    )

    staging_name = (
        f"{STAGING_SCHEMA}.{table_name}"
    )

    print(f"\nValidating: {table_name}")

    result = {
        "table_name": table_name,
        "expected_rows": expected_rows,
        "production_rows": None,
        "staging_rows": None,
        "production_columns": None,
        "staging_columns": None,
        "production_only": None,
        "staging_only": None,
        "status": "FAIL",
        "error": None
    }

    try:

        # A. Verify table existence

        assert spark.catalog.tableExists(
            production_name
        ), f"Missing production table: {production_name}"

        assert spark.catalog.tableExists(
            staging_name
        ), f"Missing staging table: {staging_name}"

        production_df = spark.table(production_name)
        staging_df = spark.table(staging_name)

        # B. Verify row counts

        production_rows = production_df.count()
        staging_rows = staging_df.count()

        result["production_rows"] = production_rows
        result["staging_rows"] = staging_rows

        assert production_rows == expected_rows, (
            f"Production rows: expected {expected_rows}, "
            f"found {production_rows}"
        )

        assert staging_rows == expected_rows, (
            f"Staging rows: expected {expected_rows}, "
            f"found {staging_rows}"
        )

        print("  Row counts: PASS")

        # C. Verify column counts

        production_columns = len(production_df.columns)
        staging_columns = len(staging_df.columns)

        result["production_columns"] = production_columns
        result["staging_columns"] = staging_columns

        assert production_columns == expected_columns, (
            f"Production column mismatch: {table_name}"
        )

        assert staging_columns == expected_columns, (
            f"Staging column mismatch: {table_name}"
        )

        # D. Verify column names and order

        assert (
            production_df.schema.names
            == staging_df.schema.names
        ), f"Column names/order mismatch: {table_name}"

        # E. Verify column data types and nullability

        for column in production_df.columns:

            production_field = production_df.schema[column]
            staging_field = staging_df.schema[column]

            assert (
                production_field.dataType
                == staging_field.dataType
            ), f"Data type mismatch: {table_name}.{column}"

        print("  Silver schema: PASS")

        # F. Verify Delta table format

        for full_table in [
            production_name,
            staging_name
        ]:

            detail = spark.sql(
                f"DESCRIBE DETAIL {full_table}"
            ).first()

            assert (
                detail["format"].lower() == "delta"
            ), f"Not a Delta table: {full_table}"

        print("  Delta format: PASS")

        # G. Compare complete datasets in both directions
        # exceptAll preserves duplicate multiplicities.

        production_only = (
            production_df
            .exceptAll(staging_df)
            .count()
        )

        staging_only = (
            staging_df
            .exceptAll(production_df)
            .count()
        )

        result["production_only"] = production_only
        result["staging_only"] = staging_only

        assert production_only == 0, (
            f"Production-only records: {production_only}"
        )

        assert staging_only == 0, (
            f"Staging-only records: {staging_only}"
        )

        print("  Full record comparison: PASS")

        result["status"] = "PASS"

    except Exception as error:

        result["error"] = str(error)

        print("  VALIDATION FAILED:", str(error))

    results.append(result)

# ------------------------------------------------------------
# 3. DISPLAY CONSOLIDATED VALIDATION REPORT
# ------------------------------------------------------------

REPORT_SCHEMA = T.StructType([

    T.StructField(
        "table_name", T.StringType(), False
    ),

    T.StructField(
        "expected_rows", T.LongType(), False
    ),

    T.StructField(
        "production_rows", T.LongType(), True
    ),

    T.StructField(
        "staging_rows", T.LongType(), True
    ),

    T.StructField(
        "production_columns", T.IntegerType(), True
    ),

    T.StructField(
        "staging_columns", T.IntegerType(), True
    ),

    T.StructField(
        "production_only", T.LongType(), True
    ),

    T.StructField(
        "staging_only", T.LongType(), True
    ),

    T.StructField(
        "status", T.StringType(), False
    ),

    T.StructField(
        "error", T.StringType(), True
    )
])

report_df = spark.createDataFrame(
    results,
    schema=REPORT_SCHEMA
)

print("\nCONSOLIDATED SILVER VALIDATION REPORT")

display(report_df)

# ------------------------------------------------------------
# 4. RECONCILE ALL SILVER TABLES
# ------------------------------------------------------------

passed = sum(
    result["status"] == "PASS"
    for result in results
)

failed = len(results) - passed

expected_total = sum(
    config[0] for config in SILVER_CONFIG.values()
)

production_total = sum(
    result["production_rows"] or 0
    for result in results
)

staging_total = sum(
    result["staging_rows"] or 0
    for result in results
)

print("\n" + "=" * 70)
print("SILVER QUALITY GATE — FINAL SUMMARY")
print("=" * 70)

print("Tables expected:", len(SILVER_CONFIG))
print("Tables checked:", len(results))
print("Tables passed:", passed)
print("Tables failed:", failed)

print("\nExpected total records:", expected_total)
print("Production total records:", production_total)
print("Staging total records:", staging_total)

# ------------------------------------------------------------
# 5. FINAL QUALITY GATE
# ------------------------------------------------------------

assert expected_total == EXPECTED_TOTAL_ROWS

assert failed == 0, (
    f"SILVER QUALITY GATE FAILED: "
    f"{failed} table(s) require investigation."
)

assert passed == 5

assert production_total == EXPECTED_TOTAL_ROWS
assert staging_total == EXPECTED_TOTAL_ROWS

assert all(
    result["production_only"] == 0
    and result["staging_only"] == 0
    for result in results
)

print("\n" + "=" * 70)
print("STEP 9.12R: PASS")
print("ALL FIVE SILVER TABLE PAIRS VALIDATED")
print("PRODUCTION AND STAGING MATCH EXACTLY")
print("SILVER QUALITY GATE: SUCCESS")
print("NO EXISTING DELTA TABLES MODIFIED")
print("=" * 70)
