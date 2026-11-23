# Databricks notebook source
# Complete audit code

from pyspark.sql import functions as F

# ==========================================================
# ASSETFUSION360 — STEP 9.12W
# END-TO-END BRONZE / SILVER / GOLD AUDIT
# READ ONLY — NO TABLE WRITES
# ==========================================================

PROD = "workspace.assetfusion360"
STAGE = "workspace.assetfusion360_staging"

DATASETS = [
    "amps_assets",
    "amps_workorders",
    "amps_transactions",
    "amps_transaction_details",
    "amps_labor"
]

GOLD = [
    "dim_asset",
    "bridge_asset_code",
    "fact_workorders",
    "fact_transactions",
    "fact_labor",
    "fact_transaction_details",
    "dim_workorder_type",
    "dim_workorder_priority",
    "dim_transaction_type",
    "dim_transaction_status",
    "dim_detail_type",
    "dim_part",
    "dim_date",
    "gold_workorder_monthly_kpis",
    "gold_labor_monthly_kpis",
    "gold_transaction_monthly_kpis",
    "gold_material_monthly_kpis"
]

bronze = [f"{PROD}.bronze_{d}" for d in DATASETS]
silver_prod = [f"{PROD}.silver_{d}" for d in DATASETS]
silver_stage = [f"{STAGE}.silver_{d}" for d in DATASETS]
gold_prod = [f"{PROD}.{g}" for g in GOLD]
gold_stage = [f"{STAGE}.{g}" for g in GOLD]

all_tables = (
    bronze + silver_prod + silver_stage
    + gold_prod + gold_stage
)

assert len(all_tables) == 49
assert len(set(all_tables)) == 49

# Set True only for expensive, row-level equality checks
FULL_COMPARE = False

# ----------------------------------------------------------
# 1. Confirm all tables exist and are Delta
# ----------------------------------------------------------

inventory = []
counts = {}
schemas = {}

for table in all_tables:

    assert spark.catalog.tableExists(table), (
        f"Missing required table: {table}"
    )

    detail = spark.sql(
        f"DESCRIBE DETAIL {table}"
    ).first().asDict()

    assert detail["format"].lower() == "delta", (
        f"Not a Delta table: {table}"
    )

    df = spark.table(table)
    row_count = df.count()

    counts[table] = row_count
    schemas[table] = df.schema

    inventory.append((
        table,
        row_count,
        len(df.columns),
        detail["format"]
    ))

print("=== DELTA TABLE INVENTORY ===")

display(
    spark.createDataFrame(
        inventory,
        ["table_name", "row_count", "column_count", "format"]
    )
)

# ----------------------------------------------------------
# 2. Bronze -> Silver -> staging reconciliation
# ----------------------------------------------------------

lineage_results = []

for dataset in DATASETS:

    b = f"{PROD}.bronze_{dataset}"
    s = f"{PROD}.silver_{dataset}"
    t = f"{STAGE}.silver_{dataset}"

    b_count = counts[b]
    s_count = counts[s]
    t_count = counts[t]

    passed = (
        b_count == s_count == t_count
    )

    lineage_results.append((
        dataset,
        b_count,
        s_count,
        t_count,
        "PASS" if passed else "FAIL"
    ))

print("=== BRONZE / SILVER LINEAGE ===")

display(
    spark.createDataFrame(
        lineage_results,
        [
            "dataset",
            "bronze_rows",
            "production_silver_rows",
            "staging_silver_rows",
            "result"
        ]
    )
)

# ----------------------------------------------------------
# 3. Compare production and staging tables
# ----------------------------------------------------------

comparison_results = []

pairs = (
    [(f"{PROD}.silver_{d}",
      f"{STAGE}.silver_{d}") for d in DATASETS]
    +
    [(f"{PROD}.{g}",
      f"{STAGE}.{g}") for g in GOLD]
)

for prod_table, stage_table in pairs:

    rows_match = (
        counts[prod_table] == counts[stage_table]
    )

    schema_match = (
        schemas[prod_table] == schemas[stage_table]
    )

    content_match = None

    if FULL_COMPARE and rows_match and schema_match:

        prod_df = spark.table(prod_table)
        stage_df = spark.table(stage_table)

        # exceptAll preserves duplicate-row differences.
        # Both directions are needed.
        left_diff = (
            prod_df.exceptAll(stage_df).limit(1).count()
        )

        right_diff = (
            stage_df.exceptAll(prod_df).limit(1).count()
        )

        content_match = (
            left_diff == 0 and right_diff == 0
        )

    passed = (
        rows_match and schema_match
        and (content_match is not False)
    )

    comparison_results.append((
        prod_table,
        stage_table,
        rows_match,
        schema_match,
        content_match,
        "PASS" if passed else "FAIL"
    ))

print("=== PRODUCTION VS STAGING ===")

display(
    spark.createDataFrame(
        comparison_results,
        [
            "production_table",
            "staging_table",
            "row_counts_match",
            "schemas_match",
            "content_match_if_checked",
            "result"
        ]
    )
)

# ----------------------------------------------------------
# 4. Final assertions
# ----------------------------------------------------------

failed_lineage = [
    r for r in lineage_results if r[-1] == "FAIL"
]

failed_pairs = [
    r for r in comparison_results if r[-1] == "FAIL"
]

assert not failed_lineage, (
    f"Bronze/Silver reconciliation failures: {failed_lineage}"
)

assert not failed_pairs, (
    f"Production/staging failures: {failed_pairs}"
)

print("=" * 60)
print("STEP 9.12W — PASS")
print("49 Delta tables validated")
print("5 source lineage count checks passed")
print("22 production/staging pairs passed")
print(f"Full row-content comparison performed: {FULL_COMPARE}")
print("No existing tables were modified")
print("=" * 60)
