# Databricks notebook source

# ============================================================
# AssetFusion360 — Workflow 01: Validate Inputs
# READ-ONLY: No tables are created, modified, or overwritten.
# ============================================================

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

GOLD_TABLES = [
    "dim_asset",
    "bridge_asset_code",
    "fact_workorders",
    "fact_labor",
    "fact_transactions",
    "fact_transaction_details",
    "dim_workorder_type",
    "dim_workorder_priority",
    "dim_transaction_type",
    "dim_transaction_status",
    "dim_detail_type",
    "dim_date",
    "dim_part",
    "gold_workorder_monthly_kpis",
    "gold_labor_monthly_kpis",
    "gold_transaction_monthly_kpis",
    "gold_material_monthly_kpis"
]

# 1. Check catalog schemas
assert spark.catalog.databaseExists(PRODUCTION), (
    f"Missing production schema: {PRODUCTION}"
)

assert spark.catalog.databaseExists(STAGING), (
    f"Missing staging schema: {STAGING}"
)

# 2. Read table inventories
production_tables = {
    t.name
    for t in spark.catalog.listTables(PRODUCTION)
    if not t.isTemporary
}

staging_tables = {
    t.name
    for t in spark.catalog.listTables(STAGING)
    if not t.isTemporary
}

# 3. Verify Gold tables in both schemas
for table in GOLD_TABLES:
    assert table in production_tables, (
        f"Production table missing: {table}"
    )
    assert table in staging_tables, (
        f"Staging table missing: {table}"
    )

# 4. Identify the five Silver staging tables
silver_tables = sorted(
    staging_tables - set(GOLD_TABLES)
)

assert len(silver_tables) == 5, (
    f"Expected 5 Silver tables; found "
    f"{len(silver_tables)}: {silver_tables}"
)

for table in silver_tables:
    assert table in production_tables, (
        f"Silver production reference missing: {table}"
    )

# 5. Verify all 22 staging tables use Delta
for table in silver_tables + GOLD_TABLES:
    metadata = spark.sql(
        f"DESCRIBE DETAIL {STAGING}.{table}"
    ).first()

    assert metadata["format"].lower() == "delta", (
        f"Not a Delta table: {table}"
    )

# 6. Final validation summary
print("=" * 60)
print("ASSETFUSION360 — WORKFLOW 01 INPUT VALIDATION")
print("=" * 60)

print("Production schema:", PRODUCTION)
print("Staging schema:", STAGING)
print("Silver staging tables:", len(silver_tables))
print("Gold staging tables:", len(GOLD_TABLES))
print("Total staging tables:", len(staging_tables))

print("\nSilver tables:")
for table in silver_tables:
    print(" -", table)

print("\nWORKFLOW 01 INPUT VALIDATION: PASS")
print("NO TABLES MODIFIED")
