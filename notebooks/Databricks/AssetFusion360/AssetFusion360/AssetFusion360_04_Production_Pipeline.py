# Databricks notebook source

# Step 9.11A — Prepare an isolated Delta staging schema.
# Keep all 27 existing Bronze, Silver, and Gold tables unchanged.
# Create only the staging namespace for future pipeline outputs.

PRODUCTION_SCHEMA = "workspace.assetfusion360"
STAGING_SCHEMA = "workspace.assetfusion360_staging"

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}
""")

schemas = [
    row.databaseName
    for row in spark.sql("SHOW SCHEMAS IN workspace").collect()
]

assert "assetfusion360" in schemas
assert "assetfusion360_staging" in schemas

print("Production schema:", PRODUCTION_SCHEMA)
print("Staging schema:", STAGING_SCHEMA)
print("STAGING SCHEMA: READY")
print("EXISTING DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11F — Validate all five Silver staging Delta tables.
# Confirm row counts, schemas, and complete data equivalence.
# Read-only: do not modify production or staging tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

expected = {
    "silver_amps_assets": 148522,
    "silver_amps_workorders": 2231535,
    "silver_amps_labor": 3230423,
    "silver_amps_transactions": 755122,
    "silver_amps_transaction_details": 2301927
}

total_rows = 0

print("=" * 65)
print("STEP 9.11F — SILVER STAGING VALIDATION")
print("=" * 65)

for name, expected_rows in expected.items():
    staging_table = f"{STAGING}.{name}"
    production_table = f"{PRODUCTION}.{name}"

    assert spark.catalog.tableExists(staging_table), (
        f"Missing staging table: {staging_table}"
    )

    staged = spark.table(staging_table)
    reference = spark.table(production_table)

    assert staged.schema.names == reference.schema.names

    assert all(
        staged.schema[c].dataType == reference.schema[c].dataType
        for c in reference.columns
    ), f"Schema mismatch: {name}"

    count = staged.count()

    assert count == expected_rows, (
        f"{name}: expected {expected_rows}, found {count}"
    )

    # Verify complete contents, including duplicate records.
    forward = staged.exceptAll(reference).count()
    reverse = reference.exceptAll(staged).count()

    assert forward == 0 and reverse == 0, (
        f"{name}: data mismatches: {forward + reverse}"
    )

    total_rows += count

    print(
        f"{name:38} {count:>10,} rows | "
        f"0 mismatches | PASS"
    )

assert total_rows == 8667529

print("\nTotal Silver records:", f"{total_rows:,}")
print("Validated staging tables:", len(expected))
print("Total data mismatches: 0")

print("\nSTEP 9.11F: PASS")
print("PRODUCTION AND STAGING DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11G — Inventory existing Gold Delta tables.
# Identify which Gold tables still need staging publication.
# Read-only; no existing tables are modified.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

gold_tables = [
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
    "dim_date",
    "dim_part",
    "gold_workorder_monthly_kpis",
    "gold_labor_monthly_kpis",
    "gold_transaction_monthly_kpis",
    "gold_material_monthly_kpis"
]

print("=" * 65)
print("STEP 9.11G — GOLD STAGING INVENTORY")
print("=" * 65)

for name in gold_tables:
    production_table = f"{PRODUCTION}.{name}"
    staging_table = f"{STAGING}.{name}"

    assert spark.catalog.tableExists(production_table), (
        f"Missing Gold reference: {production_table}"
    )

    status = (
        "ALREADY EXISTS"
        if spark.catalog.tableExists(staging_table)
        else "NOT YET CREATED"
    )

    print(f"{name:38} {status}")

print("\nGold reference tables:", len(gold_tables))
print("GOLD INVENTORY: COMPLETE")
print("EXISTING DELTA TABLES: UNCHANGED")
