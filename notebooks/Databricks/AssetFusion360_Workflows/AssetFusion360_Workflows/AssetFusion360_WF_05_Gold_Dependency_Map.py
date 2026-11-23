# Databricks notebook source

# ============================================================
# ASSETFUSION360 — STEP 9.12T
# GOLD TRANSFORMATION DEPENDENCY MAPPING
#
# Notebook:
# AssetFusion360_WF_05_Gold_Dependency_Map
#
# PURPOSE:
# - Register all 17 Gold targets
# - Define Silver and Gold dependencies
# - Detect missing dependencies and cycles
# - Calculate safe execution levels
# - Verify existing production/staging inventories
#
# READ-ONLY — NO DELTA TABLES MODIFIED
# ============================================================

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

# ------------------------------------------------------------
# 1. DEFINE GOLD DEPENDENCIES
#
# Every target has:
# - silver: required Silver input tables
# - gold: required Gold input tables
# ------------------------------------------------------------

GOLD_DEPENDENCIES = {

    "dim_asset": {
        "silver": ["silver_amps_assets"],
        "gold": []
    },

    "bridge_asset_code": {
        "silver": [],
        "gold": ["dim_asset"]
    },

    "fact_workorders": {
        "silver": ["silver_amps_workorders"],
        "gold": ["bridge_asset_code"]
    },

    "fact_transactions": {
        "silver": ["silver_amps_transactions"],
        "gold": []
    },

    "fact_labor": {
        "silver": ["silver_amps_labor"],
        "gold": ["fact_workorders"]
    },

    "fact_transaction_details": {
        "silver": [
            "silver_amps_transaction_details"
        ],
        "gold": [
            "fact_transactions",
            "fact_workorders"
        ]
    },

    "dim_workorder_type": {
        "silver": [],
        "gold": ["fact_workorders"]
    },

    "dim_workorder_priority": {
        "silver": [],
        "gold": ["fact_workorders"]
    },

    "dim_transaction_type": {
        "silver": [],
        "gold": ["fact_transactions"]
    },

    "dim_transaction_status": {
        "silver": [],
        "gold": ["fact_transactions"]
    },

    "dim_detail_type": {
        "silver": [],
        "gold": ["fact_transaction_details"]
    },

    "dim_part": {
        "silver": [],
        "gold": ["fact_transaction_details"]
    },

    "dim_date": {
        "silver": [],
        "gold": [
            "fact_workorders",
            "fact_labor",
            "fact_transactions",
            "fact_transaction_details"
        ]
    },

    "gold_workorder_monthly_kpis": {
        "silver": [],
        "gold": ["fact_workorders"]
    },

    "gold_labor_monthly_kpis": {
        "silver": [],
        "gold": ["fact_labor"]
    },

    "gold_transaction_monthly_kpis": {
        "silver": [],
        "gold": ["fact_transactions"]
    },

    "gold_material_monthly_kpis": {
        "silver": [],
        "gold": ["fact_transaction_details"]
    }
}

EXPECTED_GOLD_TABLES = 17

assert len(GOLD_DEPENDENCIES) == EXPECTED_GOLD_TABLES

print("GOLD DEPENDENCY DEFINITIONS: PASS")

# ------------------------------------------------------------
# 2. VERIFY ALL REFERENCED GOLD DEPENDENCIES EXIST
# ------------------------------------------------------------

gold_targets = set(GOLD_DEPENDENCIES)

for target, config in GOLD_DEPENDENCIES.items():

    for dependency in config["gold"]:

        assert dependency in gold_targets, (
            f"{target} references undefined Gold "
            f"dependency: {dependency}"
        )

    assert target not in config["gold"], (
        f"Self-dependency detected: {target}"
    )

print("GOLD DEPENDENCY REFERENCES: PASS")

# ------------------------------------------------------------
# 3. VERIFY SILVER SOURCE TABLES EXIST
# ------------------------------------------------------------

silver_inputs = sorted({
    table
    for config in GOLD_DEPENDENCIES.values()
    for table in config["silver"]
})

assert len(silver_inputs) == 5

for table_name in silver_inputs:

    full_table = f"{STAGING}.{table_name}"

    assert spark.catalog.tableExists(full_table), (
        f"Missing required staging Silver input: "
        f"{full_table}"
    )

print("SILVER SOURCE DEPENDENCIES: PASS")

# ------------------------------------------------------------
# 4. VERIFY ALL GOLD TARGET TABLES EXIST
#
# Check actual table names rather than relying
# on prefixes alone.
# ------------------------------------------------------------

for target in GOLD_DEPENDENCIES:

    production_table = f"{PRODUCTION}.{target}"
    staging_table = f"{STAGING}.{target}"

    assert spark.catalog.tableExists(
        production_table
    ), f"Missing production Gold table: {target}"

    assert spark.catalog.tableExists(
        staging_table
    ), f"Missing staging Gold table: {target}"

print("ALL 17 GOLD TARGETS: PRESENT")

# ------------------------------------------------------------
# 5. CALCULATE DEPENDENCY LEVELS
#
# Topological sorting:
# A task can execute only after all required
# Gold inputs have been completed.
# ------------------------------------------------------------

remaining = set(GOLD_DEPENDENCIES)

completed = set()

execution_levels = []

while remaining:

    ready = sorted([
        target
        for target in remaining
        if set(
            GOLD_DEPENDENCIES[target]["gold"]
        ).issubset(completed)
    ])

    assert ready, (
        "Gold dependency cycle detected. "
        f"Unresolved tables: {sorted(remaining)}"
    )

    execution_levels.append(ready)

    completed.update(ready)
    remaining.difference_update(ready)

assert completed == gold_targets

print("GOLD DEPENDENCY GRAPH: PASS")
print("NO CIRCULAR DEPENDENCIES FOUND")

# ------------------------------------------------------------
# 6. DISPLAY EXECUTION PLAN
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("GOLD WORKFLOW EXECUTION PLAN")
print("=" * 70)

for level_number, tables in enumerate(
    execution_levels,
    start=1
):

    print(f"\nLEVEL {level_number}")

    for table_name in tables:

        config = GOLD_DEPENDENCIES[table_name]

        print(f"  {table_name}")

        print(
            "    Silver dependencies:",
            config["silver"] or "None"
        )

        print(
            "    Gold dependencies:",
            config["gold"] or "None"
        )

# ------------------------------------------------------------
# 7. CREATE DEPENDENCY REPORT
# ------------------------------------------------------------

report_rows = []

for level_number, tables in enumerate(
    execution_levels,
    start=1
):

    for target in tables:

        config = GOLD_DEPENDENCIES[target]

        report_rows.append({
            "level": level_number,
            "gold_target": target,
            "silver_dependencies": ", ".join(
                config["silver"]
            ),
            "gold_dependencies": ", ".join(
                config["gold"]
            )
        })

from pyspark.sql import types as T

report_schema = T.StructType([
    T.StructField("level", T.IntegerType()),
    T.StructField("gold_target", T.StringType()),
    T.StructField("silver_dependencies", T.StringType()),
    T.StructField("gold_dependencies", T.StringType())
])

report_df = spark.createDataFrame(
    report_rows,
    schema=report_schema
)

display(
    report_df.orderBy("level", "gold_target")
)

# ------------------------------------------------------------
# 8. FINAL VALIDATION
# ------------------------------------------------------------

assert len(report_rows) == 17

assert len({
    row["gold_target"]
    for row in report_rows
}) == 17

print("\n" + "=" * 70)
print("STEP 9.12T — GOLD DEPENDENCY MAPPING")
print("=" * 70)

print("Gold targets:", len(GOLD_DEPENDENCIES))
print("Silver input tables:", len(silver_inputs))
print("Execution levels:", len(execution_levels))

print("Dependency graph: VALID")
print("Circular dependencies: NONE")
print("Production Gold tables modified: NO")
print("Staging Gold tables modified: NO")

print("\nSTEP 9.12T: PASS")
