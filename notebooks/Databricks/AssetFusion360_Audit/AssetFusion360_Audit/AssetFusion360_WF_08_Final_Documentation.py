# Databricks notebook source
# Complete documentation code

from pyspark.sql import functions as F

# ==========================================================
# ASSETFUSION360 — STEP 9.12Y
# FINAL ARCHITECTURE AND DOCUMENTATION
# READ ONLY
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

groups = [
    ("Bronze", "Production",
     [f"{PROD}.bronze_{d}" for d in DATASETS]),

    ("Silver", "Production",
     [f"{PROD}.silver_{d}" for d in DATASETS]),

    ("Silver", "Staging",
     [f"{STAGE}.silver_{d}" for d in DATASETS]),

    ("Gold", "Production",
     [f"{PROD}.{g}" for g in GOLD]),

    ("Gold", "Staging",
     [f"{STAGE}.{g}" for g in GOLD])
]

# ----------------------------------------------------------
# 1. Build documentation inventory
# ----------------------------------------------------------

documentation = []

for layer, environment, tables in groups:

    for table in tables:

        assert spark.catalog.tableExists(table), (
            f"Missing required table: {table}"
        )

        detail = spark.sql(
            f"DESCRIBE DETAIL {table}"
        ).first().asDict()

        assert detail["format"].lower() == "delta"

        latest = (
            spark.sql(f"DESCRIBE HISTORY {table}")
            .orderBy(F.desc("version"))
            .limit(1)
            .first()
        )

        assert latest is not None

        df = spark.table(table)

        documentation.append((
            layer,
            environment,
            table,
            df.count(),
            len(df.columns),
            int(latest["version"]),
            str(latest["timestamp"]),
            int(detail.get("numFiles") or 0)
        ))

assert len(documentation) == 49

print("=== ASSETFUSION360 TABLE DOCUMENTATION ===")

display(
    spark.createDataFrame(
        documentation,
        [
            "layer",
            "environment",
            "table_name",
            "row_count",
            "column_count",
            "delta_version",
            "latest_change_timestamp",
            "delta_file_count"
        ]
    )
)

# ----------------------------------------------------------
# 2. Document validation workflow
# ----------------------------------------------------------

workflow_tasks = [
    ("validate_inputs",
     "AssetFusion360_WF_01_Validate_Inputs"),

    ("silver_assets",
     "AssetFusion360_WF_02A_Silver_Assets"),

    ("silver_workorders",
     "AssetFusion360_WF_02B_Silver_WorkOrders"),

    ("silver_transactions",
     "AssetFusion360_WF_02C_Silver_Transactions"),

    ("silver_transaction_details",
     "AssetFusion360_WF_02D_Silver_TransactionDetails"),

    ("silver_labor",
     "AssetFusion360_WF_02E_Silver_Labor"),

    ("validate_silver",
     "AssetFusion360_WF_03_Validate_All_Silver"),

    ("gold_readiness",
     "AssetFusion360_WF_04_Gold_Readiness"),

    ("gold_dependencies",
     "AssetFusion360_WF_05_Gold_Dependency_Map"),

    ("end_to_end_audit",
     "AssetFusion360_WF_06_End_to_End_Audit"),

    ("final_quality_safety_audit",
     "AssetFusion360_WF_07_Final_Quality_Safety_Audit"),

    ("final_documentation",
     "AssetFusion360_WF_08_Final_Documentation")
]

print("\n=== JOB TASK DOCUMENTATION ===")

for i, (task, notebook) in enumerate(
    workflow_tasks, start=1
):
    print(f"{i:02d}. {task}")
    print(f"    Notebook: {notebook}")

# ----------------------------------------------------------
# 3. Print architecture documentation
# ----------------------------------------------------------

architecture = """
ASSETFUSION360
Enterprise Asset Management, Predictive Maintenance
and Operational Analytics

PLATFORM
Azure Databricks / Delta Lake / Unity Catalog

PRODUCTION SCHEMA
workspace.assetfusion360

STAGING SCHEMA
workspace.assetfusion360_staging

DATA ARCHITECTURE

BRONZE:
5 source-ingestion Delta tables

SILVER:
5 standardized production Delta tables
5 corresponding staging Delta tables

GOLD:
17 production dimension, bridge, fact and KPI tables
17 corresponding staging Gold tables

TOTAL:
27 production Delta tables
22 staging Delta tables
49 Delta tables overall

VALIDATION WORKFLOW:
AssetFusion360_Bronze_Silver_Gold_Validation

VALIDATION RESPONSIBILITIES:
- Source and table availability
- Independent Silver transformation reconstruction
- Silver production/staging validation
- Gold availability and consistency
- Gold dependency mapping
- End-to-end lineage and count checks
- Final Delta metadata and data-quality audits

IMPORTANT SCOPE NOTE:
This workflow validates existing Delta tables.
It does not itself perform a complete fresh Bronze
ingestion, publish Silver staging tables, or execute
all 17 Gold transformation writes.

PLANNED DOWNSTREAM COMPONENTS:
Microsoft Fabric integration
Power BI enterprise asset management dashboards
AssetGuard predictive maintenance machine learning
Risk scoring and explainability
"""

print("\n=== FINAL ARCHITECTURE SUMMARY ===")
print(architecture)

# ----------------------------------------------------------
# 4. Final documentation checkpoint
# ----------------------------------------------------------

print("=" * 60)
print("STEP 9.12Y — PASS")
print("49 existing Delta tables documented")
print("12 workflow notebook/task definitions listed")
print("Architecture summary generated")
print("Read-only execution completed")
print("=" * 60)
