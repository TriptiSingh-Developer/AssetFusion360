# Databricks notebook source
# Complete quality audit code

from pyspark.sql import functions as F
from functools import reduce

# ==========================================================
# ASSETFUSION360 — STEP 9.12X
# FINAL DATA QUALITY AND SAFETY AUDIT
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

bronze = [f"{PROD}.bronze_{x}" for x in DATASETS]

silver_prod = [
    f"{PROD}.silver_{x}" for x in DATASETS
]

silver_stage = [
    f"{STAGE}.silver_{x}" for x in DATASETS
]

gold_prod = [f"{PROD}.{x}" for x in GOLD]
gold_stage = [f"{STAGE}.{x}" for x in GOLD]

tables = (
    bronze + silver_prod + silver_stage
    + gold_prod + gold_stage
)

assert len(tables) == 49

# ----------------------------------------------------------
# 1. Inspect Delta metadata and latest history
# ----------------------------------------------------------

metadata_results = []

for table in tables:

    assert spark.catalog.tableExists(table), (
        f"Missing table: {table}"
    )

    detail = spark.sql(
        f"DESCRIBE DETAIL {table}"
    ).first().asDict()

    assert detail["format"].lower() == "delta", (
        f"Invalid format: {table}"
    )

    history = (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .orderBy(F.desc("version"))
        .limit(1)
        .collect()
    )

    assert history, f"No history available: {table}"

    latest = history[0].asDict()

    metadata_results.append((
        table,
        int(latest["version"]),
        str(latest.get("operation")),
        str(latest.get("timestamp")),
        int(detail.get("numFiles") or 0),
        int(detail.get("sizeInBytes") or 0)
    ))

print("=== DELTA TABLE HISTORY AUDIT ===")

display(
    spark.createDataFrame(
        metadata_results,
        [
            "table_name",
            "latest_delta_version",
            "latest_operation",
            "latest_operation_timestamp",
            "delta_file_count",
            "delta_size_bytes"
        ]
    )
)

# ----------------------------------------------------------
# 2. Validate 22 production/staging schema pairs
# ----------------------------------------------------------

pairs = (
    [(f"{PROD}.silver_{d}",
      f"{STAGE}.silver_{d}") for d in DATASETS]
    +
    [(f"{PROD}.{g}",
      f"{STAGE}.{g}") for g in GOLD]
)

schema_results = []

for prod_table, stage_table in pairs:

    prod_schema = spark.table(prod_table).schema
    stage_schema = spark.table(stage_table).schema

    schema_match = prod_schema == stage_schema

    schema_results.append((
        prod_table,
        stage_table,
        schema_match,
        "PASS" if schema_match else "FAIL"
    ))

print("=== PRODUCTION / STAGING SCHEMAS ===")

display(
    spark.createDataFrame(
        schema_results,
        [
            "production_table",
            "staging_table",
            "schema_match",
            "result"
        ]
    )
)

# ----------------------------------------------------------
# 3. Profile Silver data-quality flags
# ----------------------------------------------------------

dq_results = []

for dataset in DATASETS:

    table = f"{PROD}.silver_{dataset}"
    df = spark.table(table)

    dq_columns = [
        c for c in df.columns
        if c.lower().startswith("dq_")
    ]

    if not dq_columns:
        dq_results.append((
            dataset,
            "(no dq_ columns found)",
            None,
            None
        ))
        continue

    # This is a descriptive audit, not a universal
    # pass/fail interpretation of every dq_ field.
    #
    # Different dq_ columns can use different meanings.
    # Capture populated and common flag-like values.

    expressions = []

    for col_name in dq_columns:

        flag = F.lower(
            F.trim(F.col(f"`{col_name}`").cast("string"))
        )

        expressions.extend([
            F.count(
                F.col(f"`{col_name}`")
            ).alias(f"{col_name}__populated"),

            F.sum(
                F.when(
                    flag.isin(
                        "true", "1", "yes",
                        "fail", "failed",
                        "error", "invalid"
                    ),
                    1
                ).otherwise(0)
            ).alias(f"{col_name}__flagged")
        ])

    metrics = df.agg(*expressions).first().asDict()

    for col_name in dq_columns:

        dq_results.append((
            dataset,
            col_name,
            int(metrics[f"{col_name}__populated"]),
            int(metrics[f"{col_name}__flagged"] or 0)
        ))

print("=== SILVER DATA QUALITY FLAG PROFILE ===")

display(
    spark.createDataFrame(
        dq_results,
        [
            "dataset",
            "dq_column",
            "non_null_rows",
            "common_flag_value_rows"
        ]
    )
)

# ----------------------------------------------------------
# 4. Final gate
# ----------------------------------------------------------

failed_schemas = [
    r for r in schema_results if not r[2]
]

assert len(metadata_results) == 49, (
    "Not all 49 Delta tables were audited"
)

assert not failed_schemas, (
    f"Schema mismatches found: {failed_schemas}"
)

print("=" * 60)
print("STEP 9.12X — PASS")
print("49 Delta table histories inspected")
print("22 production/staging schemas validated")
print("5 production Silver datasets profiled for DQ columns")
print("No INSERT, UPDATE, DELETE, MERGE or OVERWRITE executed")
print("=" * 60)
