# Databricks notebook source

# Step 9.7F — Rebuild the asset-code bridge using production business rules.
# Preserve ambiguous asset identities and unresolved relationships.
# Compare with existing Gold data without modifying any Delta tables.

from pyspark.sql import functions as F

assets = spark.table("workspace.assetfusion360.dim_asset")
existing = spark.table("workspace.assetfusion360.bridge_asset_code")

bridge = (
    assets
    .groupBy("asset_code")
    .agg(
        F.count("*").cast("int").alias("asset_match_count"),
        F.sort_array(
            F.collect_list("asset_identity")
        ).alias("candidate_asset_identities"),
        F.max(
            F.col("asset_code_placeholder").cast("int")
        ).cast("boolean").alias("is_placeholder")
    )
    .select(
        "asset_code",
        "asset_match_count",

        F.when(
            F.col("asset_match_count") == 1,
            F.element_at("candidate_asset_identities", 1)
        ).cast("long").alias("resolved_asset_identity"),

        "candidate_asset_identities",

        F.when(
            F.col("asset_match_count") > 1,
            F.lit("AMBIGUOUS")
        ).otherwise(
            F.lit("UNIQUE")
        ).alias("resolution_status"),

        (
            F.col("asset_match_count") > 1
        ).alias("is_ambiguous"),

        "is_placeholder"
    )
)

assert bridge.count() == 148521
assert bridge.columns == existing.columns

ambiguous = bridge.filter(F.col("is_ambiguous")).collect()

assert len(ambiguous) == 1
assert ambiguous[0]["asset_code"] == "B223PA"
assert ambiguous[0]["candidate_asset_identities"] == [5514, 6133]
assert ambiguous[0]["resolved_asset_identity"] is None

forward = bridge.exceptAll(existing).count()
reverse = existing.exceptAll(bridge).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("=" * 65)
print("STEP 9.7F — GOLD ASSET-CODE BRIDGE")
print("=" * 65)

print("Source asset records:", assets.count())
print("Bridge records:", bridge.count())
print("Ambiguous asset codes:", len(ambiguous))
print("B223PA candidates:", ambiguous[0]["candidate_asset_identities"])
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7F ASSET BRIDGE TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7G — Reproduce the production Gold Work Orders transformation.
# Resolve asset identities through the bridge while preserving ambiguity.
# Compare all records with existing Gold; do not overwrite Delta tables.

from pyspark.sql import functions as F

silver = spark.table("workspace.assetfusion360.silver_amps_workorders")
bridge = spark.table("workspace.assetfusion360.bridge_asset_code")
existing = spark.table("workspace.assetfusion360.fact_workorders")

# Original Gold-to-Silver source mapping
mapping = {
    "workorder_identity": "wo_identity_int",
    "workorder_code": "evt_code",
    "asset_code": "evt_object",
    "workorder_type": "evt_type",
    "workorder_description": "evt_desc",
    "workorder_priority": "evt_priority",
    "workorder_class": "evt_class",
    "job_type": "evt_jobtype",
    "workorder_date": "evt_date_ts",
    "reported_at": "evt_reported_ts",
    "started_at": "evt_start_ts",
    "due_at": "evt_due_ts",
    "completed_at": "evt_completed_ts",
    "created_at": "evt_created_ts",
    "updated_at": "evt_updated_ts",
    "duration": "wo_duration_decimal",
    "update_count": "wo_updatecount_int",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
}

# Join by the original asset code; never guess an ambiguous identity
joined = silver.alias("s").join(
    bridge.select(
        "asset_code",
        "resolved_asset_identity",
        "resolution_status",
        "is_ambiguous",
        "is_placeholder"
    ).alias("b"),
    F.col("s.evt_object") == F.col("b.asset_code"),
    "left"
)

# The local production pipeline rejects unmatched asset codes
unmatched = joined.filter(
    F.col("b.asset_code").isNull()
).count()

assert unmatched == 0, f"Unmatched asset references: {unmatched}"

# Reproduce the original Gold schema and column order
derived = {
    "resolved_asset_identity": F.col("b.resolved_asset_identity"),
    "asset_resolution_status": F.col("b.resolution_status"),
    "asset_reference_ambiguous": F.col("b.is_ambiguous"),
    "asset_code_placeholder": F.col("b.is_placeholder"),
}

gold = joined.select(*[
    (
        derived[name]
        if name in derived
        else F.col(f"s.{mapping[name]}")
    ).cast(existing.schema[name].dataType).alias(name)
    for name in existing.columns
])

# Validate original production business rules
assert gold.count() == 2231535

summary = gold.agg(
    F.sum(F.when(
        F.col("asset_resolution_status") == "UNIQUE", 1
    ).otherwise(0)).alias("unique_assets"),

    F.sum(F.when(
        F.col("asset_resolution_status") == "AMBIGUOUS", 1
    ).otherwise(0)).alias("ambiguous_assets"),

    F.sum(F.col("workorder_priority").isNull().cast("int"))
     .alias("missing_priorities"),

    F.sum(F.col("completed_at").isNull().cast("int"))
     .alias("missing_completions"),

    F.sum(F.when(
        F.col("silver_dq_issue") == True, 1
    ).otherwise(0)).alias("dq_issues")
).first()

assert summary["unique_assets"] == 2231135
assert summary["ambiguous_assets"] == 400
assert summary["missing_priorities"] == 994238
assert summary["missing_completions"] == 25585
assert summary["dq_issues"] == 0

# Compare the recreated output against the validated production table
forward = gold.exceptAll(existing).count()
reverse = existing.exceptAll(gold).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("=" * 65)
print("STEP 9.7G — GOLD WORK ORDERS TRANSFORMATION")
print("=" * 65)

print("Silver source rows:", 2231535)
print("Gold transformed rows:", 2231535)
print("Unique asset mappings:", summary["unique_assets"])
print("Ambiguous asset mappings:", summary["ambiguous_assets"])
print("Missing priorities:", summary["missing_priorities"])
print("Missing completion timestamps:", summary["missing_completions"])
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7G GOLD WORK ORDERS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7H — Reproduce the production Gold Transactions transformation.
# Preserve transaction identities, source fields, and data-quality flags.
# Reconcile against existing Gold without modifying any Delta tables.

from pyspark.sql import functions as F

silver = spark.table("workspace.assetfusion360.silver_amps_transactions")
existing = spark.table("workspace.assetfusion360.fact_transactions")

mapping = {
    "transaction_identity": "transaction_identity_int",
    "transaction_code": "tra_code",
    "transaction_code_numeric": "transaction_code_int",
    "transaction_description": "tra_desc",
    "transaction_type": "tra_type",
    "transaction_status": "tra_status",
    "request_code": "tra_req",
    "request_identity": "transaction_request_int",
    "from_entity": "tra_fromentity",
    "from_code": "tra_fromcode",
    "to_entity": "tra_toentity",
    "to_code": "tra_tocode",
    "transaction_date": "transaction_date_ts",
    "created_at": "transaction_created_ts",
    "updated_at": "transaction_updated_ts",
    "update_count": "transaction_updatecount_int",
    "dq_transaction_code_invalid": "dq_transaction_code_invalid",
    "dq_transaction_identity_invalid": "dq_transaction_identity_invalid",
    "dq_transaction_request_invalid": "dq_transaction_request_invalid",
    "dq_transaction_updatecount_invalid": "dq_transaction_updatecount_invalid",
    "dq_transaction_date_invalid": "dq_transaction_date_invalid",
    "dq_transaction_created_invalid": "dq_transaction_created_invalid",
    "dq_transaction_updated_invalid": "dq_transaction_updated_invalid",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
    "source_batch": "_bronze_batch_id",
    "source_ingested_at": "_bronze_ingested_at_utc",
}

assert set(mapping) == set(existing.columns)
assert all(c in silver.columns for c in mapping.values())

# Apply the production field mappings and preserve the Gold schema
gold = silver.select(*[
    F.col(mapping[name])
    .cast(existing.schema[name].dataType)
    .alias(name)
    for name in existing.columns
])

# Validate production row count and unique transaction keys
assert gold.count() == 755122

keys = gold.agg(
    F.countDistinct("transaction_identity").alias("identities"),
    F.countDistinct("transaction_code").alias("codes"),
    F.sum(
        F.col("transaction_identity").isNull().cast("int")
    ).alias("null_identities"),
    F.sum(
        (
            F.col("transaction_code").isNull()
            | (F.length(F.trim("transaction_code")) == 0)
        ).cast("int")
    ).alias("missing_codes")
).first()

assert keys["identities"] == 755122
assert keys["codes"] == 755122
assert keys["null_identities"] == 0
assert keys["missing_codes"] == 0

# Verify the Silver quality indicators remain unchanged
quality_columns = [
    c for c in gold.columns
    if c.startswith("dq_") or c == "silver_dq_issue"
]

quality = gold.agg(*[
    F.sum(
        F.when(F.col(c) == True, 1).otherwise(0)
    ).alias(c)
    for c in quality_columns
]).first()

assert all(quality[c] == 0 for c in quality_columns)

# Verify every field and record against the existing production table
forward = gold.exceptAll(existing).count()
reverse = existing.exceptAll(gold).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("=" * 65)
print("STEP 9.7H — GOLD TRANSACTIONS TRANSFORMATION")
print("=" * 65)
print("Silver source rows:", 755122)
print("Gold transformed rows:", 755122)
print("Unique transaction identities:", keys["identities"])
print("Unique transaction codes:", keys["codes"])
print("Gold columns:", len(gold.columns))
print("Data-quality flags raised:", sum(quality[c] for c in quality_columns))
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7H GOLD TRANSACTIONS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7I — Reproduce the original Gold Labor transformation using PySpark.
# Resolve work-order references and preserve labor financial values.
# Compare against production Gold without overwriting any Delta table.

from pyspark.sql import functions as F

silver = spark.table("workspace.assetfusion360.silver_amps_labor")
workorders = spark.table("workspace.assetfusion360.fact_workorders")
existing = spark.table("workspace.assetfusion360.fact_labor")

# Step 1: Define original Gold-to-Silver source mappings
mapping = {
    "labor_identity": "labor_identity_int",
    "workorder_code": "boo_event",
    "labor_date": "labor_date_ts",
    "labor_activity": "boo_act",
    "trade": "boo_trade",
    "occupation_type": "boo_octype",
    "labor_hours": "labor_hours_decimal",
    "labor_rate": "labor_rate_decimal",
    "labor_cost": "labor_cost_decimal",
    "correction_code": "boo_correction",
    "labor_description": "boo_desc",
    "labor_code": "boo_code",
    "negative_hours_flag": "dq_negative_labor_hours",
    "negative_cost_flag": "dq_negative_labor_cost",
    "zero_rate_flag": "dq_zero_labor_rate",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
    "source_batch": "_bronze_batch_id",
}

# Step 2: Create the work-order reference lookup
lookup = workorders.select(
    F.col("workorder_code").alias("_lookup_code"),
    F.col("workorder_identity").alias("_lookup_identity")
)

# A one-to-many lookup would duplicate labor records
assert workorders.select("workorder_code").distinct().count() == 2231535

# Step 3: Join Silver labor with work-order identities
joined = silver.alias("s").join(
    lookup.alias("w"),
    F.col("s.boo_event") == F.col("w._lookup_code"),
    "left"
)

# Step 4: Reproduce the original relationship-resolution rules
reference_missing = (
    F.col("s.boo_event").isNull()
    | (F.length(F.trim(F.col("s.boo_event"))) == 0)
)

derived = {
    "workorder_identity": F.col("w._lookup_identity"),
    "workorder_match_status": (
        F.when(reference_missing, F.lit("MISSING_REFERENCE"))
         .when(F.col("w._lookup_code").isNotNull(), F.lit("MATCHED"))
         .otherwise(F.lit("UNMATCHED"))
    ),
}

# Step 5: Preserve the production Gold schema and column order
gold_labor = joined.select(*[
    (
        derived[name]
        if name in derived
        else F.col(f"s.{mapping[name]}")
    ).cast(existing.schema[name].dataType).alias(name)
    for name in existing.columns
])

# Step 6: Validate original production metrics
assert gold_labor.count() == 3230423

metrics = gold_labor.agg(
    F.sum((F.col("workorder_match_status") == "MATCHED")
          .cast("int")).alias("matched"),

    F.sum((F.col("workorder_match_status") == "UNMATCHED")
          .cast("int")).alias("unmatched"),

    F.sum((F.col("workorder_match_status") == "MISSING_REFERENCE")
          .cast("int")).alias("missing"),

    F.sum((F.col("negative_hours_flag") == True)
          .cast("int")).alias("negative_hours"),

    F.sum((F.col("negative_cost_flag") == True)
          .cast("int")).alias("negative_cost"),

    F.sum((F.col("zero_rate_flag") == True)
          .cast("int")).alias("zero_rate"),

    F.sum((F.col("silver_dq_issue") == True)
          .cast("int")).alias("dq_issues")
).first()

expected_metrics = {
    "matched": 3217345,
    "unmatched": 13078,
    "missing": 0,
    "negative_hours": 74609,
    "negative_cost": 71374,
    "zero_rate": 188165,
    "dq_issues": 0,
}

for metric, expected_value in expected_metrics.items():
    assert metrics[metric] == expected_value, (
        f"{metric}: expected {expected_value}, found {metrics[metric]}"
    )

# Step 7: Compare the entire transformed dataset
forward = gold_labor.exceptAll(existing).count()
reverse = existing.exceptAll(gold_labor).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("=" * 65)
print("STEP 9.7I — GOLD LABOR FACT TRANSFORMATION")
print("=" * 65)

print("Silver labor rows:", 3230423)
print("Transformed Gold rows:", 3230423)
print("Matched work orders:", metrics["matched"])
print("Unmatched work orders:", metrics["unmatched"])
print("Negative labor hours:", metrics["negative_hours"])
print("Negative labor costs:", metrics["negative_cost"])
print("Zero labor rates:", metrics["zero_rate"])
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7I GOLD LABOR TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7J — Reproduce the original transaction-details Gold transformation.
# Resolve transaction and work-order relationships while preserving missing references.
# Validate full data equivalence without modifying existing Delta tables.

from pyspark.sql import functions as F

silver = spark.table(
    "workspace.assetfusion360.silver_amps_transaction_details"
)

transactions = spark.table(
    "workspace.assetfusion360.fact_transactions"
)

workorders = spark.table(
    "workspace.assetfusion360.fact_workorders"
)

existing = spark.table(
    "workspace.assetfusion360.fact_transaction_details"
)

# Step 1: Original source-to-Gold column mappings
mapping = {
    "detail_identity": "detail_identity_int",
    "transaction_code": "trl_trans",
    "workorder_code": "trl_event",
    "transaction_line": "trl_line",
    "detail_type": "trl_type",
    "activity_code": "trl_act",
    "detail_description": "trl_desc",
    "detail_date": "detail_date_ts",
    "detail_created_at": "detail_created_ts",
    "detail_updated_at": "detail_updated_ts",
    "part_code": "par_code",
    "part_identity": "part_identity_int",
    "part_description": "par_desc",
    "part_class": "par_class",
    "part_uom": "par_uom",
    "store_code": "trl_store",
    "bin_code": "trl_bin",
    "quantity": "detail_quantity_decimal",
    "unit_price": "detail_price_decimal",
    "average_price": "detail_avgprice_decimal",
    "negative_quantity_flag": "dq_negative_quantity",
    "negative_price_flag": "dq_negative_price",
    "zero_quantity_flag": "dq_zero_quantity",
    "zero_price_flag": "dq_zero_price",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
    "source_batch": "_bronze_batch_id",
}

# Step 2: Prepare relationship lookups
transaction_lookup = transactions.select(
    F.col("transaction_code").alias("_transaction_code"),
    F.col("transaction_identity").alias("_transaction_identity")
)

workorder_lookup = workorders.select(
    F.col("workorder_code").alias("_workorder_code"),
    F.col("workorder_identity").alias("_workorder_identity")
)

# Step 3: Join both lookups without filtering Silver records
joined = (
    silver.alias("s")
    .join(
        transaction_lookup.alias("t"),
        F.col("s.trl_trans") == F.col("t._transaction_code"),
        "left"
    )
    .join(
        workorder_lookup.alias("w"),
        F.col("s.trl_event") == F.col("w._workorder_code"),
        "left"
    )
)

# Step 4: Preserve the original missing/unmatched business rules
missing_transaction = (
    F.col("s.trl_trans").isNull()
    | (F.length(F.trim(F.col("s.trl_trans"))) == 0)
)

missing_workorder = (
    F.col("s.trl_event").isNull()
    | (F.length(F.trim(F.col("s.trl_event"))) == 0)
)

derived = {
    "transaction_identity": F.col("t._transaction_identity"),

    "transaction_match_status": (
        F.when(missing_transaction, F.lit("MISSING_REFERENCE"))
         .when(
             F.col("t._transaction_code").isNotNull(),
             F.lit("MATCHED")
         )
         .otherwise(F.lit("UNMATCHED"))
    ),

    "workorder_identity": F.col("w._workorder_identity"),

    "workorder_match_status": (
        F.when(missing_workorder, F.lit("MISSING_REFERENCE"))
         .when(
             F.col("w._workorder_code").isNotNull(),
             F.lit("MATCHED")
         )
         .otherwise(F.lit("UNMATCHED"))
    ),
}

# Step 5: Reproduce the original Gold schema and column order
gold_details = joined.select(*[
    (
        derived[name]
        if name in derived
        else F.col(f"s.{mapping[name]}")
    ).cast(existing.schema[name].dataType).alias(name)
    for name in existing.columns
])

# Step 6: Validate the original production counts
assert gold_details.count() == 2301927

metrics = gold_details.agg(
    F.countDistinct("detail_identity").alias("unique_details"),

    *[
        F.count(
            F.when(F.col(column) == status, 1)
        ).alias(alias)
        for column, status, alias in [
            ("transaction_match_status", "MATCHED", "transaction_matched"),
            ("transaction_match_status", "UNMATCHED", "transaction_unmatched"),
            ("transaction_match_status", "MISSING_REFERENCE", "transaction_missing"),
            ("workorder_match_status", "MATCHED", "workorder_matched"),
            ("workorder_match_status", "UNMATCHED", "workorder_unmatched"),
            ("workorder_match_status", "MISSING_REFERENCE", "workorder_missing"),
        ]
    ],

    *[
        F.count(
            F.when(F.col(column) == True, 1)
        ).alias(alias)
        for column, alias in [
            ("negative_quantity_flag", "negative_quantity"),
            ("negative_price_flag", "negative_price"),
            ("zero_quantity_flag", "zero_quantity"),
            ("zero_price_flag", "zero_price"),
            ("silver_dq_issue", "dq_issues"),
        ]
    ]
).first()

expected = {
    "unique_details": 2301927,
    "transaction_matched": 2300536,
    "transaction_unmatched": 1391,
    "transaction_missing": 0,
    "workorder_matched": 809009,
    "workorder_unmatched": 135,
    "workorder_missing": 1492783,
    "negative_quantity": 44053,
    "negative_price": 5501,
    "zero_quantity": 164449,
    "zero_price": 61515,
    "dq_issues": 0,
}

for name, expected_value in expected.items():
    assert metrics[name] == expected_value, (
        f"{name}: expected {expected_value}, got {metrics[name]}"
    )

# Step 7: Compare the entire transformed dataset
forward = gold_details.exceptAll(existing).count()
reverse = existing.exceptAll(gold_details).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

# Step 8: Final result
print("=" * 65)
print("STEP 9.7J — GOLD TRANSACTION DETAILS TRANSFORMATION")
print("=" * 65)

print("Silver detail rows:", 2301927)
print("Unique detail identities:", metrics["unique_details"])
print("Matched transactions:", metrics["transaction_matched"])
print("Unmatched transactions:", metrics["transaction_unmatched"])
print("Matched work orders:", metrics["workorder_matched"])
print("Unmatched work orders:", metrics["workorder_unmatched"])
print("Missing work-order references:", metrics["workorder_missing"])
print("Negative quantities:", metrics["negative_quantity"])
print("Negative prices:", metrics["negative_price"])
print("Zero quantities:", metrics["zero_quantity"])
print("Zero prices:", metrics["zero_price"])
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7J GOLD TRANSACTION DETAILS: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7K — Rebuild five categorical Gold dimensions automatically.
# Preserve original missing-value members, sorted keys, and source counts.
# Compare every result against existing Gold without modifying Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.assetfusion360"

# Production configurations from the original dimension scripts
configs = [
    {
        "target": "dim_workorder_type",
        "source": "fact_workorders",
        "column": "workorder_type",
        "key": "workorder_type_key",
        "count": "source_workorder_count",
        "missing": None,
        "flag": None,
    },
    {
        "target": "dim_workorder_priority",
        "source": "fact_workorders",
        "column": "workorder_priority",
        "key": "workorder_priority_key",
        "count": "source_workorder_count",
        "missing": "<MISSING_PRIORITY>",
        "flag": "is_missing_priority",
    },
    {
        "target": "dim_transaction_type",
        "source": "fact_transactions",
        "column": "transaction_type",
        "key": "transaction_type_key",
        "count": "source_transaction_count",
        "missing": "<MISSING_TRANSACTION_TYPE>",
        "flag": "is_missing_type",
    },
    {
        "target": "dim_transaction_status",
        "source": "fact_transactions",
        "column": "transaction_status",
        "key": "transaction_status_key",
        "count": "source_transaction_count",
        "missing": "<MISSING_TRANSACTION_STATUS>",
        "flag": "is_missing_status",
    },
    {
        "target": "dim_detail_type",
        "source": "fact_transaction_details",
        "column": "detail_type",
        "key": "detail_type_key",
        "count": "source_detail_count",
        "missing": "<MISSING_DETAIL_TYPE>",
        "flag": "is_missing_type",
    },
]

def rebuild_dimension(config):
    source = spark.table(f"{SCHEMA}.{config['source']}")
    existing = spark.table(f"{SCHEMA}.{config['target']}")

    column = config["column"]
    key = config["key"]
    count_name = config["count"]
    missing_label = config["missing"]
    flag_name = config["flag"]

    # Count source categories, including nulls
    counts = source.groupBy(column).agg(
        F.count("*").cast("long").alias(count_name)
    )

    if missing_label is None:
        assert counts.filter(F.col(column).isNull()).count() == 0

        result = counts.withColumn(
            key,
            F.row_number().over(Window.orderBy(column)).cast("int")
        )
    else:
        # Source values must not conflict with artificial missing labels
        assert counts.filter(F.col(column) == missing_label).count() == 0

        valid = (
            counts
            .filter(F.col(column).isNotNull())
            .withColumn(
                key,
                F.row_number().over(Window.orderBy(column)).cast("int")
            )
            .withColumn(flag_name, F.lit(False))
        )

        missing = (
            counts
            .filter(F.col(column).isNull())
            .select(
                F.lit(missing_label).alias(column),
                F.col(count_name),
                F.lit(0).cast("int").alias(key),
                F.lit(True).alias(flag_name),
            )
        )

        result = valid.unionByName(missing)

    # Preserve original Gold field order and data types
    result = result.select(*[
        F.col(name)
        .cast(existing.schema[name].dataType)
        .alias(name)
        for name in existing.columns
    ])

    # Compare every row and column against production
    forward = result.exceptAll(existing).count()
    reverse = existing.exceptAll(result).count()

    assert forward == 0 and reverse == 0, (
        f"{config['target']}: "
        f"{forward} forward, {reverse} reverse mismatches"
    )

    return (
        config["target"],
        result.count(),
        forward + reverse
    )

results = []

for config in configs:
    print(f"\nPROCESSING: {config['target']}")
    name, rows, mismatches = rebuild_dimension(config)

    print(f"ROWS: {rows:,}")
    print(f"MISMATCHES: {mismatches}")

    results.append((name, rows, mismatches))

assert len(results) == 5
assert all(mismatches == 0 for _, _, mismatches in results)

print("\n" + "=" * 65)
print("STEP 9.7K — AUTOMATED GOLD DIMENSION TRANSFORMATIONS")
print("=" * 65)

for name, rows, mismatches in results:
    print(f"{name:32} {rows:>8,} rows | {mismatches} mismatches")

print("\nSTEP 9.7K GOLD DIMENSION AUTOMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7L — Rebuild the production calendar from all four Gold fact tables.
# Generate continuous dates, calendar attributes, and ISO week information.
# Verify equivalence with existing dim_date without modifying Delta tables.

from pyspark.sql import functions as F

schema = "workspace.assetfusion360"

sources = {
    "fact_workorders": "workorder_date",
    "fact_labor": "labor_date",
    "fact_transactions": "transaction_date",
    "fact_transaction_details": "detail_date",
}

existing = spark.table(f"{schema}.dim_date")

# Step 1: Collect global date coverage from all four fact tables
coverage = []

for table_name, date_column in sources.items():
    source = spark.table(f"{schema}.{table_name}")

    bounds = source.select(
        F.min(F.to_date(F.col(date_column))).alias("first_date"),
        F.max(F.to_date(F.col(date_column))).alias("last_date")
    ).first()

    coverage.append(bounds)

first_date = min(row["first_date"] for row in coverage if row["first_date"])
last_date = max(row["last_date"] for row in coverage if row["last_date"])

# Step 2: Generate every calendar date in the original date range
calendar = spark.sql(f"""
    SELECT explode(
        sequence(
            DATE '{first_date}',
            DATE '{last_date}',
            INTERVAL 1 DAY
        )
    ) AS calendar_date
""")

# Step 3: Reproduce the original calendar attributes
iso_weekday = (
    (F.dayofweek("calendar_date") + F.lit(5)) % F.lit(7)
    + F.lit(1)
)

result = (
    calendar
    .withColumn(
        "date_key",
        F.date_format("calendar_date", "yyyyMMdd").cast("int")
    )
    .withColumn("year", F.year("calendar_date").cast("smallint"))
    .withColumn("quarter", F.quarter("calendar_date").cast("tinyint"))
    .withColumn("month", F.month("calendar_date").cast("tinyint"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("year_month", F.date_format("calendar_date", "yyyy-MM"))
    .withColumn("day_of_month", F.dayofmonth("calendar_date").cast("tinyint"))
    .withColumn("day_of_week", iso_weekday.cast("tinyint"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("iso_week", F.weekofyear("calendar_date").cast("tinyint"))
    .withColumn("is_weekend", iso_weekday >= 6)
)

# Step 4: Match original column order and data types
result = result.select(*[
    F.col(name).cast(existing.schema[name].dataType).alias(name)
    for name in existing.columns
])

# Step 5: Validate calendar continuity and production equivalence
expected_days = (last_date - first_date).days + 1

assert result.count() == expected_days
assert result.count() == 9356

forward = result.exceptAll(existing).count()
reverse = existing.exceptAll(result).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("=" * 65)
print("STEP 9.7L — GOLD DATE DIMENSION")
print("=" * 65)
print("First date:", first_date)
print("Last date:", last_date)
print("Calendar records:", result.count())
print("Expected continuous days:", expected_days)
print("Gold columns:", len(result.columns))
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7L DATE DIMENSION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7M — Rebuild the production Part dimension from transaction details.
# Preserve original part attributes, missing references, and surrogate keys.
# Verify complete equivalence without modifying existing Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

source = spark.table(
    "workspace.assetfusion360.fact_transaction_details"
)
existing = spark.table(
    "workspace.assetfusion360.dim_part"
)

attributes = [
    "part_identity",
    "part_description",
    "part_class",
    "part_uom"
]

# Step 1: Validate missing part references
missing = source.agg(
    F.count("*").alias("total"),
    F.sum(F.col("part_code").isNull().cast("long")).alias("missing_code"),
    F.sum(F.col("part_identity").isNull().cast("long")).alias("missing_id"),
    F.sum(
        (
            F.col("part_code").isNull()
            & F.col("part_identity").isNull()
        ).cast("long")
    ).alias("missing_both")
).first()

assert missing["total"] == 2301927
assert missing["missing_code"] == 27891
assert missing["missing_id"] == 27891
assert missing["missing_both"] == 27891

# Step 2: Keep only complete part relationships
valid = source.filter(
    F.col("part_code").isNotNull()
    & F.col("part_identity").isNotNull()
)

# Step 3: Confirm part attributes do not conflict
part_groups = (
    valid
    .groupBy("part_code")
    .agg(
        F.count("*").alias("source_detail_count"),
        F.countDistinct(
            F.struct(*[F.col(c) for c in attributes])
        ).alias("_attribute_versions"),
        *[
            F.first(c, ignorenulls=False).alias(c)
            for c in attributes
        ]
    )
)

assert part_groups.filter(
    F.col("_attribute_versions") != 1
).count() == 0

# Each source identity must refer to only one part code
identity_conflicts = (
    valid
    .groupBy("part_identity")
    .agg(
        F.countDistinct("part_code").alias("code_count")
    )
    .filter(F.col("code_count") != 1)
    .count()
)

assert identity_conflicts == 0

# Step 4: Assign deterministic surrogate keys
parts = (
    part_groups
    .drop("_attribute_versions")
    .withColumn(
        "part_key",
        F.row_number().over(
            Window.orderBy("part_code")
        ).cast("int")
    )
    .withColumn("is_missing_part", F.lit(False))
)

assert parts.count() == 14422

# Step 5: Create the explicit missing-part reporting member
missing_part = spark.createDataFrame(
    [(
        0,
        "<MISSING_PART_CODE>",
        None,
        "Missing part reference",
        None,
        None,
        True,
        27891
    )],
    """
    part_key INT,
    part_code STRING,
    part_identity LONG,
    part_description STRING,
    part_class STRING,
    part_uom STRING,
    is_missing_part BOOLEAN,
    source_detail_count LONG
    """
)

# Preserve production schema and column order
parts = parts.select(*existing.columns)
missing_part = missing_part.select(*existing.columns)

result = parts.unionByName(missing_part)

# Step 6: Validate the complete dimension
assert result.count() == 14423

assert result.agg(
    F.sum("source_detail_count")
).first()[0] == 2301927

forward = result.exceptAll(existing).count()
reverse = existing.exceptAll(result).count()

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("=" * 65)
print("STEP 9.7M — GOLD PART DIMENSION")
print("=" * 65)

print("Source detail records:", missing["total"])
print("Valid distinct parts:", 14422)
print("Missing part references:", missing["missing_both"])
print("Gold dimension records:", result.count())
print("Attribute conflicts:", 0)
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.7M PART DIMENSION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.7N — Reproduce all four monthly KPI transformations in PySpark.
# Preserve original grouping, missing categories, signed totals, and counts.
# Reconcile against production Gold; never overwrite existing Delta tables.

from pyspark.sql import functions as F

BASE = "workspace.assetfusion360"

def count_if(condition):
    return F.sum(
        F.when(condition, 1).otherwise(0)
    ).cast("long")

def month_of(column, allow_missing=False):
    value = F.date_format(F.col(column), "yyyy-MM")
    return (
        F.coalesce(value, F.lit("<MISSING_DATE>"))
        if allow_missing else value
    )

def missing_value(column, label):
    return F.coalesce(F.col(column), F.lit(label))

# 1. Work Order KPIs — month x work-order type
def build_workorder_kpis():
    df = spark.table(f"{BASE}.fact_workorders")

    assert df.filter(F.col("workorder_date").isNull()).count() == 0

    return (
        df.withColumn("reporting_month", month_of("workorder_date"))
        .groupBy("reporting_month", "workorder_type")
        .agg(
            F.count("*").alias("workorder_count"),
            count_if(F.col("completed_at").isNotNull()).alias(
                "completion_timestamp_present_count"
            ),
            count_if(F.col("completed_at").isNull()).alias(
                "completion_timestamp_missing_count"
            ),
            count_if(F.col("workorder_priority").isNotNull()).alias(
                "priority_present_count"
            ),
            count_if(F.col("workorder_priority").isNull()).alias(
                "priority_missing_count"
            ),
            count_if(F.col("asset_resolution_status") == "AMBIGUOUS").alias(
                "ambiguous_asset_mapping_count"
            ),
            count_if(F.col("asset_resolution_status") == "UNIQUE").alias(
                "unique_asset_mapping_count"
            )
        )
    )

# 2. Labor KPIs — month x work-order match status
def build_labor_kpis():
    df = spark.table(f"{BASE}.fact_labor")

    assert df.filter(
        F.col("labor_date").isNull()
        | F.col("labor_hours").isNull()
        | F.col("labor_cost").isNull()
        | F.col("labor_rate").isNull()
    ).count() == 0

    return (
        df.withColumn("reporting_month", month_of("labor_date"))
        .groupBy("reporting_month", "workorder_match_status")
        .agg(
            F.count("*").alias("labor_record_count"),
            F.sum("labor_hours").cast("decimal(30,6)").alias(
                "total_labor_hours"
            ),
            F.sum("labor_cost").cast("decimal(30,6)").alias(
                "total_labor_cost"
            ),
            count_if(F.col("labor_hours") < 0).alias(
                "negative_hours_count"
            ),
            count_if(F.col("labor_cost") < 0).alias(
                "negative_cost_count"
            ),
            count_if(F.col("labor_rate") == 0).alias(
                "zero_rate_count"
            )
        )
    )

# 3. Transaction KPIs — month x transaction type x status
def build_transaction_kpis():
    df = spark.table(f"{BASE}.fact_transactions")

    return (
        df.withColumn(
            "reporting_month",
            month_of("transaction_date", allow_missing=True)
        )
        .withColumn(
            "transaction_type",
            missing_value("transaction_type", "<MISSING_TYPE>")
        )
        .withColumn(
            "transaction_status",
            missing_value("transaction_status", "<MISSING_STATUS>")
        )
        .groupBy(
            "reporting_month", "transaction_type", "transaction_status"
        )
        .agg(
            F.count("*").alias("transaction_count"),
            count_if(F.col("transaction_date").isNull()).alias(
                "missing_date_count"
            ),
            count_if(F.col("dq_transaction_date_invalid") == True).alias(
                "invalid_date_flag_count"
            ),
            count_if(F.col("silver_dq_issue") == True).alias(
                "silver_dq_issue_count"
            )
        )
    )

# 4. Material KPIs — month x type x UOM x two match statuses
def build_material_kpis():
    df = spark.table(f"{BASE}.fact_transaction_details")

    return (
        df.withColumn(
            "reporting_month",
            month_of("detail_date", allow_missing=True)
        )
        .withColumn(
            "detail_type",
            missing_value("detail_type", "<MISSING_TYPE>")
        )
        .withColumn(
            "part_uom",
            missing_value("part_uom", "<MISSING_UOM>")
        )
        .withColumn(
            "transaction_match_status",
            missing_value(
                "transaction_match_status",
                "<MISSING_TRANSACTION_STATUS>"
            )
        )
        .withColumn(
            "workorder_match_status",
            missing_value(
                "workorder_match_status",
                "<MISSING_WORKORDER_STATUS>"
            )
        )
        .groupBy(
            "reporting_month",
            "detail_type",
            "part_uom",
            "transaction_match_status",
            "workorder_match_status"
        )
        .agg(
            F.count("*").alias("material_detail_count"),
            F.coalesce(
                F.sum("quantity"),
                F.lit(0).cast("decimal(30,6)")
            ).cast("decimal(30,6)").alias("total_signed_quantity"),
            count_if(F.col("quantity").isNotNull()).alias(
                "quantity_present_count"
            ),
            count_if(F.col("quantity").isNull()).alias(
                "quantity_missing_count"
            ),
            count_if(F.col("quantity") < 0).alias(
                "negative_quantity_count"
            ),
            count_if(F.col("quantity") == 0).alias(
                "zero_quantity_count"
            ),
            count_if(F.col("unit_price").isNotNull()).alias(
                "unit_price_present_count"
            ),
            count_if(F.col("unit_price").isNull()).alias(
                "unit_price_missing_count"
            ),
            count_if(F.col("unit_price") < 0).alias(
                "negative_unit_price_count"
            ),
            count_if(F.col("unit_price") == 0).alias(
                "zero_unit_price_count"
            ),
            count_if(F.col("average_price").isNull()).alias(
                "average_price_missing_count"
            )
        )
    )

# Run all four transformations and compare against production
pipelines = [
    ("gold_workorder_monthly_kpis", build_workorder_kpis, 1280,
     "workorder_count", 2231535),
    ("gold_labor_monthly_kpis", build_labor_kpis, 232,
     "labor_record_count", 3230423),
    ("gold_transaction_monthly_kpis", build_transaction_kpis, 1689,
     "transaction_count", 755122),
    ("gold_material_monthly_kpis", build_material_kpis, 12267,
     "material_detail_count", 2301927),
]

results = []

for name, builder, expected_rows, count_field, source_rows in pipelines:
    print(f"\nPROCESSING: {name}")

    existing = spark.table(f"{BASE}.{name}")
    transformed = builder()

    # Match the original production column order and types
    transformed = transformed.select(*[
        F.col(c).cast(existing.schema[c].dataType).alias(c)
        for c in existing.columns
    ])

    row_count = transformed.count()

    assert row_count == expected_rows, (
        f"{name}: expected {expected_rows}, found {row_count}"
    )

    reconciled = transformed.agg(
        F.sum(count_field)
    ).first()[0]

    assert reconciled == source_rows, (
        f"{name}: source records do not reconcile"
    )

    forward = transformed.exceptAll(existing).count()
    reverse = existing.exceptAll(transformed).count()

    assert forward == 0 and reverse == 0, (
        f"{name}: {forward} forward and {reverse} reverse mismatches"
    )

    print(f"ROWS: {row_count:,}")
    print(f"SOURCE RECORDS RECONCILED: {reconciled:,}")
    print("DATA MISMATCHES: 0")

    results.append((name, row_count))

print("\n" + "=" * 65)
print("STEP 9.7N — AUTOMATED GOLD MONTHLY KPI TRANSFORMATIONS")
print("=" * 65)

for name, rows in results:
    print(f"{name:35} {rows:>8,} rows | 0 mismatches")

assert len(results) == 4

print("\nSTEP 9.7N GOLD KPI AUTOMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")
