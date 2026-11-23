# Databricks notebook source

# Step 9.11H — Initialize the Gold staging transformation pipeline.
# Read five validated Silver staging tables and Gold reference schemas.
# Preserve all production and staging Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SILVER_TABLES = [
    "silver_amps_assets",
    "silver_amps_workorders",
    "silver_amps_labor",
    "silver_amps_transactions",
    "silver_amps_transaction_details"
]

silver = {}

for name in SILVER_TABLES:
    table = f"{STAGING}.{name}"

    assert spark.catalog.tableExists(table), (
        f"Missing Silver staging table: {table}"
    )

    silver[name] = spark.table(table)

print("=" * 65)
print("STEP 9.11H — GOLD STAGING INITIALIZATION")
print("=" * 65)

print("Production schema:", PRODUCTION)
print("Staging schema:", STAGING)
print("Available Silver staging tables:", len(silver))

for name in SILVER_TABLES:
    print("READY:", name)

print("\nGOLD STAGING INITIALIZATION: PASS")
print("EXISTING DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11I — Build Gold dim_asset from the validated Silver staging table.
# Preserve all 148,522 identities and ambiguous asset-code relationships.
# Verify against production without modifying existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

source = spark.table(
    f"{STAGING}.silver_amps_assets"
)

reference = spark.table(
    f"{PRODUCTION}.dim_asset"
)

# 1. Preserve original asset-code normalization
assets = (
    source
    .withColumn(
        "_asset_code",
        F.when(
            F.length(F.trim(F.col("OBJ_CODE"))) > 0,
            F.trim(F.col("OBJ_CODE"))
        )
    )
)

# 2. Count how many asset identities share each valid code
code_counts = (
    assets
    .filter(F.col("_asset_code").isNotNull())
    .groupBy("_asset_code")
    .agg(
        F.count("*").cast("int").alias(
            "_asset_code_match_count"
        )
    )
)

# 3. Join counts back without removing duplicate business codes
enriched = (
    assets.alias("a")
    .join(
        code_counts.alias("c"),
        F.col("a._asset_code") == F.col("c._asset_code"),
        "left"
    )
)

# 4. Apply the original Gold dimension mappings
gold_dim_asset_staged = enriched.select(
    F.col("a.asset_identity_int")
        .alias("asset_identity"),

    F.col("a._asset_code")
        .alias("asset_code"),

    F.col("a.OBJ_STATUS")
        .alias("asset_status"),

    F.col("a.OBJ_PARENT")
        .alias("parent_asset_code"),

    F.col("a.asset_commissioned_ts")
        .alias("commissioned_at"),

    F.col("a.asset_value_decimal")
        .alias("asset_value"),

    F.coalesce(
        F.col("c._asset_code_match_count"),
        F.lit(0)
    ).alias("asset_code_match_count"),

    (
        F.coalesce(
            F.col("c._asset_code_match_count"),
            F.lit(0)
        ) > 1
    ).alias("asset_code_ambiguous"),

    (
        F.col("a._asset_code").isNotNull()
        & F.upper(F.col("a._asset_code")).isin(
            "*", "N/A", "NA", "NULL", "UNKNOWN"
        )
    ).alias("asset_code_placeholder"),

    F.col("a._bronze_source_file")
        .alias("source_file")
)

# 5. Match the existing Gold schema exactly
gold_dim_asset_staged = gold_dim_asset_staged.select(
    *[
        F.col(name)
        .cast(reference.schema[name].dataType)
        .alias(name)
        for name in reference.columns
    ]
)

# 6. Validate source and dimension record counts
assert source.count() == 148522
assert gold_dim_asset_staged.count() == 148522

unique_identities = (
    gold_dim_asset_staged
    .select("asset_identity")
    .distinct()
    .count()
)

assert unique_identities == 148522

# 7. Confirm the original ambiguous asset code
ambiguous = (
    gold_dim_asset_staged
    .filter(F.col("asset_code_ambiguous"))
    .select("asset_code", "asset_identity")
    .orderBy("asset_identity")
    .collect()
)

assert len(ambiguous) == 2
assert {row["asset_code"] for row in ambiguous} == {"B223PA"}
assert [row["asset_identity"] for row in ambiguous] == [5514, 6133]

# 8. Verify column names, types and complete data
assert gold_dim_asset_staged.schema.names == reference.schema.names

assert all(
    gold_dim_asset_staged.schema[c].dataType
    == reference.schema[c].dataType
    for c in reference.columns
)

forward = gold_dim_asset_staged.exceptAll(reference).count()
reverse = reference.exceptAll(gold_dim_asset_staged).count()

print("=" * 65)
print("STEP 9.11I — GOLD ASSET DIMENSION FROM SILVER STAGING")
print("=" * 65)

print("Silver staging records:", source.count())
print("Gold dimension records:", gold_dim_asset_staged.count())
print("Gold columns:", len(gold_dim_asset_staged.columns))
print("Unique asset identities:", unique_identities)
print("Ambiguous asset code:", "B223PA")
print("Ambiguous asset identities:", [5514, 6133])
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11I GOLD DIM_ASSET: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11J — Publish the validated Gold Asset dimension to staging.
# Verify every published record against the existing Gold reference.
# Never overwrite staging or modify production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_asset"
REFERENCE = f"{PRODUCTION}.dim_asset"
EXPECTED_ROWS = 148522

reference = spark.table(REFERENCE)

# Reuse the validated Gold transformation from Cell 2
try:
    source = gold_dim_asset_staged
except NameError:
    raise RuntimeError(
        "Run Cell 2 — Step 9.11I before publishing dim_asset."
    )

# Validate the source before publication
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

assert all(
    source.schema[c].dataType == reference.schema[c].dataType
    for c in reference.columns
)

source_forward = source.exceptAll(reference).count()
source_reverse = reference.exceptAll(source).count()

assert source_forward == 0 and source_reverse == 0, (
    "Gold dim_asset transformation does not match reference."
)

print("GOLD DIM_ASSET TRANSFORMATION: VERIFIED")

# Create staging only if the table does not already exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD DIM_ASSET STAGING: NEWLY PUBLISHED")
else:
    print("GOLD DIM_ASSET STAGING: ALREADY EXISTS — VERIFYING")

# Independently validate the published staging Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

assert all(
    published.schema[c].dataType == reference.schema[c].dataType
    for c in reference.columns
)

forward = published.exceptAll(reference).count()
reverse = reference.exceptAll(published).count()

assert forward == 0 and reverse == 0, (
    f"Published data mismatches: {forward + reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11J — GOLD DIM_ASSET STAGING PUBLICATION")
print("=" * 65)

print("Source records:", source.count())
print("Published records:", published.count())
print("Published table:", TARGET)
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.11J GOLD DIM_ASSET STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11K — Build the asset-code bridge from staged Gold dim_asset.
# Preserve ambiguous identities and compare all 148,521 bridge records.
# Publish only to staging; never overwrite production Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SOURCE = f"{STAGING}.dim_asset"
REFERENCE = f"{PRODUCTION}.bridge_asset_code"
TARGET = f"{STAGING}.bridge_asset_code"

assets = spark.table(SOURCE)
reference = spark.table(REFERENCE)

# 1. Build the bridge using the original business rules
bridge_staged = (
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

# 2. Validate record counts and schema
assert assets.count() == 148522
assert bridge_staged.count() == 148521

assert bridge_staged.schema.names == reference.schema.names

assert all(
    bridge_staged.schema[c].dataType == reference.schema[c].dataType
    for c in reference.columns
)

# 3. Preserve the known ambiguous asset-code relationship
ambiguous = (
    bridge_staged
    .filter(F.col("is_ambiguous"))
    .collect()
)

assert len(ambiguous) == 1
assert ambiguous[0]["asset_code"] == "B223PA"
assert ambiguous[0]["candidate_asset_identities"] == [5514, 6133]
assert ambiguous[0]["resolved_asset_identity"] is None

# 4. Compare the complete bridge against production
source_forward = bridge_staged.exceptAll(reference).count()
source_reverse = reference.exceptAll(bridge_staged).count()

assert source_forward == 0 and source_reverse == 0, (
    f"Bridge transformation mismatches: "
    f"{source_forward + source_reverse}"
)

print("ASSET-CODE BRIDGE TRANSFORMATION: VERIFIED")

# 5. Publish to staging only if the table does not exist
if not spark.catalog.tableExists(TARGET):
    bridge_staged.write.format("delta").saveAsTable(TARGET)
    print("BRIDGE STAGING: NEWLY PUBLISHED")
else:
    print("BRIDGE STAGING: ALREADY EXISTS — VERIFYING")

# 6. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == 148521
assert published.schema.names == reference.schema.names

forward = published.exceptAll(reference).count()
reverse = reference.exceptAll(published).count()

assert forward == 0 and reverse == 0, (
    f"Published bridge mismatches: {forward + reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11K — GOLD ASSET-CODE BRIDGE STAGING")
print("=" * 65)

print("Source asset records:", assets.count())
print("Published bridge records:", published.count())
print("Ambiguous asset codes:", len(ambiguous))
print("B223PA candidate identities:", [5514, 6133])
print("Published table:", TARGET)
print("Data mismatches:", forward + reverse)

print("\nSTEP 9.11K GOLD BRIDGE STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11K-A — Diagnose the Gold asset-code bridge assertion.
# Check row counts, column names, data types and ambiguous codes.
# Read-only; do not modify or overwrite Delta tables.

from pyspark.sql import functions as F

print("=" * 65)
print("STEP 9.11K-A — ASSET BRIDGE ERROR DIAGNOSIS")
print("=" * 65)

# 1. Check input and transformed row counts
print("\n1. ROW COUNTS")

print("Staged dim_asset:", assets.count())
print("Transformed bridge:", bridge_staged.count())
print("Production bridge:", reference.count())

# 2. Check column names
print("\n2. COLUMN COMPARISON")

print("Transformed columns:", bridge_staged.columns)
print("Production columns:", reference.columns)
print("Column names match:",
      bridge_staged.columns == reference.columns)

# 3. Check exact data types
print("\n3. DATA TYPE COMPARISON")

for name in reference.columns:
    staged_type = (
        str(bridge_staged.schema[name].dataType)
        if name in bridge_staged.columns
        else "MISSING"
    )

    reference_type = str(reference.schema[name].dataType)

    status = (
        "PASS"
        if staged_type == reference_type
        else "MISMATCH"
    )

    print(
        f"{name}: "
        f"staging={staged_type}, "
        f"production={reference_type} "
        f"[{status}]"
    )

# 4. Check ambiguous asset relationships
print("\n4. AMBIGUOUS ASSET CODES")

ambiguous_rows = (
    bridge_staged
    .filter(F.col("is_ambiguous"))
    .collect()
)

print("Ambiguous codes:", len(ambiguous_rows))

for row in ambiguous_rows:
    print(row.asDict())

# 5. Check publication status
print("\n5. STAGING PUBLICATION STATUS")

target = "workspace.assetfusion360_staging.bridge_asset_code"

print("Staging table exists:",
      spark.catalog.tableExists(target))

print("\nDIAGNOSTIC COMPLETE")
print("EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11K-B — Correct the bridge array element nullability.
# Validate all bridge records and publish to Gold staging.
# Preserve production and existing staging tables without overwriting.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

REFERENCE = f"{PRODUCTION}.bridge_asset_code"
TARGET = f"{STAGING}.bridge_asset_code"

reference = spark.table(REFERENCE)

# 1. Reconstruct the array using the production array schema.
# This changes the Spark schema metadata, not the array values.
array_type = reference.schema[
    "candidate_asset_identities"
].dataType

bridge_corrected = bridge_staged.withColumn(
    "candidate_asset_identities",
    F.from_json(
        F.to_json(F.col("candidate_asset_identities")),
        array_type
    )
)

# 2. Verify exact production schema
assert bridge_corrected.schema.names == reference.schema.names

for name in reference.columns:
    actual = bridge_corrected.schema[name].dataType
    expected = reference.schema[name].dataType

    assert actual == expected, (
        f"Schema mismatch for {name}: "
        f"{actual} != {expected}"
    )

print("BRIDGE SCHEMA: PASS")

# 3. Validate row count and known ambiguous asset identities
assert bridge_corrected.count() == 148521

ambiguous = (
    bridge_corrected
    .filter(F.col("is_ambiguous"))
    .collect()
)

assert len(ambiguous) == 1
assert ambiguous[0]["asset_code"] == "B223PA"
assert ambiguous[0]["candidate_asset_identities"] == [5514, 6133]
assert ambiguous[0]["resolved_asset_identity"] is None

# 4. Compare all transformed records with production
forward = bridge_corrected.exceptAll(reference).count()
reverse = reference.exceptAll(bridge_corrected).count()

assert forward == 0 and reverse == 0, (
    f"Transformation mismatches: {forward + reverse}"
)

print("BRIDGE DATA VALIDATION: PASS")

# 5. Publish only when the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    bridge_corrected.write.format("delta").saveAsTable(TARGET)
    print("BRIDGE STAGING: NEWLY PUBLISHED")
else:
    print("BRIDGE STAGING: ALREADY EXISTS — VERIFYING")

# 6. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == 148521
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0
assert published_reverse == 0

print("\n" + "=" * 65)
print("STEP 9.11K-B — GOLD ASSET BRIDGE STAGING")
print("=" * 65)

print("Source asset records:", 148522)
print("Published bridge records:", published.count())
print("Gold columns:", len(published.columns))
print("Ambiguous asset codes:", len(ambiguous))
print("B223PA candidate identities:", [5514, 6133])
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11K-B GOLD BRIDGE STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11L — Rebuild Gold Work Orders from staged Silver and Gold bridge.
# Preserve all 2,231,535 work orders and ambiguous asset relationships.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

silver_wo = spark.table(
    f"{STAGING}.silver_amps_workorders"
)

asset_bridge = spark.table(
    f"{STAGING}.bridge_asset_code"
)

reference_wo = spark.table(
    f"{PRODUCTION}.fact_workorders"
)

# 1. Original Silver-to-Gold field mappings
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
    "source_file": "_bronze_source_file"
}

assert all(
    name in silver_wo.columns
    for name in mapping.values()
), "Missing required Silver Work Orders columns"

assert silver_wo.count() == 2231535
assert asset_bridge.count() == 148521

# 2. Join Work Orders to the staged asset-code bridge
joined = (
    silver_wo.alias("w")
    .join(
        asset_bridge.alias("b"),
        F.col("w.evt_object") == F.col("b.asset_code"),
        "left"
    )
)

# 3. Create the 23-column production Gold structure
derived = {
    "resolved_asset_identity":
        F.col("b.resolved_asset_identity"),

    "asset_resolution_status":
        F.col("b.resolution_status"),

    "asset_reference_ambiguous":
        F.col("b.is_ambiguous"),

    "asset_code_placeholder":
        F.col("b.is_placeholder")
}

gold_fact_workorders_staged = joined.select(
    *[
        (
            derived[name]
            if name in derived
            else F.col(f"w.{mapping[name]}")
        )
        .cast(reference_wo.schema[name].dataType)
        .alias(name)
        for name in reference_wo.columns
    ]
)

# 4. Validate transformation results
assert gold_fact_workorders_staged.count() == 2231535
assert len(gold_fact_workorders_staged.columns) == 23

# 5. Check asset mapping and data-quality statistics
stats = gold_fact_workorders_staged.agg(
    F.sum(
        F.when(
            F.col("asset_resolution_status") == "UNIQUE", 1
        ).otherwise(0)
    ).alias("unique_assets"),

    F.sum(
        F.when(
            F.col("asset_resolution_status") == "AMBIGUOUS", 1
        ).otherwise(0)
    ).alias("ambiguous_assets"),

    F.sum(
        F.col("asset_resolution_status").isNull().cast("int")
    ).alias("unmatched_assets"),

    F.sum(
        F.col("workorder_priority").isNull().cast("int")
    ).alias("missing_priority"),

    F.sum(
        F.col("completed_at").isNull().cast("int")
    ).alias("missing_completion"),

    F.sum(
        F.when(F.col("silver_dq_issue") == True, 1)
         .otherwise(0)
    ).alias("dq_issues")
).first()

assert stats["unique_assets"] == 2231135
assert stats["ambiguous_assets"] == 400
assert stats["unmatched_assets"] == 0
assert stats["missing_priority"] == 994238
assert stats["missing_completion"] == 25585
assert stats["dq_issues"] == 0

# 6. Compare all records against the existing Gold table
forward = (
    gold_fact_workorders_staged
    .exceptAll(reference_wo)
    .count()
)

reverse = (
    reference_wo
    .exceptAll(gold_fact_workorders_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11L — GOLD WORK ORDERS TRANSFORMATION")
print("=" * 65)

print("Silver Work Orders:", silver_wo.count())
print("Gold transformed rows:", gold_fact_workorders_staged.count())
print("Gold columns:", len(gold_fact_workorders_staged.columns))
print("Unique asset mappings:", stats["unique_assets"])
print("Ambiguous asset mappings:", stats["ambiguous_assets"])
print("Unmatched asset mappings:", stats["unmatched_assets"])
print("Missing priorities:", stats["missing_priority"])
print("Missing completion timestamps:", stats["missing_completion"])
print("Data-quality issues:", stats["dq_issues"])
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11L GOLD WORK ORDERS: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11M — Publish validated Gold Work Orders to staging.
# Verify all 2,231,535 records against the production Gold reference.
# Never overwrite staging tables or modify production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.fact_workorders"
REFERENCE = f"{PRODUCTION}.fact_workorders"
EXPECTED_ROWS = 2231535

reference = spark.table(REFERENCE)

# 1. Reuse the validated transformation from Step 9.11L
try:
    source = gold_fact_workorders_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11L before publishing fact_workorders."
    )

# 2. Validate before publishing
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD WORK ORDERS TRANSFORMATION: VERIFIED")

# 3. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("WORK ORDERS STAGING: NEWLY PUBLISHED")
else:
    print("WORK ORDERS STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11M — GOLD WORK ORDERS STAGING PUBLICATION")
print("=" * 65)

print("Source records:", source.count())
print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11M GOLD WORK ORDERS STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11N — Rebuild Gold Labor using staged Silver and Work Orders.
# Preserve all 3,230,423 labor records and unmatched references.
# Validate against production without changing existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

silver_labor = spark.table(
    f"{STAGING}.silver_amps_labor"
)

workorders = spark.table(
    f"{STAGING}.fact_workorders"
)

reference_labor = spark.table(
    f"{PRODUCTION}.fact_labor"
)

EXPECTED_ROWS = 3230423

# 1. Original production mappings
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
    "source_batch": "_bronze_batch_id"
}

assert all(
    c in silver_labor.columns
    for c in mapping.values()
)

assert silver_labor.count() == EXPECTED_ROWS
assert workorders.count() == 2231535

# 2. Build a unique Work Order lookup
wo_lookup = workorders.select(
    "workorder_code",
    "workorder_identity"
)

assert (
    wo_lookup
    .filter(
        F.col("workorder_code").isNull()
        | F.col("workorder_identity").isNull()
    )
    .count()
    == 0
)

assert (
    wo_lookup.select("workorder_code").distinct().count()
    == 2231535
)

# 3. Resolve Labor work-order references
joined = (
    silver_labor.alias("l")
    .join(
        wo_lookup.alias("w"),
        F.col("l.boo_event") == F.col("w.workorder_code"),
        "left"
    )
)

# 4. Preserve original matching rules
derived = {
    "workorder_identity":
        F.col("w.workorder_identity"),

    "workorder_match_status":
        F.when(
            F.col("l.boo_event").isNull()
            | (F.length(F.trim(F.col("l.boo_event"))) == 0),
            F.lit("MISSING_REFERENCE")
        )
        .when(
            F.col("w.workorder_identity").isNotNull(),
            F.lit("MATCHED")
        )
        .otherwise(F.lit("UNMATCHED"))
}

# 5. Reproduce the production Gold schema
gold_fact_labor_staged = joined.select(
    *[
        (
            derived[name]
            if name in derived
            else F.col(f"l.{mapping[name]}")
        )
        .cast(reference_labor.schema[name].dataType)
        .alias(name)
        for name in reference_labor.columns
    ]
)

assert gold_fact_labor_staged.count() == EXPECTED_ROWS
assert len(gold_fact_labor_staged.columns) == 20

# 6. Validate original business rules
stats = gold_fact_labor_staged.agg(
    F.sum(
        F.when(
            F.col("workorder_match_status") == "MATCHED", 1
        ).otherwise(0)
    ).alias("matched"),

    F.sum(
        F.when(
            F.col("workorder_match_status") == "UNMATCHED", 1
        ).otherwise(0)
    ).alias("unmatched"),

    F.sum(
        F.when(
            F.col("workorder_match_status")
            == "MISSING_REFERENCE", 1
        ).otherwise(0)
    ).alias("missing_reference"),

    F.sum(
        F.when(F.col("negative_hours_flag") == True, 1)
        .otherwise(0)
    ).alias("negative_hours"),

    F.sum(
        F.when(F.col("negative_cost_flag") == True, 1)
        .otherwise(0)
    ).alias("negative_cost"),

    F.sum(
        F.when(F.col("zero_rate_flag") == True, 1)
        .otherwise(0)
    ).alias("zero_rate"),

    F.sum(
        F.when(F.col("silver_dq_issue") == True, 1)
        .otherwise(0)
    ).alias("dq_issues")
).first()

assert stats["matched"] == 3217345
assert stats["unmatched"] == 13078
assert stats["missing_reference"] == 0
assert stats["negative_hours"] == 74609
assert stats["negative_cost"] == 71374
assert stats["zero_rate"] == 188165
assert stats["dq_issues"] == 0

# 7. Verify all column names and types
assert gold_fact_labor_staged.schema.names == (
    reference_labor.schema.names
)

for name in reference_labor.columns:
    assert (
        gold_fact_labor_staged.schema[name].dataType
        == reference_labor.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 8. Compare every record against production
forward = (
    gold_fact_labor_staged
    .exceptAll(reference_labor)
    .count()
)

reverse = (
    reference_labor
    .exceptAll(gold_fact_labor_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11N — GOLD LABOR TRANSFORMATION")
print("=" * 65)

print("Silver Labor records:", silver_labor.count())
print("Gold transformed records:", gold_fact_labor_staged.count())
print("Gold columns:", len(gold_fact_labor_staged.columns))
print("Matched Work Orders:", stats["matched"])
print("Unmatched Work Orders:", stats["unmatched"])
print("Missing references:", stats["missing_reference"])
print("Negative hours:", stats["negative_hours"])
print("Negative costs:", stats["negative_cost"])
print("Zero rates:", stats["zero_rate"])
print("Data-quality issues:", stats["dq_issues"])
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11N GOLD LABOR TRANSFORMATION: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11O — Publish validated Gold Labor into staging.
# Verify all 3,230,423 records against the production Gold reference.
# Never overwrite existing tables or modify production data.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.fact_labor"
REFERENCE = f"{PRODUCTION}.fact_labor"
EXPECTED_ROWS = 3230423

reference = spark.table(REFERENCE)

# 1. Retrieve the validated transformation from Step 9.11N
try:
    source = gold_fact_labor_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11N before publishing fact_labor."
    )

# 2. Verify source records and schema
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD LABOR TRANSFORMATION: VERIFIED")

# 3. Publish only when staging does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD LABOR STAGING: NEWLY PUBLISHED")
else:
    print("GOLD LABOR STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify published Delta records
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11O — GOLD LABOR STAGING PUBLICATION")
print("=" * 65)

print("Source records:", source.count())
print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11O GOLD LABOR STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11P — Rebuild Gold Transactions from staged Silver data.
# Preserve all 755,122 transaction records and data-quality indicators.
# Compare against production without modifying existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

silver_transactions = spark.table(
    f"{STAGING}.silver_amps_transactions"
)

reference_transactions = spark.table(
    f"{PRODUCTION}.fact_transactions"
)

# 1. Reuse the verified transaction field mappings
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
    "source_ingested_at": "_bronze_ingested_at_utc"
}

assert set(mapping) == set(reference_transactions.columns)

assert all(
    c in silver_transactions.columns
    for c in mapping.values()
)

# 2. Build the production-equivalent Gold transaction structure
gold_fact_transactions_staged = silver_transactions.select(
    *[
        F.col(mapping[name])
        .cast(reference_transactions.schema[name].dataType)
        .alias(name)
        for name in reference_transactions.columns
    ]
)

# 3. Validate row count and unique transaction keys
assert silver_transactions.count() == 755122
assert gold_fact_transactions_staged.count() == 755122

keys = gold_fact_transactions_staged.agg(
    F.countDistinct("transaction_identity")
        .alias("identities"),

    F.countDistinct("transaction_code")
        .alias("codes"),

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

# 4. Verify the data-quality indicators
quality_columns = [
    c for c in gold_fact_transactions_staged.columns
    if c.startswith("dq_") or c == "silver_dq_issue"
]

quality = gold_fact_transactions_staged.agg(
    *[
        F.sum(
            F.when(F.col(c) == True, 1).otherwise(0)
        ).alias(c)
        for c in quality_columns
    ]
).first()

assert all(
    quality[c] == 0
    for c in quality_columns
)

# 5. Validate the production Gold schema
assert (
    gold_fact_transactions_staged.schema.names
    == reference_transactions.schema.names
)

for name in reference_transactions.columns:
    assert (
        gold_fact_transactions_staged.schema[name].dataType
        == reference_transactions.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 6. Compare every transformed record against production
forward = (
    gold_fact_transactions_staged
    .exceptAll(reference_transactions)
    .count()
)

reverse = (
    reference_transactions
    .exceptAll(gold_fact_transactions_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11P — GOLD TRANSACTIONS TRANSFORMATION")
print("=" * 65)

print("Silver source records:", silver_transactions.count())
print("Gold transformed records:", gold_fact_transactions_staged.count())
print("Gold columns:", len(gold_fact_transactions_staged.columns))
print("Unique transaction identities:", keys["identities"])
print("Unique transaction codes:", keys["codes"])
print("Data-quality flags raised:", sum(
    quality[c] for c in quality_columns
))
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11P GOLD TRANSACTIONS: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11Q — Publish validated Gold Transactions to staging.
# Verify all 755,122 records against the production Gold reference.
# Never overwrite existing tables or modify production Delta data.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.fact_transactions"
REFERENCE = f"{PRODUCTION}.fact_transactions"
EXPECTED_ROWS = 755122

reference = spark.table(REFERENCE)

# 1. Use the validated transformation from Step 9.11P
try:
    source = gold_fact_transactions_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11P before publishing fact_transactions."
    )

# 2. Validate the source before publication
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD TRANSACTIONS TRANSFORMATION: VERIFIED")

# 3. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD TRANSACTIONS STAGING: NEWLY PUBLISHED")
else:
    print("GOLD TRANSACTIONS STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11Q — GOLD TRANSACTIONS STAGING PUBLICATION")
print("=" * 65)

print("Source records:", source.count())
print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11Q GOLD TRANSACTIONS STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11R — Rebuild Gold Transaction Details from staged data.
# Preserve transaction/work-order relationships and all 2,301,927 records.
# Validate against production without modifying any Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

silver_details = spark.table(
    f"{STAGING}.silver_amps_transaction_details"
)

transactions = spark.table(
    f"{STAGING}.fact_transactions"
)

workorders = spark.table(
    f"{STAGING}.fact_workorders"
)

reference_details = spark.table(
    f"{PRODUCTION}.fact_transaction_details"
)

EXPECTED_ROWS = 2301927

# 1. Original Silver-to-Gold field mappings
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
    "source_batch": "_bronze_batch_id"
}

assert all(
    c in silver_details.columns
    for c in mapping.values()
), "Missing Silver Transaction Details columns"

assert silver_details.count() == EXPECTED_ROWS
assert transactions.count() == 755122
assert workorders.count() == 2231535

# 2. Build transaction and work-order lookups
transaction_lookup = transactions.select(
    F.col("transaction_code").alias("_transaction_code"),
    F.col("transaction_identity").alias("_transaction_identity")
)

workorder_lookup = workorders.select(
    F.col("workorder_code").alias("_workorder_code"),
    F.col("workorder_identity").alias("_workorder_identity")
)

# Verify that joins cannot duplicate detail records
assert (
    transaction_lookup
    .select("_transaction_code")
    .distinct()
    .count()
    == 755122
)

assert (
    workorder_lookup
    .select("_workorder_code")
    .distinct()
    .count()
    == 2231535
)

# 3. Resolve both relationships from staged Gold facts
joined = (
    silver_details.alias("d")
    .join(
        transaction_lookup.alias("t"),
        F.col("d.trl_trans") == F.col("t._transaction_code"),
        "left"
    )
    .join(
        workorder_lookup.alias("w"),
        F.col("d.trl_event") == F.col("w._workorder_code"),
        "left"
    )
)

# 4. Preserve the original relationship-status rules
derived = {
    "transaction_identity":
        F.col("t._transaction_identity"),

    "transaction_match_status":
        F.when(
            F.col("d.trl_trans").isNull()
            | (F.length(F.trim(F.col("d.trl_trans"))) == 0),
            F.lit("MISSING_REFERENCE")
        )
        .when(
            F.col("t._transaction_identity").isNotNull(),
            F.lit("MATCHED")
        )
        .otherwise(F.lit("UNMATCHED")),

    "workorder_identity":
        F.col("w._workorder_identity"),

    "workorder_match_status":
        F.when(
            F.col("d.trl_event").isNull()
            | (F.length(F.trim(F.col("d.trl_event"))) == 0),
            F.lit("MISSING_REFERENCE")
        )
        .when(
            F.col("w._workorder_identity").isNotNull(),
            F.lit("MATCHED")
        )
        .otherwise(F.lit("UNMATCHED"))
}

# 5. Reproduce the exact production Gold column structure
gold_fact_transaction_details_staged = joined.select(
    *[
        (
            derived[name]
            if name in derived
            else F.col(f"d.{mapping[name]}")
        )
        .cast(reference_details.schema[name].dataType)
        .alias(name)
        for name in reference_details.columns
    ]
)

# 6. Verify row count, identity uniqueness and schema
assert gold_fact_transaction_details_staged.count() == EXPECTED_ROWS
assert len(gold_fact_transaction_details_staged.columns) == 31

unique_details = (
    gold_fact_transaction_details_staged
    .select("detail_identity")
    .distinct()
    .count()
)

assert unique_details == EXPECTED_ROWS

assert (
    gold_fact_transaction_details_staged.schema.names
    == reference_details.schema.names
)

for name in reference_details.columns:
    assert (
        gold_fact_transaction_details_staged.schema[name].dataType
        == reference_details.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 7. Validate relationship matches and quality indicators
checks = {
    "transaction_matched": (
        F.col("transaction_match_status") == "MATCHED"
    ),
    "transaction_unmatched": (
        F.col("transaction_match_status") == "UNMATCHED"
    ),
    "transaction_missing": (
        F.col("transaction_match_status") == "MISSING_REFERENCE"
    ),
    "workorder_matched": (
        F.col("workorder_match_status") == "MATCHED"
    ),
    "workorder_unmatched": (
        F.col("workorder_match_status") == "UNMATCHED"
    ),
    "workorder_missing": (
        F.col("workorder_match_status") == "MISSING_REFERENCE"
    ),
    "negative_qty": F.col("negative_quantity_flag") == True,
    "negative_price": F.col("negative_price_flag") == True,
    "zero_qty": F.col("zero_quantity_flag") == True,
    "zero_price": F.col("zero_price_flag") == True,
    "dq_issues": F.col("silver_dq_issue") == True
}

stats = gold_fact_transaction_details_staged.agg(
    *[
        F.sum(
            F.when(condition, 1).otherwise(0)
        ).alias(name)
        for name, condition in checks.items()
    ]
).first()

expected = {
    "transaction_matched": 2300536,
    "transaction_unmatched": 1391,
    "transaction_missing": 0,
    "workorder_matched": 809009,
    "workorder_unmatched": 135,
    "workorder_missing": 1492783,
    "negative_qty": 44053,
    "negative_price": 5501,
    "zero_qty": 164449,
    "zero_price": 61515,
    "dq_issues": 0
}

for name, expected_value in expected.items():
    assert stats[name] == expected_value, (
        f"{name}: {stats[name]} != {expected_value}"
    )

# 8. Compare every record against production Gold
forward = (
    gold_fact_transaction_details_staged
    .exceptAll(reference_details)
    .count()
)

reverse = (
    reference_details
    .exceptAll(gold_fact_transaction_details_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11R — GOLD TRANSACTION DETAILS TRANSFORMATION")
print("=" * 65)

print("Silver source records:", silver_details.count())
print("Gold transformed records:",
      gold_fact_transaction_details_staged.count())
print("Gold columns:", len(gold_fact_transaction_details_staged.columns))
print("Unique detail identities:", unique_details)

for name in expected:
    print(f"{name}: {stats[name]}")

print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11R GOLD TRANSACTION DETAILS: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11S — Publish validated Gold Transaction Details to staging.
# Verify all 2,301,927 records and the exact production schema.
# Never overwrite existing tables or modify production Delta data.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.fact_transaction_details"
REFERENCE = f"{PRODUCTION}.fact_transaction_details"
EXPECTED_ROWS = 2301927

reference = spark.table(REFERENCE)

# 1. Reuse the validated transformation from Step 9.11R
try:
    source = gold_fact_transaction_details_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11R before publishing fact_transaction_details."
    )

# 2. Verify source records and schema
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

# 3. Verify all source records against production
forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD TRANSACTION DETAILS TRANSFORMATION: VERIFIED")

# 4. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD TRANSACTION DETAILS STAGING: NEWLY PUBLISHED")
else:
    print("GOLD TRANSACTION DETAILS STAGING: ALREADY EXISTS — VERIFYING")

# 5. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11S — GOLD TRANSACTION DETAILS STAGING PUBLICATION")
print("=" * 65)

print("Source records:", source.count())
print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11S GOLD TRANSACTION DETAILS STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11T — Rebuild the Work Order Type dimension from staged Gold.
# Preserve seven business types and their original dimension keys.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

source = spark.table(
    f"{STAGING}.fact_workorders"
)

reference = spark.table(
    f"{PRODUCTION}.dim_workorder_type"
)

# 1. Count work orders by business type
type_counts = (
    source
    .groupBy("workorder_type")
    .agg(
        F.count("*").cast("long").alias(
            "source_workorder_count"
        )
    )
)

# 2. Assign deterministic dimension keys
key_window = Window.orderBy("workorder_type")

gold_dim_workorder_type_staged = (
    type_counts
    .withColumn(
        "workorder_type_key",
        F.row_number().over(key_window).cast("int")
    )
    .select(
        "workorder_type_key",
        "workorder_type",
        "source_workorder_count"
    )
)

# 3. Validate source and dimension counts
assert source.count() == 2231535
assert gold_dim_workorder_type_staged.count() == 7

assert (
    gold_dim_workorder_type_staged
    .filter(F.col("workorder_type").isNull())
    .count()
    == 0
)

assert (
    gold_dim_workorder_type_staged
    .agg(F.sum("source_workorder_count"))
    .first()[0]
    == 2231535
)

# 4. Validate the original type distribution
expected = {
    "JOB": 1848051,
    "IN": 108258,
    "PPM": 101154,
    "AA": 100622,
    "STAT": 43219,
    "MRC": 15136,
    "XA": 15095
}

actual = {
    row["workorder_type"]: row["source_workorder_count"]
    for row in gold_dim_workorder_type_staged.collect()
}

assert actual == expected, (
    f"Work Order Type distribution mismatch: {actual}"
)

# 5. Verify exact production schema
assert (
    gold_dim_workorder_type_staged.schema.names
    == reference.schema.names
)

for name in reference.columns:
    assert (
        gold_dim_workorder_type_staged.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 6. Compare every dimension record against production
forward = (
    gold_dim_workorder_type_staged
    .exceptAll(reference)
    .count()
)

reverse = (
    reference
    .exceptAll(gold_dim_workorder_type_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11T — GOLD WORK ORDER TYPE DIMENSION")
print("=" * 65)

print("Source Work Orders:", source.count())
print("Dimension records:",
      gold_dim_workorder_type_staged.count())
print("Gold columns:",
      len(gold_dim_workorder_type_staged.columns))

print("\nWORK ORDER TYPE DISTRIBUTION")

gold_dim_workorder_type_staged.orderBy(
    "workorder_type_key"
).show(7, truncate=False)

print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11T GOLD DIM_WORKORDER_TYPE: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11U — Publish the validated Work Order Type dimension.
# Verify seven records against the production Gold reference.
# Never overwrite existing staging or production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_workorder_type"
REFERENCE = f"{PRODUCTION}.dim_workorder_type"
EXPECTED_ROWS = 7

reference = spark.table(REFERENCE)

# 1. Reuse the validated dimension transformation
try:
    source = gold_dim_workorder_type_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11T before publishing dim_workorder_type."
    )

# 2. Verify schema and data before publication
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("WORK ORDER TYPE DIMENSION: VERIFIED")

# 3. Publish only if the target does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("WORK ORDER TYPE STAGING: NEWLY PUBLISHED")
else:
    print("WORK ORDER TYPE STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11U — WORK ORDER TYPE STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11U GOLD DIM_WORKORDER_TYPE STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11V — Rebuild Work Order Priority dimension from staged Gold.
# Preserve the missing-priority member and deterministic dimension keys.
# Validate all records against production without modifying Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

workorders = spark.table(
    f"{STAGING}.fact_workorders"
)

reference_priority = spark.table(
    f"{PRODUCTION}.dim_workorder_priority"
)

MISSING_LABEL = "<MISSING_PRIORITY>"

# 1. Count all work orders by priority, including NULL
priority_counts = (
    workorders
    .groupBy("workorder_priority")
    .agg(
        F.count("*").cast("long").alias(
            "source_workorder_count"
        )
    )
)

# 2. Preserve the missing-priority reporting member
missing_priorities = (
    priority_counts
    .filter(F.col("workorder_priority").isNull())
    .select(
        F.lit(0).cast("int").alias("workorder_priority_key"),
        F.lit(MISSING_LABEL).alias("workorder_priority"),
        F.lit(True).alias("is_missing_priority"),
        "source_workorder_count"
    )
)

# 3. Assign keys to actual priority values in alphabetical order
priority_window = Window.orderBy("workorder_priority")

actual_priorities = (
    priority_counts
    .filter(F.col("workorder_priority").isNotNull())
    .withColumn(
        "workorder_priority_key",
        F.row_number().over(priority_window).cast("int")
    )
    .select(
        "workorder_priority_key",
        "workorder_priority",
        F.lit(False).alias("is_missing_priority"),
        "source_workorder_count"
    )
)

# 4. Combine missing and actual priority members
gold_dim_workorder_priority_staged = (
    missing_priorities
    .unionByName(actual_priorities)
    .select(
        *[
            F.col(name)
            .cast(reference_priority.schema[name].dataType)
            .alias(name)
            for name in reference_priority.columns
        ]
    )
)

# 5. Validate business rules
assert workorders.count() == 2231535
assert gold_dim_workorder_priority_staged.count() == 11

missing = (
    gold_dim_workorder_priority_staged
    .filter(F.col("is_missing_priority"))
    .collect()
)

assert len(missing) == 1
assert missing[0]["workorder_priority_key"] == 0
assert missing[0]["workorder_priority"] == MISSING_LABEL
assert missing[0]["source_workorder_count"] == 994238

assert (
    gold_dim_workorder_priority_staged
    .agg(F.sum("source_workorder_count"))
    .first()[0]
    == 2231535
)

assert (
    gold_dim_workorder_priority_staged
    .select("workorder_priority_key")
    .distinct()
    .count()
    == 11
)

# 6. Verify production schema
assert (
    gold_dim_workorder_priority_staged.schema.names
    == reference_priority.schema.names
)

for name in reference_priority.columns:
    assert (
        gold_dim_workorder_priority_staged.schema[name].dataType
        == reference_priority.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 7. Reconcile all dimension records
forward = (
    gold_dim_workorder_priority_staged
    .exceptAll(reference_priority)
    .count()
)

reverse = (
    reference_priority
    .exceptAll(gold_dim_workorder_priority_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11V — GOLD WORK ORDER PRIORITY DIMENSION")
print("=" * 65)

print("Source Work Orders:", workorders.count())
print("Dimension records:",
      gold_dim_workorder_priority_staged.count())
print("Gold columns:",
      len(gold_dim_workorder_priority_staged.columns))
print("Missing priority records:", missing[0]["source_workorder_count"])

gold_dim_workorder_priority_staged.orderBy(
    "workorder_priority_key"
).show(11, truncate=False)

print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11V GOLD DIM_WORKORDER_PRIORITY: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11W — Publish the validated Work Order Priority dimension.
# Verify all 11 records against the production Gold reference.
# Never overwrite existing staging or production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_workorder_priority"
REFERENCE = f"{PRODUCTION}.dim_workorder_priority"
EXPECTED_ROWS = 11

reference = spark.table(REFERENCE)

# 1. Reuse the validated transformation
try:
    source = gold_dim_workorder_priority_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11V before publishing dim_workorder_priority."
    )

# 2. Verify source records, schema and data
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("WORK ORDER PRIORITY DIMENSION: VERIFIED")

# 3. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("WORK ORDER PRIORITY STAGING: NEWLY PUBLISHED")
else:
    print("WORK ORDER PRIORITY STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11W — WORK ORDER PRIORITY STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11W GOLD DIM_WORKORDER_PRIORITY STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11X — Rebuild the Transaction Type dimension from staged Gold.
# Preserve original transaction types and missing-value reporting rules.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

# 1. Confirm the previous staging publication
assert spark.catalog.tableExists(
    f"{STAGING}.dim_workorder_priority"
), "Complete Step 9.11W before continuing."

transactions = spark.table(
    f"{STAGING}.fact_transactions"
)

reference = spark.table(
    f"{PRODUCTION}.dim_transaction_type"
)

MISSING_LABEL = "<MISSING_TRANSACTION_TYPE>"

# 2. Count transactions by type, including NULL
type_counts = (
    transactions
    .groupBy("transaction_type")
    .agg(
        F.count("*").cast("long").alias(
            "source_transaction_count"
        )
    )
)

# 3. Create the missing-value member only if needed
missing_types = (
    type_counts
    .filter(F.col("transaction_type").isNull())
    .select(
        F.lit(0).cast("int").alias("transaction_type_key"),
        F.lit(MISSING_LABEL).alias("transaction_type"),
        F.lit(True).alias("is_missing_type"),
        "source_transaction_count"
    )
)

# 4. Assign alphabetical keys to non-null transaction types
type_window = Window.orderBy("transaction_type")

actual_types = (
    type_counts
    .filter(F.col("transaction_type").isNotNull())
    .withColumn(
        "transaction_type_key",
        F.row_number().over(type_window).cast("int")
    )
    .select(
        "transaction_type_key",
        "transaction_type",
        F.lit(False).alias("is_missing_type"),
        "source_transaction_count"
    )
)

# 5. Combine members and match production column types
gold_dim_transaction_type_staged = (
    missing_types
    .unionByName(actual_types)
    .select(
        *[
            F.col(name)
            .cast(reference.schema[name].dataType)
            .alias(name)
            for name in reference.columns
        ]
    )
)

# 6. Validate counts, keys and source reconciliation
assert transactions.count() == 755122
assert gold_dim_transaction_type_staged.count() == 6

assert (
    gold_dim_transaction_type_staged
    .agg(F.sum("source_transaction_count"))
    .first()[0]
    == 755122
)

assert (
    gold_dim_transaction_type_staged
    .select("transaction_type_key")
    .distinct()
    .count()
    == 6
)

assert (
    gold_dim_transaction_type_staged
    .filter(F.col("transaction_type") == MISSING_LABEL)
    .filter(~F.col("is_missing_type"))
    .count()
    == 0
)

# 7. Verify exact production column names and data types
assert (
    gold_dim_transaction_type_staged.schema.names
    == reference.schema.names
)

for name in reference.columns:
    assert (
        gold_dim_transaction_type_staged.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 8. Compare every dimension record against production
forward = (
    gold_dim_transaction_type_staged
    .exceptAll(reference)
    .count()
)

reverse = (
    reference
    .exceptAll(gold_dim_transaction_type_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11X — GOLD TRANSACTION TYPE DIMENSION")
print("=" * 65)

print("Source transaction records:", transactions.count())
print("Dimension records:",
      gold_dim_transaction_type_staged.count())
print("Gold columns:",
      len(gold_dim_transaction_type_staged.columns))

gold_dim_transaction_type_staged.orderBy(
    "transaction_type_key"
).show(truncate=False)

print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11X GOLD DIM_TRANSACTION_TYPE: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11Y — Publish the validated Transaction Type dimension.
# Verify all 6 records against the existing production Gold reference.
# Never overwrite staging tables or modify production Delta data.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_transaction_type"
REFERENCE = f"{PRODUCTION}.dim_transaction_type"
EXPECTED_ROWS = 6

reference = spark.table(REFERENCE)

# 1. Retrieve the transformation created in Step 9.11X
try:
    source = gold_dim_transaction_type_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11X before publishing dim_transaction_type."
    )

# 2. Validate the source before publication
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("TRANSACTION TYPE DIMENSION: VERIFIED")

# 3. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("TRANSACTION TYPE STAGING: NEWLY PUBLISHED")
else:
    print("TRANSACTION TYPE STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11Y — TRANSACTION TYPE STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11Y GOLD DIM_TRANSACTION_TYPE STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11Z — Rebuild Transaction Status dimension from staged Gold.
# Preserve original statuses, deterministic keys and missing-value rules.
# Validate against production without modifying Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

# 1. Confirm the previous staging publication
assert spark.catalog.tableExists(
    f"{STAGING}.dim_transaction_type"
), "Complete Step 9.11Y before continuing."

transactions_status_source = spark.table(
    f"{STAGING}.fact_transactions"
)

reference_status = spark.table(
    f"{PRODUCTION}.dim_transaction_status"
)

MISSING_LABEL = "<MISSING_TRANSACTION_STATUS>"

# 2. Count transactions by status, including NULL
status_counts = (
    transactions_status_source
    .groupBy("transaction_status")
    .agg(
        F.count("*").cast("long").alias(
            "source_transaction_count"
        )
    )
)

# 3. Create a missing-status member only when NULL exists
missing_statuses = (
    status_counts
    .filter(F.col("transaction_status").isNull())
    .select(
        F.lit(0).cast("int").alias("transaction_status_key"),
        F.lit(MISSING_LABEL).alias("transaction_status"),
        F.lit(True).alias("is_missing_status"),
        "source_transaction_count"
    )
)

# 4. Assign alphabetical keys to actual statuses
status_window = Window.orderBy("transaction_status")

actual_statuses = (
    status_counts
    .filter(F.col("transaction_status").isNotNull())
    .withColumn(
        "transaction_status_key",
        F.row_number().over(status_window).cast("int")
    )
    .select(
        "transaction_status_key",
        "transaction_status",
        F.lit(False).alias("is_missing_status"),
        "source_transaction_count"
    )
)

# 5. Combine members and match production column types
gold_dim_transaction_status_staged = (
    missing_statuses
    .unionByName(actual_statuses)
    .select(
        *[
            F.col(name)
            .cast(reference_status.schema[name].dataType)
            .alias(name)
            for name in reference_status.columns
        ]
    )
)

# 6. Validate source reconciliation and dimension keys
assert transactions_status_source.count() == 755122
assert gold_dim_transaction_status_staged.count() == 3

assert (
    gold_dim_transaction_status_staged
    .agg(F.sum("source_transaction_count"))
    .first()[0]
    == 755122
)

assert (
    gold_dim_transaction_status_staged
    .select("transaction_status_key")
    .distinct()
    .count()
    == 3
)

assert (
    gold_dim_transaction_status_staged
    .filter(
        (F.col("transaction_status") == MISSING_LABEL)
        & (~F.col("is_missing_status"))
    )
    .count()
    == 0
)

# 7. Verify the exact production schema
assert (
    gold_dim_transaction_status_staged.schema.names
    == reference_status.schema.names
)

for name in reference_status.columns:
    assert (
        gold_dim_transaction_status_staged.schema[name].dataType
        == reference_status.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 8. Compare every dimension record against production
forward = (
    gold_dim_transaction_status_staged
    .exceptAll(reference_status)
    .count()
)

reverse = (
    reference_status
    .exceptAll(gold_dim_transaction_status_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11Z — GOLD TRANSACTION STATUS DIMENSION")
print("=" * 65)

print("Source transaction records:",
      transactions_status_source.count())
print("Dimension records:",
      gold_dim_transaction_status_staged.count())
print("Gold columns:",
      len(gold_dim_transaction_status_staged.columns))

gold_dim_transaction_status_staged.orderBy(
    "transaction_status_key"
).show(truncate=False)

print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11Z GOLD DIM_TRANSACTION_STATUS: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AA — Publish the validated Transaction Status dimension.
# Verify all 3 records against the production Gold reference.
# Never overwrite existing staging or production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_transaction_status"
REFERENCE = f"{PRODUCTION}.dim_transaction_status"
EXPECTED_ROWS = 3

reference = spark.table(REFERENCE)

# 1. Retrieve the transformation from Step 9.11Z
try:
    source = gold_dim_transaction_status_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11Z before publishing dim_transaction_status."
    )

# 2. Validate source schema and records
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("TRANSACTION STATUS DIMENSION: VERIFIED")

# 3. Publish only if the target does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("TRANSACTION STATUS STAGING: NEWLY PUBLISHED")
else:
    print("TRANSACTION STATUS STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11AA — TRANSACTION STATUS STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11AA GOLD DIM_TRANSACTION_STATUS STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11AB — Rebuild the Detail Type dimension from staged Gold.
# Preserve original detail types, missing-value members and dimension keys.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

# 1. Verify the previous Gold staging table
assert spark.catalog.tableExists(
    f"{STAGING}.dim_transaction_status"
), "Complete Step 9.11AA before continuing."

details = spark.table(
    f"{STAGING}.fact_transaction_details"
)

reference = spark.table(
    f"{PRODUCTION}.dim_detail_type"
)

MISSING_LABEL = "<MISSING_DETAIL_TYPE>"

# 2. Count detail records by type, including NULL
type_counts = (
    details
    .groupBy("detail_type")
    .agg(
        F.count("*").cast("long").alias(
            "source_detail_count"
        )
    )
)

# 3. Preserve the missing-detail-type reporting member
missing_types = (
    type_counts
    .filter(F.col("detail_type").isNull())
    .select(
        F.lit(0).cast("int").alias("detail_type_key"),
        F.lit(MISSING_LABEL).alias("detail_type"),
        F.lit(True).alias("is_missing_type"),
        "source_detail_count"
    )
)

# 4. Assign alphabetical dimension keys to actual types
type_window = Window.orderBy("detail_type")

actual_types = (
    type_counts
    .filter(F.col("detail_type").isNotNull())
    .withColumn(
        "detail_type_key",
        F.row_number().over(type_window).cast("int")
    )
    .select(
        "detail_type_key",
        "detail_type",
        F.lit(False).alias("is_missing_type"),
        "source_detail_count"
    )
)

# 5. Combine and align with production Gold schema
gold_dim_detail_type_staged = (
    missing_types
    .unionByName(actual_types)
    .select(
        *[
            F.col(name)
            .cast(reference.schema[name].dataType)
            .alias(name)
            for name in reference.columns
        ]
    )
)

# 6. Validate source and dimension counts
assert details.count() == 2301927
assert gold_dim_detail_type_staged.count() == 6

assert (
    gold_dim_detail_type_staged
    .agg(F.sum("source_detail_count"))
    .first()[0]
    == 2301927
)

assert (
    gold_dim_detail_type_staged
    .select("detail_type_key")
    .distinct()
    .count()
    == 6
)

# Ensure the missing-value label is not a real business type
assert (
    type_counts
    .filter(F.col("detail_type") == MISSING_LABEL)
    .count()
    == 0
)

# 7. Verify exact production column names and types
assert (
    gold_dim_detail_type_staged.schema.names
    == reference.schema.names
)

for name in reference.columns:
    assert (
        gold_dim_detail_type_staged.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 8. Compare all dimension records with production
forward = (
    gold_dim_detail_type_staged
    .exceptAll(reference)
    .count()
)

reverse = (
    reference
    .exceptAll(gold_dim_detail_type_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11AB — GOLD DETAIL TYPE DIMENSION")
print("=" * 65)

print("Source Transaction Details:", details.count())
print("Dimension records:", gold_dim_detail_type_staged.count())
print("Gold columns:", len(gold_dim_detail_type_staged.columns))

gold_dim_detail_type_staged.orderBy(
    "detail_type_key"
).show(truncate=False)

print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11AB GOLD DIM_DETAIL_TYPE: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AC — Publish the validated Detail Type dimension.
# Verify all 6 records against the production Gold reference.
# Never overwrite existing staging or production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_detail_type"
REFERENCE = f"{PRODUCTION}.dim_detail_type"
EXPECTED_ROWS = 6

reference = spark.table(REFERENCE)

# 1. Reuse the validated transformation from Step 9.11AB
try:
    source = gold_dim_detail_type_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AB before publishing dim_detail_type."
    )

# 2. Verify row count, schema and source data
assert source.count() == EXPECTED_ROWS, (
    "Unexpected Detail Type dimension row count."
)

assert source.schema.names == reference.schema.names, (
    "Detail Type column names do not match production."
)

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("DETAIL TYPE DIMENSION: VERIFIED")

# 3. Publish only when the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("DETAIL TYPE STAGING: NEWLY PUBLISHED")
else:
    print("DETAIL TYPE STAGING: ALREADY EXISTS — VERIFYING")

# 4. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11AC — DETAIL TYPE STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11AC GOLD DIM_DETAIL_TYPE STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11AD — Rebuild dim_date from four staged Gold fact tables.
# Preserve the original continuous calendar and ISO date attributes.
# Validate against production without modifying any Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

REFERENCE = f"{PRODUCTION}.dim_date"

DATE_SOURCES = {
    "fact_workorders": ("workorder_date", 2231535),
    "fact_labor": ("labor_date", 3230423),
    "fact_transactions": ("transaction_date", 755122),
    "fact_transaction_details": ("detail_date", 2301927)
}

assert spark.catalog.tableExists(
    f"{STAGING}.dim_detail_type"
), "Complete Step 9.11AC before continuing."

reference_date = spark.table(REFERENCE)

# 1. Determine the calendar range from staged Gold fact dates
first_dates = []
last_dates = []

for table_name, (date_column, expected_rows) in DATE_SOURCES.items():
    table = spark.table(f"{STAGING}.{table_name}")

    assert table.count() == expected_rows
    assert date_column in table.columns

    date_stats = table.select(
        F.to_date(F.col(date_column)).alias("source_date")
    ).agg(
        F.min("source_date").alias("first_date"),
        F.max("source_date").alias("last_date")
    ).first()

    assert date_stats["first_date"] is not None
    assert date_stats["last_date"] is not None

    first_dates.append(date_stats["first_date"])
    last_dates.append(date_stats["last_date"])

    print(
        f"{table_name}: "
        f"{date_stats['first_date']} to "
        f"{date_stats['last_date']}"
    )

first_date = min(first_dates)
last_date = max(last_dates)

# 2. Generate a continuous calendar
calendar = spark.range(1).select(
    F.explode(
        F.sequence(
            F.lit(first_date).cast("date"),
            F.lit(last_date).cast("date"),
            F.expr("INTERVAL 1 DAY")
        )
    ).alias("calendar_date")
)

# 3. Reproduce the original calendar attributes
iso_day = (
    (F.dayofweek("calendar_date") + F.lit(5)) % 7
) + 1

gold_dim_date_staged = calendar.select(
    F.date_format("calendar_date", "yyyyMMdd")
        .cast("int").alias("date_key"),

    F.col("calendar_date"),

    F.year("calendar_date")
        .cast("smallint").alias("year"),

    F.quarter("calendar_date")
        .cast("tinyint").alias("quarter"),

    F.month("calendar_date")
        .cast("tinyint").alias("month"),

    F.date_format("calendar_date", "MMMM")
        .alias("month_name"),

    F.date_format("calendar_date", "yyyy-MM")
        .alias("year_month"),

    F.dayofmonth("calendar_date")
        .cast("tinyint").alias("day_of_month"),

    iso_day.cast("tinyint").alias("day_of_week"),

    F.date_format("calendar_date", "EEEE")
        .alias("day_name"),

    F.weekofyear("calendar_date")
        .cast("tinyint").alias("iso_week"),

    (iso_day >= 6).alias("is_weekend")
)

# 4. Match the production Gold column types
gold_dim_date_staged = gold_dim_date_staged.select(
    *[
        F.col(name)
        .cast(reference_date.schema[name].dataType)
        .alias(name)
        for name in reference_date.columns
    ]
)

# 5. Validate the calendar coverage
expected_days = (last_date - first_date).days + 1

assert first_date.isoformat() == "2001-03-21"
assert last_date.isoformat() == "2026-10-31"
assert expected_days == 9356

assert gold_dim_date_staged.count() == 9356

assert (
    gold_dim_date_staged
    .select("date_key")
    .distinct()
    .count()
    == 9356
)

assert (
    gold_dim_date_staged
    .select("calendar_date")
    .distinct()
    .count()
    == 9356
)

# 6. Verify production schema
assert gold_dim_date_staged.schema.names == (
    reference_date.schema.names
)

for name in reference_date.columns:
    assert (
        gold_dim_date_staged.schema[name].dataType
        == reference_date.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 7. Compare all generated calendar records against production
forward = gold_dim_date_staged.exceptAll(reference_date).count()
reverse = reference_date.exceptAll(gold_dim_date_staged).count()

print("\n" + "=" * 65)
print("STEP 9.11AD — GOLD DATE DIMENSION")
print("=" * 65)

print("First calendar date:", first_date)
print("Last calendar date:", last_date)
print("Calendar records:", gold_dim_date_staged.count())
print("Gold columns:", len(gold_dim_date_staged.columns))
print("Unique date keys: 9356")
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11AD GOLD DIM_DATE: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AE — Publish the validated Gold Calendar dimension.
# Verify all 9,356 calendar records against the production Gold reference.
# Never overwrite existing staging or production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_date"
REFERENCE = f"{PRODUCTION}.dim_date"
EXPECTED_ROWS = 9356

reference = spark.table(REFERENCE)

# 1. Retrieve the calendar transformation from Step 9.11AD
try:
    source = gold_dim_date_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AD before publishing dim_date."
    )

# 2. Verify source records, schema and date coverage
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

coverage = source.agg(
    F.min("calendar_date").alias("first_date"),
    F.max("calendar_date").alias("last_date")
).first()

assert str(coverage["first_date"]) == "2001-03-21"
assert str(coverage["last_date"]) == "2026-10-31"

# 3. Compare every record with production
forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD CALENDAR DIMENSION: VERIFIED")

# 4. Publish only if staging does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD CALENDAR STAGING: NEWLY PUBLISHED")
else:
    print("GOLD CALENDAR STAGING: ALREADY EXISTS — VERIFYING")

# 5. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11AE — GOLD CALENDAR STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Gold columns:", len(published.columns))
print("First calendar date:", coverage["first_date"])
print("Last calendar date:", coverage["last_date"])
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11AE GOLD DIM_DATE STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11AF — Rebuild Gold Part dimension from staged Transaction Details.
# Preserve 14,422 valid parts, source attributes and missing-part reporting.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

assert spark.catalog.tableExists(
    f"{STAGING}.dim_date"
), "Complete Step 9.11AE before continuing."

details_part_source = spark.table(
    f"{STAGING}.fact_transaction_details"
)

reference_part = spark.table(
    f"{PRODUCTION}.dim_part"
)

MISSING_LABEL = "<MISSING_PART_CODE>"
EXPECTED_SOURCE = 2301927
EXPECTED_VALID_PARTS = 14422
EXPECTED_MISSING = 27891

# 1. Validate source records and missing-part relationships
assert details_part_source.count() == EXPECTED_SOURCE

missing_stats = details_part_source.agg(
    F.sum(F.col("part_code").isNull().cast("long"))
        .alias("missing_codes"),
    F.sum(F.col("part_identity").isNull().cast("long"))
        .alias("missing_identities"),
    F.sum(
        (
            F.col("part_code").isNull()
            & F.col("part_identity").isNull()
        ).cast("long")
    ).alias("missing_pairs")
).first()

assert missing_stats["missing_codes"] == EXPECTED_MISSING
assert missing_stats["missing_identities"] == EXPECTED_MISSING
assert missing_stats["missing_pairs"] == EXPECTED_MISSING

# 2. Select valid source part relationships
valid_parts = details_part_source.filter(
    F.col("part_code").isNotNull()
    & F.col("part_identity").isNotNull()
)

assert valid_parts.count() == (
    EXPECTED_SOURCE - EXPECTED_MISSING
)

# 3. Verify that a part code has consistent source attributes
part_attributes = [
    "part_code",
    "part_identity",
    "part_description",
    "part_class",
    "part_uom"
]

unique_attributes = valid_parts.select(
    *part_attributes
).distinct()

assert unique_attributes.count() == EXPECTED_VALID_PARTS

assert (
    unique_attributes
    .select("part_code")
    .distinct()
    .count()
    == EXPECTED_VALID_PARTS
), "Conflicting attributes for a part code."

assert (
    unique_attributes
    .select("part_identity")
    .distinct()
    .count()
    == EXPECTED_VALID_PARTS
), "Conflicting part identity/code relationships."

# 4. Calculate the source record count per part
part_counts = (
    valid_parts
    .groupBy("part_code")
    .agg(
        F.count("*").cast("long")
        .alias("source_detail_count")
    )
)

# 5. Assign deterministic keys alphabetically
part_window = Window.orderBy("part_code")

actual_parts = (
    unique_attributes
    .join(part_counts, "part_code", "inner")
    .withColumn(
        "part_key",
        F.row_number().over(part_window).cast("int")
    )
    .withColumn("is_missing_part", F.lit(False))
)

# 6. Create the missing-part reporting member
missing_part = (
    spark.range(1)
    .select(
        F.lit(0).cast("int").alias("part_key"),
        F.lit(MISSING_LABEL).alias("part_code"),
        F.lit(None).cast("long").alias("part_identity"),
        F.lit("Missing part reference").alias("part_description"),
        F.lit(None).cast("string").alias("part_class"),
        F.lit(None).cast("string").alias("part_uom"),
        F.lit(True).alias("is_missing_part"),
        F.lit(EXPECTED_MISSING).cast("long")
            .alias("source_detail_count")
    )
)

# 7. Combine valid parts and the missing-part member
gold_dim_part_staged = (
    actual_parts
    .select(
        *[
            "part_key",
            "part_code",
            "part_identity",
            "part_description",
            "part_class",
            "part_uom",
            "is_missing_part",
            "source_detail_count"
        ]
    )
    .unionByName(missing_part)
    .select(
        *[
            F.col(name)
            .cast(reference_part.schema[name].dataType)
            .alias(name)
            for name in reference_part.columns
        ]
    )
)

# 8. Validate the completed dimension
assert gold_dim_part_staged.count() == 14423

assert (
    gold_dim_part_staged
    .select("part_key")
    .distinct()
    .count()
    == 14423
)

assert (
    gold_dim_part_staged
    .select("part_code")
    .distinct()
    .count()
    == 14423
)

assert (
    gold_dim_part_staged
    .agg(F.sum("source_detail_count"))
    .first()[0]
    == EXPECTED_SOURCE
)

missing_member = gold_dim_part_staged.filter(
    F.col("is_missing_part")
).collect()

assert len(missing_member) == 1
assert missing_member[0]["part_key"] == 0
assert missing_member[0]["part_code"] == MISSING_LABEL
assert missing_member[0]["source_detail_count"] == EXPECTED_MISSING

# 9. Verify exact production schema
assert gold_dim_part_staged.schema.names == (
    reference_part.schema.names
)

for name in reference_part.columns:
    assert (
        gold_dim_part_staged.schema[name].dataType
        == reference_part.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 10. Compare all dimension records against production
forward = gold_dim_part_staged.exceptAll(reference_part).count()
reverse = reference_part.exceptAll(gold_dim_part_staged).count()

print("=" * 65)
print("STEP 9.11AF — GOLD PART DIMENSION")
print("=" * 65)

print("Source Transaction Details:", EXPECTED_SOURCE)
print("Valid distinct parts:", EXPECTED_VALID_PARTS)
print("Missing part references:", EXPECTED_MISSING)
print("Dimension records:", gold_dim_part_staged.count())
print("Gold columns:", len(gold_dim_part_staged.columns))
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11AF GOLD DIM_PART: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AG — Publish the validated Gold Part dimension.
# Verify all 14,423 records against the production Gold reference.
# Never overwrite existing staging or production Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.dim_part"
REFERENCE = f"{PRODUCTION}.dim_part"
EXPECTED_ROWS = 14423
EXPECTED_MISSING = 27891

reference = spark.table(REFERENCE)

# 1. Retrieve the transformation from Step 9.11AF
try:
    source = gold_dim_part_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AF before publishing dim_part."
    )

# 2. Verify row count and exact production schema
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

# 3. Verify the missing-part reporting member
missing = source.filter(
    F.col("is_missing_part") == True
).collect()

assert len(missing) == 1
assert missing[0]["part_key"] == 0
assert missing[0]["part_code"] == "<MISSING_PART_CODE>"
assert missing[0]["source_detail_count"] == EXPECTED_MISSING

# 4. Compare all transformed records against production
forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD PART DIMENSION: VERIFIED")

# 5. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD PART STAGING: NEWLY PUBLISHED")
else:
    print("GOLD PART STAGING: ALREADY EXISTS — VERIFYING")

# 6. Independently validate the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

# 7. Confirm source-to-dimension reconciliation
total_source_records = published.agg(
    F.sum("source_detail_count")
).first()[0]

assert total_source_records == 2301927

print("\n" + "=" * 65)
print("STEP 9.11AG — GOLD PART STAGING PUBLICATION")
print("=" * 65)

print("Published records:", published.count())
print("Valid part members:", EXPECTED_ROWS - 1)
print("Missing part references:", EXPECTED_MISSING)
print("Gold columns:", len(published.columns))
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11AG GOLD DIM_PART STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11AH — Rebuild Monthly Work Order KPIs from staged Gold.
# Preserve monthly/type aggregates, completion and asset-mapping metrics.
# Compare against production without modifying existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SOURCE = f"{STAGING}.fact_workorders"
REFERENCE = f"{PRODUCTION}.gold_workorder_monthly_kpis"

# 1. Confirm the previous Gold dimension was published
assert spark.catalog.tableExists(
    f"{STAGING}.dim_part"
), "Complete Step 9.11AG before continuing."

workorders_kpi_source = spark.table(SOURCE)
reference_workorder_kpis = spark.table(REFERENCE)

assert workorders_kpi_source.count() == 2231535

# 2. Validate required source fields
required = [
    "workorder_date",
    "workorder_type",
    "workorder_priority",
    "completed_at",
    "asset_resolution_status"
]

assert all(
    c in workorders_kpi_source.columns
    for c in required
)

assert workorders_kpi_source.filter(
    F.col("workorder_date").isNull()
    | F.col("workorder_type").isNull()
    | (F.length(F.trim("workorder_type")) == 0)
).count() == 0

# 3. Aggregate by reporting month and Work Order type
gold_workorder_monthly_kpis_staged = (
    workorders_kpi_source
    .withColumn(
        "reporting_month",
        F.date_format("workorder_date", "yyyy-MM")
    )
    .groupBy(
        "reporting_month",
        "workorder_type"
    )
    .agg(
        F.count("*").cast("long").alias(
            "workorder_count"
        ),

        F.sum(
            F.col("completed_at").isNotNull().cast("long")
        ).alias("completion_timestamp_present_count"),

        F.sum(
            F.col("completed_at").isNull().cast("long")
        ).alias("completion_timestamp_missing_count"),

        F.sum(
            F.col("workorder_priority").isNotNull().cast("long")
        ).alias("priority_present_count"),

        F.sum(
            F.col("workorder_priority").isNull().cast("long")
        ).alias("priority_missing_count"),

        F.sum(
            (
                F.col("asset_resolution_status") == "AMBIGUOUS"
            ).cast("long")
        ).alias("ambiguous_asset_mapping_count"),

        F.sum(
            (
                F.col("asset_resolution_status") == "UNIQUE"
            ).cast("long")
        ).alias("unique_asset_mapping_count")
    )
)

# 4. Align columns with the existing production schema
gold_workorder_monthly_kpis_staged = (
    gold_workorder_monthly_kpis_staged.select(
        *[
            F.col(name)
            .cast(reference_workorder_kpis.schema[name].dataType)
            .alias(name)
            for name in reference_workorder_kpis.columns
        ]
    )
)

# 5. Validate original monthly KPI business rules
assert gold_workorder_monthly_kpis_staged.count() == 1280

assert (
    gold_workorder_monthly_kpis_staged
    .select("reporting_month")
    .distinct()
    .count()
    == 224
)

assert (
    gold_workorder_monthly_kpis_staged
    .select("reporting_month", "workorder_type")
    .distinct()
    .count()
    == 1280
)

stats = gold_workorder_monthly_kpis_staged.agg(
    F.sum("workorder_count").alias("total_workorders"),

    F.sum("completion_timestamp_missing_count")
        .alias("missing_completion"),

    F.sum("priority_missing_count")
        .alias("missing_priority"),

    F.sum("ambiguous_asset_mapping_count")
        .alias("ambiguous_assets"),

    F.min("reporting_month").alias("first_month"),

    F.max("reporting_month").alias("last_month")
).first()

assert stats["total_workorders"] == 2231535
assert stats["missing_completion"] == 25585
assert stats["missing_priority"] == 994238
assert stats["ambiguous_assets"] == 400
assert stats["first_month"] == "2001-03"
assert stats["last_month"] == "2026-10"

# 6. Validate internal KPI reconciliation
invalid_groups = gold_workorder_monthly_kpis_staged.filter(
    (
        F.col("completion_timestamp_present_count")
        + F.col("completion_timestamp_missing_count")
        != F.col("workorder_count")
    )
    |
    (
        F.col("priority_present_count")
        + F.col("priority_missing_count")
        != F.col("workorder_count")
    )
    |
    (
        F.col("ambiguous_asset_mapping_count")
        + F.col("unique_asset_mapping_count")
        != F.col("workorder_count")
    )
).count()

assert invalid_groups == 0

# 7. Validate exact production schema
assert (
    gold_workorder_monthly_kpis_staged.schema.names
    == reference_workorder_kpis.schema.names
)

for name in reference_workorder_kpis.columns:
    assert (
        gold_workorder_monthly_kpis_staged.schema[name].dataType
        == reference_workorder_kpis.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 8. Compare all monthly KPI records against production
forward = (
    gold_workorder_monthly_kpis_staged
    .exceptAll(reference_workorder_kpis)
    .count()
)

reverse = (
    reference_workorder_kpis
    .exceptAll(gold_workorder_monthly_kpis_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11AH — GOLD MONTHLY WORK ORDER KPIs")
print("=" * 65)

print("Source Work Orders:", stats["total_workorders"])
print("KPI records:", gold_workorder_monthly_kpis_staged.count())
print("Gold columns:", len(gold_workorder_monthly_kpis_staged.columns))
print("Reporting months:", 224)
print("First reporting month:", stats["first_month"])
print("Last reporting month:", stats["last_month"])
print("Missing completion timestamps:", stats["missing_completion"])
print("Missing priorities:", stats["missing_priority"])
print("Ambiguous asset mappings:", stats["ambiguous_assets"])
print("Invalid KPI groups:", invalid_groups)
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11AH GOLD WORKORDER MONTHLY KPIs: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AI — Publish validated Monthly Work Order KPIs to staging.
# Verify all 1,280 KPI records and their reconciliation totals.
# Never overwrite existing staging or production Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.gold_workorder_monthly_kpis"
REFERENCE = f"{PRODUCTION}.gold_workorder_monthly_kpis"
EXPECTED_ROWS = 1280

reference = spark.table(REFERENCE)

# 1. Reuse the transformation from Step 9.11AH
try:
    source = gold_workorder_monthly_kpis_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AH before publishing "
        "gold_workorder_monthly_kpis."
    )

# 2. Validate source records and schema
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

# 3. Verify all source records against production
forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

# 4. Verify monthly KPI reconciliation
totals = source.agg(
    F.sum("workorder_count").alias("workorders"),
    F.sum("completion_timestamp_missing_count")
        .alias("missing_completion"),
    F.sum("priority_missing_count")
        .alias("missing_priority"),
    F.sum("ambiguous_asset_mapping_count")
        .alias("ambiguous_assets")
).first()

assert totals["workorders"] == 2231535
assert totals["missing_completion"] == 25585
assert totals["missing_priority"] == 994238
assert totals["ambiguous_assets"] == 400

print("GOLD WORK ORDER MONTHLY KPIs: VERIFIED")

# 5. Publish only if the staging table does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("WORK ORDER MONTHLY KPI STAGING: NEWLY PUBLISHED")
else:
    print("WORK ORDER MONTHLY KPI STAGING: ALREADY EXISTS — VERIFYING")

# 6. Independently validate the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

print("\n" + "=" * 65)
print("STEP 9.11AI — WORK ORDER MONTHLY KPI STAGING")
print("=" * 65)

print("Published KPI records:", published.count())
print("Gold columns:", len(published.columns))
print("Source Work Orders:", totals["workorders"])
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11AI GOLD WORKORDER MONTHLY KPI STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11AJ — Rebuild Monthly Labor KPIs from staged Gold fact_labor.
# Preserve signed labor hours, costs, unmatched work orders and quality flags.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SOURCE = f"{STAGING}.fact_labor"
REFERENCE = f"{PRODUCTION}.gold_labor_monthly_kpis"

# 1. Confirm the previous monthly KPI table was published
assert spark.catalog.tableExists(
    f"{STAGING}.gold_workorder_monthly_kpis"
), "Complete Step 9.11AI before continuing."

labor_kpi_source = spark.table(SOURCE)
reference_labor_kpis = spark.table(REFERENCE)

EXPECTED_SOURCE = 3230423
EXPECTED_KPI_ROWS = 232

assert labor_kpi_source.count() == EXPECTED_SOURCE

# 2. Validate original required source fields
required = [
    "labor_date",
    "labor_hours",
    "labor_cost",
    "labor_rate",
    "workorder_match_status",
    "negative_hours_flag",
    "negative_cost_flag",
    "zero_rate_flag"
]

assert all(c in labor_kpi_source.columns for c in required)

assert labor_kpi_source.filter(
    F.col("labor_date").isNull()
    | F.col("labor_hours").isNull()
    | F.col("labor_cost").isNull()
    | F.col("labor_rate").isNull()
    | F.col("workorder_match_status").isNull()
).count() == 0

assert labor_kpi_source.filter(
    ~F.col("workorder_match_status").isin(
        "MATCHED", "UNMATCHED", "MISSING_REFERENCE"
    )
).count() == 0

# 3. Validate Silver-derived flags against the actual values
invalid_flags = labor_kpi_source.filter(
    ~F.col("negative_hours_flag").eqNullSafe(
        F.col("labor_hours") < 0
    )
    |
    ~F.col("negative_cost_flag").eqNullSafe(
        F.col("labor_cost") < 0
    )
    |
    ~F.col("zero_rate_flag").eqNullSafe(
        F.col("labor_rate") == 0
    )
).count()

assert invalid_flags == 0, (
    f"Inconsistent labor quality flags: {invalid_flags}"
)

# 4. Aggregate by month and work-order match status
gold_labor_monthly_kpis_staged = (
    labor_kpi_source
    .withColumn(
        "reporting_month",
        F.date_format("labor_date", "yyyy-MM")
    )
    .groupBy(
        "reporting_month",
        "workorder_match_status"
    )
    .agg(
        F.count("*").cast("long")
            .alias("labor_record_count"),

        F.sum(
            F.col("labor_hours").cast("decimal(30,6)")
        ).alias("total_labor_hours"),

        F.sum(
            F.col("labor_cost").cast("decimal(30,6)")
        ).alias("total_labor_cost"),

        F.sum(
            F.when(F.col("negative_hours_flag"), 1)
            .otherwise(0)
        ).cast("long").alias("negative_hours_count"),

        F.sum(
            F.when(F.col("negative_cost_flag"), 1)
            .otherwise(0)
        ).cast("long").alias("negative_cost_count"),

        F.sum(
            F.when(F.col("zero_rate_flag"), 1)
            .otherwise(0)
        ).cast("long").alias("zero_rate_count")
    )
)

# 5. Match the exact production Gold column types
gold_labor_monthly_kpis_staged = (
    gold_labor_monthly_kpis_staged.select(
        *[
            F.col(name)
            .cast(reference_labor_kpis.schema[name].dataType)
            .alias(name)
            for name in reference_labor_kpis.columns
        ]
    )
)

# 6. Validate original source reconciliation
assert gold_labor_monthly_kpis_staged.count() == EXPECTED_KPI_ROWS

assert (
    gold_labor_monthly_kpis_staged
    .select("reporting_month", "workorder_match_status")
    .distinct()
    .count()
    == EXPECTED_KPI_ROWS
)

stats = gold_labor_monthly_kpis_staged.agg(
    F.sum("labor_record_count").alias("labor_records"),
    F.sum("negative_hours_count").alias("negative_hours"),
    F.sum("negative_cost_count").alias("negative_cost"),
    F.sum("zero_rate_count").alias("zero_rate")
).first()

assert stats["labor_records"] == 3230423
assert stats["negative_hours"] == 74609
assert stats["negative_cost"] == 71374
assert stats["zero_rate"] == 188165

# 7. Validate matched and unmatched labor counts
status_counts = {
    row["workorder_match_status"]: row["records"]
    for row in gold_labor_monthly_kpis_staged
    .groupBy("workorder_match_status")
    .agg(F.sum("labor_record_count").alias("records"))
    .collect()
}

assert status_counts.get("MATCHED", 0) == 3217345
assert status_counts.get("UNMATCHED", 0) == 13078
assert status_counts.get("MISSING_REFERENCE", 0) == 0

# 8. Validate production column names and data types
assert (
    gold_labor_monthly_kpis_staged.schema.names
    == reference_labor_kpis.schema.names
)

for name in reference_labor_kpis.columns:
    assert (
        gold_labor_monthly_kpis_staged.schema[name].dataType
        == reference_labor_kpis.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 9. Compare every monthly KPI record against production
forward = (
    gold_labor_monthly_kpis_staged
    .exceptAll(reference_labor_kpis)
    .count()
)

reverse = (
    reference_labor_kpis
    .exceptAll(gold_labor_monthly_kpis_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11AJ — GOLD MONTHLY LABOR KPIs")
print("=" * 65)

print("Source Labor records:", stats["labor_records"])
print("Monthly KPI records:", gold_labor_monthly_kpis_staged.count())
print("Gold columns:", len(gold_labor_monthly_kpis_staged.columns))
print("Matched Labor:", status_counts.get("MATCHED", 0))
print("Unmatched Labor:", status_counts.get("UNMATCHED", 0))
print("Negative hours records:", stats["negative_hours"])
print("Negative cost records:", stats["negative_cost"])
print("Zero-rate records:", stats["zero_rate"])
print("Invalid quality flags:", invalid_flags)
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11AJ GOLD LABOR MONTHLY KPIs: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AK — Publish validated Monthly Labor KPIs to Gold staging.
# Verify 232 KPI records, financial totals and the production schema.
# Never overwrite existing staging or production Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.gold_labor_monthly_kpis"
REFERENCE = f"{PRODUCTION}.gold_labor_monthly_kpis"
EXPECTED_ROWS = 232

reference = spark.table(REFERENCE)

# 1. Reuse the validated Step 9.11AJ transformation
try:
    source = gold_labor_monthly_kpis_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AJ before publishing "
        "gold_labor_monthly_kpis."
    )

# 2. Validate records and exact schema
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

# 3. Verify business totals
totals = source.agg(
    F.sum("labor_record_count").alias("labor_records"),
    F.sum("negative_hours_count").alias("negative_hours"),
    F.sum("negative_cost_count").alias("negative_cost"),
    F.sum("zero_rate_count").alias("zero_rate")
).first()

assert totals["labor_records"] == 3230423
assert totals["negative_hours"] == 74609
assert totals["negative_cost"] == 71374
assert totals["zero_rate"] == 188165

# 4. Compare source records with production
forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD LABOR MONTHLY KPIs: VERIFIED")

# 5. Publish only if staging does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("GOLD LABOR MONTHLY KPI STAGING: NEWLY PUBLISHED")
else:
    print("GOLD LABOR MONTHLY KPI STAGING: ALREADY EXISTS — VERIFYING")

# 6. Independently verify the published Delta table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

# 7. Validate published KPI totals
published_totals = published.agg(
    F.sum("labor_record_count").alias("labor_records"),
    F.sum("negative_hours_count").alias("negative_hours"),
    F.sum("negative_cost_count").alias("negative_cost"),
    F.sum("zero_rate_count").alias("zero_rate")
).first()

assert published_totals["labor_records"] == 3230423
assert published_totals["negative_hours"] == 74609
assert published_totals["negative_cost"] == 71374
assert published_totals["zero_rate"] == 188165

print("\n" + "=" * 65)
print("STEP 9.11AK — GOLD LABOR MONTHLY KPI STAGING")
print("=" * 65)

print("Published KPI records:", published.count())
print("Gold columns:", len(published.columns))
print("Source Labor records:", published_totals["labor_records"])
print("Negative hours:", published_totals["negative_hours"])
print("Negative costs:", published_totals["negative_cost"])
print("Zero-rate records:", published_totals["zero_rate"])
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11AK GOLD LABOR MONTHLY KPI STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.11AL — Rebuild Monthly Transaction KPIs from staged Gold.
# Preserve transaction categories, missing dates and data-quality indicators.
# Validate against production without modifying existing Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SOURCE = f"{STAGING}.fact_transactions"
REFERENCE = f"{PRODUCTION}.gold_transaction_monthly_kpis"

# 1. Confirm the preceding Gold staging table exists
assert spark.catalog.tableExists(
    f"{STAGING}.gold_labor_monthly_kpis"
), "Complete Step 9.11AK before continuing."

transactions_kpi_source = spark.table(SOURCE)
reference_transaction_kpis = spark.table(REFERENCE)

EXPECTED_SOURCE = 755122
EXPECTED_KPI_ROWS = 1689

assert transactions_kpi_source.count() == EXPECTED_SOURCE

# 2. Validate source fields
required = [
    "transaction_date",
    "transaction_type",
    "transaction_status",
    "dq_transaction_date_invalid",
    "silver_dq_issue"
]

assert all(
    c in transactions_kpi_source.columns
    for c in required
)

# 3. Preserve missing dates and category values
prepared_transactions = transactions_kpi_source.select(
    F.when(
        F.col("transaction_date").isNull(),
        F.lit("<MISSING_DATE>")
    ).otherwise(
        F.date_format("transaction_date", "yyyy-MM")
    ).alias("reporting_month"),

    F.coalesce(
        F.col("transaction_type"),
        F.lit("<MISSING_TYPE>")
    ).alias("transaction_type"),

    F.coalesce(
        F.col("transaction_status"),
        F.lit("<MISSING_STATUS>")
    ).alias("transaction_status"),

    F.col("transaction_date").isNull()
        .alias("_missing_date"),

    F.col("dq_transaction_date_invalid")
        .alias("_invalid_date"),

    F.col("silver_dq_issue")
        .alias("_silver_dq_issue")
)

# 4. Aggregate by month, transaction type and status
gold_transaction_monthly_kpis_staged = (
    prepared_transactions
    .groupBy(
        "reporting_month",
        "transaction_type",
        "transaction_status"
    )
    .agg(
        F.count("*").cast("long")
            .alias("transaction_count"),

        F.sum(
            F.when(F.col("_missing_date"), 1)
            .otherwise(0)
        ).cast("long").alias("missing_date_count"),

        F.sum(
            F.when(F.col("_invalid_date") == True, 1)
            .otherwise(0)
        ).cast("long").alias("invalid_date_flag_count"),

        F.sum(
            F.when(F.col("_silver_dq_issue") == True, 1)
            .otherwise(0)
        ).cast("long").alias("silver_dq_issue_count")
    )
)

# 5. Align with the exact production schema
gold_transaction_monthly_kpis_staged = (
    gold_transaction_monthly_kpis_staged.select(
        *[
            F.col(name)
            .cast(reference_transaction_kpis.schema[name].dataType)
            .alias(name)
            for name in reference_transaction_kpis.columns
        ]
    )
)

# 6. Validate monthly KPI counts
assert (
    gold_transaction_monthly_kpis_staged.count()
    == EXPECTED_KPI_ROWS
)

assert (
    gold_transaction_monthly_kpis_staged
    .select(
        "reporting_month",
        "transaction_type",
        "transaction_status"
    )
    .distinct()
    .count()
    == EXPECTED_KPI_ROWS
)

# 7. Reconcile KPI totals with the original source
source_stats = transactions_kpi_source.agg(
    F.sum(
        F.col("transaction_date").isNull().cast("long")
    ).alias("missing_dates"),

    F.sum(
        F.when(
            F.col("dq_transaction_date_invalid") == True, 1
        ).otherwise(0)
    ).alias("invalid_dates"),

    F.sum(
        F.when(
            F.col("silver_dq_issue") == True, 1
        ).otherwise(0)
    ).alias("dq_issues")
).first()

kpi_stats = gold_transaction_monthly_kpis_staged.agg(
    F.sum("transaction_count").alias("transactions"),
    F.sum("missing_date_count").alias("missing_dates"),
    F.sum("invalid_date_flag_count").alias("invalid_dates"),
    F.sum("silver_dq_issue_count").alias("dq_issues")
).first()

assert kpi_stats["transactions"] == EXPECTED_SOURCE
assert kpi_stats["missing_dates"] == source_stats["missing_dates"]
assert kpi_stats["invalid_dates"] == source_stats["invalid_dates"]
assert kpi_stats["dq_issues"] == source_stats["dq_issues"]

# 8. Verify that individual KPI counts are valid
invalid_groups = gold_transaction_monthly_kpis_staged.filter(
    (F.col("missing_date_count") > F.col("transaction_count"))
    | (F.col("invalid_date_flag_count") > F.col("transaction_count"))
    | (F.col("silver_dq_issue_count") > F.col("transaction_count"))
).count()

assert invalid_groups == 0

# 9. Validate production schema
assert (
    gold_transaction_monthly_kpis_staged.schema.names
    == reference_transaction_kpis.schema.names
)

for name in reference_transaction_kpis.columns:
    assert (
        gold_transaction_monthly_kpis_staged.schema[name].dataType
        == reference_transaction_kpis.schema[name].dataType
    ), f"Schema mismatch: {name}"

# 10. Compare all KPI records against production
forward = (
    gold_transaction_monthly_kpis_staged
    .exceptAll(reference_transaction_kpis)
    .count()
)

reverse = (
    reference_transaction_kpis
    .exceptAll(gold_transaction_monthly_kpis_staged)
    .count()
)

print("=" * 65)
print("STEP 9.11AL — GOLD MONTHLY TRANSACTION KPIs")
print("=" * 65)

print("Source Transactions:", EXPECTED_SOURCE)
print("Monthly KPI records:",
      gold_transaction_monthly_kpis_staged.count())
print("Gold columns:",
      len(gold_transaction_monthly_kpis_staged.columns))
print("Missing transaction dates:", kpi_stats["missing_dates"])
print("Invalid-date flags:", kpi_stats["invalid_dates"])
print("Silver DQ issues:", kpi_stats["dq_issues"])
print("Invalid KPI groups:", invalid_groups)
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.11AL GOLD TRANSACTION MONTHLY KPIs: PASS")
print("READ-ONLY — EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11AM — Publish validated Monthly Transaction KPIs.
# Verify all 1,689 records, schema and data-quality totals.
# Preserve existing staging and production Delta tables.

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.gold_transaction_monthly_kpis"
REFERENCE = f"{PRODUCTION}.gold_transaction_monthly_kpis"
EXPECTED_ROWS = 1689
EXPECTED_SOURCE_ROWS = 755122

reference = spark.table(REFERENCE)

# 1. Retrieve the transformation from Step 9.11AL
try:
    source = gold_transaction_monthly_kpis_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AL before publishing "
        "gold_transaction_monthly_kpis."
    )

# 2. Verify source row count and schema
assert source.count() == EXPECTED_ROWS
assert source.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

# 3. Reconcile transaction and data-quality counts
totals = source.agg(
    F.sum("transaction_count").alias("transactions"),
    F.sum("missing_date_count").alias("missing_dates"),
    F.sum("invalid_date_flag_count").alias("invalid_dates"),
    F.sum("silver_dq_issue_count").alias("dq_issues")
).first()

assert totals["transactions"] == EXPECTED_SOURCE_ROWS

source_transactions = spark.table(
    f"{STAGING}.fact_transactions"
)

source_totals = source_transactions.agg(
    F.sum(
        F.col("transaction_date").isNull().cast("long")
    ).alias("missing_dates"),

    F.sum(
        F.when(
            F.col("dq_transaction_date_invalid") == True, 1
        ).otherwise(0)
    ).alias("invalid_dates"),

    F.sum(
        F.when(
            F.col("silver_dq_issue") == True, 1
        ).otherwise(0)
    ).alias("dq_issues")
).first()

assert totals["missing_dates"] == source_totals["missing_dates"]
assert totals["invalid_dates"] == source_totals["invalid_dates"]
assert totals["dq_issues"] == source_totals["dq_issues"]

# 4. Verify complete equality with production
forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source data mismatches: {forward + reverse}"
)

print("GOLD TRANSACTION MONTHLY KPIs: VERIFIED")

# 5. Publish only if staging does not exist
if not spark.catalog.tableExists(TARGET):
    source.write.format("delta").saveAsTable(TARGET)
    print("TRANSACTION KPI STAGING: NEWLY PUBLISHED")
else:
    print("TRANSACTION KPI STAGING: ALREADY EXISTS — VERIFYING")

# 6. Independently verify the published table
published = spark.table(TARGET)

assert published.count() == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0 and published_reverse == 0, (
    f"Published data mismatches: "
    f"{published_forward + published_reverse}"
)

# 7. Verify published transaction totals
published_totals = published.agg(
    F.sum("transaction_count").alias("transactions"),
    F.sum("missing_date_count").alias("missing_dates"),
    F.sum("invalid_date_flag_count").alias("invalid_dates"),
    F.sum("silver_dq_issue_count").alias("dq_issues")
).first()

assert published_totals["transactions"] == EXPECTED_SOURCE_ROWS
assert published_totals["missing_dates"] == source_totals["missing_dates"]
assert published_totals["invalid_dates"] == source_totals["invalid_dates"]
assert published_totals["dq_issues"] == source_totals["dq_issues"]

print("\n" + "=" * 65)
print("STEP 9.11AM — TRANSACTION MONTHLY KPI STAGING")
print("=" * 65)

print("Published KPI records:", published.count())
print("Gold columns:", len(published.columns))
print("Source Transactions:", published_totals["transactions"])
print("Missing dates:", published_totals["missing_dates"])
print("Invalid date flags:", published_totals["invalid_dates"])
print("Silver DQ issues:", published_totals["dq_issues"])
print("Published table:", TARGET)
print("Forward mismatches:", published_forward)
print("Reverse mismatches:", published_reverse)

print("\nSTEP 9.11AM GOLD TRANSACTION MONTHLY KPI STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# ============================================================
# STEP 9.11AN — GOLD MONTHLY MATERIAL KPI TRANSFORMATIONS
# AssetFusion360 | Databricks Free Edition
#
# Source: Staged Gold fact_transaction_details
# Target: In-memory gold_material_monthly_kpis_staged
#
# READ ONLY — NO DELTA TABLES CREATED OR OVERWRITTEN
# ============================================================

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SOURCE = f"{STAGING}.fact_transaction_details"
REFERENCE = f"{PRODUCTION}.gold_material_monthly_kpis"

EXPECTED_SOURCE_ROWS = 2301927
EXPECTED_KPI_ROWS = 12267

# ------------------------------------------------------------
# 1. Confirm previous Gold staging table exists
# ------------------------------------------------------------

assert spark.catalog.tableExists(
    f"{STAGING}.gold_transaction_monthly_kpis"
), "Complete Step 9.11AM before continuing."

assert spark.catalog.tableExists(SOURCE)
assert spark.catalog.tableExists(REFERENCE)

details = spark.table(SOURCE)
reference = spark.table(REFERENCE)

assert details.count() == EXPECTED_SOURCE_ROWS

# ------------------------------------------------------------
# 2. Validate required source columns
# ------------------------------------------------------------

required_columns = [
    "detail_date",
    "detail_type",
    "part_uom",
    "quantity",
    "unit_price",
    "average_price",
    "transaction_match_status",
    "workorder_match_status",
    "negative_quantity_flag",
    "negative_price_flag",
    "zero_quantity_flag",
    "zero_price_flag"
]

missing_columns = [
    c for c in required_columns
    if c not in details.columns
]

assert not missing_columns, (
    f"Missing required columns: {missing_columns}"
)

print("SOURCE SCHEMA: PASS")
print("SOURCE RECORDS:", details.count())

# ------------------------------------------------------------
# 3. Independently validate source quality flags
#
# Original Python business rule:
# Validate non-null flags against actual numeric values.
# Null flags are permitted.
# ------------------------------------------------------------

flag_mismatches = details.filter(
    (
        F.col("negative_quantity_flag").isNotNull()
        & ~F.col("negative_quantity_flag").eqNullSafe(
            F.col("quantity") < 0
        )
    )
    |
    (
        F.col("zero_quantity_flag").isNotNull()
        & ~F.col("zero_quantity_flag").eqNullSafe(
            F.col("quantity") == 0
        )
    )
    |
    (
        F.col("negative_price_flag").isNotNull()
        & ~F.col("negative_price_flag").eqNullSafe(
            F.col("unit_price") < 0
        )
    )
    |
    (
        F.col("zero_price_flag").isNotNull()
        & ~F.col("zero_price_flag").eqNullSafe(
            F.col("unit_price") == 0
        )
    )
).count()

assert flag_mismatches == 0, (
    f"Inconsistent source flags: {flag_mismatches}"
)

print("SOURCE QUALITY FLAGS: PASS")

# ------------------------------------------------------------
# 4. Prepare reporting categories
# Preserve all missing values using original reporting labels.
# ------------------------------------------------------------

prepared = details.select(

    F.when(
        F.col("detail_date").isNull(),
        F.lit("<MISSING_DATE>")
    ).otherwise(
        F.date_format("detail_date", "yyyy-MM")
    ).alias("reporting_month"),

    F.coalesce(
        F.col("detail_type"),
        F.lit("<MISSING_TYPE>")
    ).alias("detail_type"),

    F.coalesce(
        F.col("part_uom"),
        F.lit("<MISSING_UOM>")
    ).alias("part_uom"),

    F.coalesce(
        F.col("transaction_match_status"),
        F.lit("<MISSING_TRANSACTION_STATUS>")
    ).alias("transaction_match_status"),

    F.coalesce(
        F.col("workorder_match_status"),
        F.lit("<MISSING_WORKORDER_STATUS>")
    ).alias("workorder_match_status"),

    F.col("quantity"),
    F.col("unit_price"),
    F.col("average_price")
)

# ------------------------------------------------------------
# 5. Aggregate monthly material metrics
# Do not take absolute values of signed quantities.
# ------------------------------------------------------------

gold_material_monthly_kpis_staged = (
    prepared
    .groupBy(
        "reporting_month",
        "detail_type",
        "part_uom",
        "transaction_match_status",
        "workorder_match_status"
    )
    .agg(

        F.count("*").cast("long")
        .alias("material_detail_count"),

        F.sum(
            F.col("quantity").cast("decimal(30,6)")
        ).alias("_signed_quantity_sum"),

        F.sum(
            F.col("quantity").isNotNull().cast("long")
        ).alias("quantity_present_count"),

        F.sum(
            F.col("quantity").isNull().cast("long")
        ).alias("quantity_missing_count"),

        F.sum(
            F.when(F.col("quantity") < 0, 1)
             .otherwise(0)
        ).cast("long").alias("negative_quantity_count"),

        F.sum(
            F.when(F.col("quantity") == 0, 1)
             .otherwise(0)
        ).cast("long").alias("zero_quantity_count"),

        F.sum(
            F.col("unit_price").isNotNull().cast("long")
        ).alias("unit_price_present_count"),

        F.sum(
            F.col("unit_price").isNull().cast("long")
        ).alias("unit_price_missing_count"),

        F.sum(
            F.when(F.col("unit_price") < 0, 1)
             .otherwise(0)
        ).cast("long").alias("negative_unit_price_count"),

        F.sum(
            F.when(F.col("unit_price") == 0, 1)
             .otherwise(0)
        ).cast("long").alias("zero_unit_price_count"),

        F.sum(
            F.col("average_price").isNull().cast("long")
        ).alias("average_price_missing_count")
    )

    # The original Python code initializes each group's
    # signed quantity at Decimal("0"), even when all
    # source quantities in that group are null.
    .withColumn(
        "total_signed_quantity",
        F.coalesce(
            F.col("_signed_quantity_sum"),
            F.lit(0).cast("decimal(30,6)")
        )
    )
    .drop("_signed_quantity_sum")
)

# ------------------------------------------------------------
# 6. Align exact production column order and data types
# ------------------------------------------------------------

gold_material_monthly_kpis_staged = (
    gold_material_monthly_kpis_staged.select(
        *[
            F.col(name)
            .cast(reference.schema[name].dataType)
            .alias(name)
            for name in reference.columns
        ]
    )
)

# ------------------------------------------------------------
# 7. Validate expected KPI records and unique grouping
# ------------------------------------------------------------

assert (
    gold_material_monthly_kpis_staged.count()
    == EXPECTED_KPI_ROWS
), "Unexpected material monthly KPI count."

grain_columns = [
    "reporting_month",
    "detail_type",
    "part_uom",
    "transaction_match_status",
    "workorder_match_status"
]

assert (
    gold_material_monthly_kpis_staged
    .select(*grain_columns)
    .distinct()
    .count()
    == EXPECTED_KPI_ROWS
), "Duplicate monthly material KPI grouping."

# ------------------------------------------------------------
# 8. Reconcile original numeric business metrics
# ------------------------------------------------------------

totals = gold_material_monthly_kpis_staged.agg(

    F.sum("material_detail_count")
    .alias("source_records"),

    F.sum("quantity_present_count")
    .alias("quantity_present"),

    F.sum("quantity_missing_count")
    .alias("quantity_missing"),

    F.sum("negative_quantity_count")
    .alias("negative_quantities"),

    F.sum("zero_quantity_count")
    .alias("zero_quantities"),

    F.sum("unit_price_present_count")
    .alias("unit_price_present"),

    F.sum("unit_price_missing_count")
    .alias("unit_price_missing"),

    F.sum("negative_unit_price_count")
    .alias("negative_unit_prices"),

    F.sum("zero_unit_price_count")
    .alias("zero_unit_prices"),

    F.sum("average_price_missing_count")
    .alias("average_price_missing")

).first()

assert totals["source_records"] == 2301927

assert totals["negative_quantities"] == 44053
assert totals["negative_unit_prices"] == 5501
assert totals["zero_quantities"] == 164449
assert totals["zero_unit_prices"] == 61515

assert (
    totals["quantity_present"]
    + totals["quantity_missing"]
    == EXPECTED_SOURCE_ROWS
)

assert (
    totals["unit_price_present"]
    + totals["unit_price_missing"]
    == EXPECTED_SOURCE_ROWS
)

# ------------------------------------------------------------
# 9. Validate each KPI group's internal reconciliation
# ------------------------------------------------------------

invalid_groups = gold_material_monthly_kpis_staged.filter(

    (
        F.col("quantity_present_count")
        + F.col("quantity_missing_count")
        != F.col("material_detail_count")
    )
    |
    (
        F.col("unit_price_present_count")
        + F.col("unit_price_missing_count")
        != F.col("material_detail_count")
    )

).count()

assert invalid_groups == 0, (
    f"Invalid KPI groups: {invalid_groups}"
)

# ------------------------------------------------------------
# 10. Verify exact production schema
# ------------------------------------------------------------

assert (
    gold_material_monthly_kpis_staged.schema.names
    == reference.schema.names
)

for name in reference.columns:

    assert (
        gold_material_monthly_kpis_staged.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Schema mismatch: {name}"

# ------------------------------------------------------------
# 11. Compare all 12,267 records against production
# ------------------------------------------------------------

forward = (
    gold_material_monthly_kpis_staged
    .exceptAll(reference)
    .count()
)

reverse = (
    reference
    .exceptAll(gold_material_monthly_kpis_staged)
    .count()
)

# ------------------------------------------------------------
# 12. Final validation report
# ------------------------------------------------------------

print("\n" + "=" * 68)
print("STEP 9.11AN — GOLD MONTHLY MATERIAL KPI VALIDATION")
print("=" * 68)

print(
    "Source detail records:",
    totals["source_records"]
)

print(
    "Monthly KPI records:",
    gold_material_monthly_kpis_staged.count()
)

print(
    "Gold columns:",
    len(gold_material_monthly_kpis_staged.columns)
)

print(
    "Negative quantities:",
    totals["negative_quantities"]
)

print(
    "Negative unit prices:",
    totals["negative_unit_prices"]
)

print(
    "Zero quantities:",
    totals["zero_quantities"]
)

print(
    "Zero unit prices:",
    totals["zero_unit_prices"]
)

print("Invalid KPI groups:", invalid_groups)
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, (
    f"Forward mismatches: {forward}"
)

assert reverse == 0, (
    f"Reverse mismatches: {reverse}"
)

print(
    "\nSTEP 9.11AN GOLD MATERIAL MONTHLY KPIs: PASS"
)

print(
    "READ-ONLY — EXISTING DELTA TABLES UNCHANGED"
)


# COMMAND ----------


# ============================================================
# STEP 9.11AO — PUBLISH GOLD MATERIAL MONTHLY KPIs
# AssetFusion360 | Databricks Free Edition
#
# Final Gold staging table: 17 of 17
#
# SAFETY:
# - Never overwrite production
# - Never overwrite existing staging tables
# - Validate source before writing
# - Independently validate published Delta data
# ============================================================

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TARGET = f"{STAGING}.gold_material_monthly_kpis"
REFERENCE = f"{PRODUCTION}.gold_material_monthly_kpis"

EXPECTED_KPI_ROWS = 12267
EXPECTED_SOURCE_ROWS = 2301927

# ------------------------------------------------------------
# 1. Retrieve validated transformation from Step 9.11AN
# ------------------------------------------------------------

try:
    source = gold_material_monthly_kpis_staged
except NameError:
    raise RuntimeError(
        "Run Step 9.11AN before publishing "
        "gold_material_monthly_kpis."
    )

assert spark.catalog.tableExists(REFERENCE), (
    f"Production reference missing: {REFERENCE}"
)

reference = spark.table(REFERENCE)

# ------------------------------------------------------------
# 2. Validate source record count and schema
# ------------------------------------------------------------

assert source.count() == EXPECTED_KPI_ROWS, (
    "Unexpected material KPI row count."
)

assert source.schema.names == reference.schema.names, (
    "Source and production column names differ."
)

for name in reference.columns:
    assert (
        source.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Source schema mismatch: {name}"

print("SOURCE SCHEMA: PASS")
print("SOURCE KPI RECORDS:", source.count())

# ------------------------------------------------------------
# 3. Verify source-level business reconciliation
# ------------------------------------------------------------

totals = source.agg(

    F.sum("material_detail_count")
        .alias("material_records"),

    F.sum("negative_quantity_count")
        .alias("negative_quantities"),

    F.sum("negative_unit_price_count")
        .alias("negative_unit_prices"),

    F.sum("zero_quantity_count")
        .alias("zero_quantities"),

    F.sum("zero_unit_price_count")
        .alias("zero_unit_prices"),

    F.sum("quantity_present_count")
        .alias("quantity_present"),

    F.sum("quantity_missing_count")
        .alias("quantity_missing"),

    F.sum("unit_price_present_count")
        .alias("unit_price_present"),

    F.sum("unit_price_missing_count")
        .alias("unit_price_missing")

).first()

assert totals["material_records"] == EXPECTED_SOURCE_ROWS
assert totals["negative_quantities"] == 44053
assert totals["negative_unit_prices"] == 5501
assert totals["zero_quantities"] == 164449
assert totals["zero_unit_prices"] == 61515

assert (
    totals["quantity_present"]
    + totals["quantity_missing"]
    == EXPECTED_SOURCE_ROWS
)

assert (
    totals["unit_price_present"]
    + totals["unit_price_missing"]
    == EXPECTED_SOURCE_ROWS
)

print("MATERIAL KPI BUSINESS RECONCILIATION: PASS")

# ------------------------------------------------------------
# 4. Verify complete source equality against production
# ------------------------------------------------------------

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source mismatches: forward={forward}, reverse={reverse}"
)

print("SOURCE VS PRODUCTION: PASS")

# ------------------------------------------------------------
# 5. Publish only when the staging table does not exist
#
# saveAsTable without overwrite preserves existing tables.
# ------------------------------------------------------------

if not spark.catalog.tableExists(TARGET):

    source.write.format("delta").saveAsTable(TARGET)

    print("MATERIAL KPI STAGING: NEWLY PUBLISHED")

else:

    print(
        "MATERIAL KPI STAGING: ALREADY EXISTS — "
        "VERIFYING WITHOUT OVERWRITE"
    )

# ------------------------------------------------------------
# 6. Independently verify published Delta records and schema
# ------------------------------------------------------------

published = spark.table(TARGET)

assert published.count() == EXPECTED_KPI_ROWS

assert published.schema.names == reference.schema.names

for name in reference.columns:
    assert (
        published.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Published schema mismatch: {name}"

published_forward = (
    published.exceptAll(reference).count()
)

published_reverse = (
    reference.exceptAll(published).count()
)

assert published_forward == 0, (
    f"Published forward mismatches: {published_forward}"
)

assert published_reverse == 0, (
    f"Published reverse mismatches: {published_reverse}"
)

# ------------------------------------------------------------
# 7. Independently reconcile published KPI totals
# ------------------------------------------------------------

published_totals = published.agg(

    F.sum("material_detail_count")
        .alias("material_records"),

    F.sum("negative_quantity_count")
        .alias("negative_quantities"),

    F.sum("negative_unit_price_count")
        .alias("negative_unit_prices"),

    F.sum("zero_quantity_count")
        .alias("zero_quantities"),

    F.sum("zero_unit_price_count")
        .alias("zero_unit_prices")

).first()

assert published_totals["material_records"] == 2301927
assert published_totals["negative_quantities"] == 44053
assert published_totals["negative_unit_prices"] == 5501
assert published_totals["zero_quantities"] == 164449
assert published_totals["zero_unit_prices"] == 61515

# ------------------------------------------------------------
# 8. Final publication report
# ------------------------------------------------------------

print("\n" + "=" * 68)
print("STEP 9.11AO — FINAL GOLD MATERIAL KPI PUBLICATION")
print("=" * 68)

print("Published KPI records:", published.count())
print("Gold columns:", len(published.columns))

print(
    "Source Transaction Details:",
    published_totals["material_records"]
)

print(
    "Negative quantities:",
    published_totals["negative_quantities"]
)

print(
    "Negative unit prices:",
    published_totals["negative_unit_prices"]
)

print(
    "Zero quantities:",
    published_totals["zero_quantities"]
)

print(
    "Zero unit prices:",
    published_totals["zero_unit_prices"]
)

print("Published Delta table:", TARGET)
print("Forward mismatches:", published_forward)
print("Reverse mismatches:", published_reverse)

print("\nSTEP 9.11AO GOLD MATERIAL MONTHLY KPI STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("FINAL GOLD STAGING TABLE: PUBLISHED AND VERIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.11AP — FULL GOLD STAGING AUDIT
# AssetFusion360 | Databricks Free Edition
#
# Compare ALL 17 Gold staging tables against production.
#
# READ-ONLY AUDIT:
# - No tables created
# - No tables overwritten
# - No production changes
# - No staging changes
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

# ------------------------------------------------------------
# 1. Define the complete Gold table inventory
# ------------------------------------------------------------

GOLD_TABLES = {
    "dim_asset": 148522,
    "bridge_asset_code": 148521,
    "fact_workorders": 2231535,
    "fact_labor": 3230423,
    "fact_transactions": 755122,
    "fact_transaction_details": 2301927,
    "dim_workorder_type": 7,
    "dim_workorder_priority": 11,
    "dim_transaction_type": 6,
    "dim_transaction_status": 3,
    "dim_detail_type": 6,
    "dim_date": 9356,
    "dim_part": 14423,
    "gold_workorder_monthly_kpis": 1280,
    "gold_labor_monthly_kpis": 232,
    "gold_transaction_monthly_kpis": 1689,
    "gold_material_monthly_kpis": 12267
}

assert len(GOLD_TABLES) == 17

# ------------------------------------------------------------
# 2. Confirm that every expected table exists
# ------------------------------------------------------------

missing_production = []
missing_staging = []

for table_name in GOLD_TABLES:

    production_name = f"{PRODUCTION}.{table_name}"
    staging_name = f"{STAGING}.{table_name}"

    if not spark.catalog.tableExists(production_name):
        missing_production.append(table_name)

    if not spark.catalog.tableExists(staging_name):
        missing_staging.append(table_name)

assert not missing_production, (
    f"Missing production Gold tables: {missing_production}"
)

assert not missing_staging, (
    f"Missing staging Gold tables: {missing_staging}"
)

print("ALL 17 REQUIRED GOLD TABLES EXIST: PASS")

# ------------------------------------------------------------
# 3. Independently compare every staging table
# ------------------------------------------------------------

audit_results = []

for index, (table_name, expected_rows) in enumerate(
    GOLD_TABLES.items(), start=1
):

    print(
        f"\n[{index}/17] Auditing {table_name}",
        flush=True
    )

    production_name = f"{PRODUCTION}.{table_name}"
    staging_name = f"{STAGING}.{table_name}"

    production_df = spark.table(production_name)
    staging_df = spark.table(staging_name)

    # Check row counts
    production_rows = production_df.count()
    staging_rows = staging_df.count()

    row_count_pass = (
        production_rows == expected_rows
        and staging_rows == expected_rows
    )

    # Check exact column names and order
    column_names_pass = (
        staging_df.schema.names
        == production_df.schema.names
    )

    # Check data types and nullability separately
    data_types_pass = (
        column_names_pass
        and all(
            staging_df.schema[name].dataType
            == production_df.schema[name].dataType
            for name in production_df.columns
        )
    )

    nullability_pass = (
        column_names_pass
        and all(
            staging_df.schema[name].nullable
            == production_df.schema[name].nullable
            for name in production_df.columns
        )
    )

    # exceptAll requires compatible schemas.
    # Do not attempt comparison if column types differ.
    if column_names_pass and data_types_pass:

        # Staging records not found in production
        forward_mismatches = (
            staging_df.exceptAll(production_df).count()
        )

        # Production records not found in staging
        reverse_mismatches = (
            production_df.exceptAll(staging_df).count()
        )

    else:

        forward_mismatches = None
        reverse_mismatches = None

    data_pass = (
        forward_mismatches == 0
        and reverse_mismatches == 0
    )

    # Nullability is reported separately because
    # identical values may have different Spark
    # schema nullability metadata.
    overall_pass = (
        row_count_pass
        and column_names_pass
        and data_types_pass
        and data_pass
    )

    audit_results.append({
        "table_name": table_name,
        "expected_rows": expected_rows,
        "production_rows": production_rows,
        "staging_rows": staging_rows,
        "production_columns": len(production_df.columns),
        "staging_columns": len(staging_df.columns),
        "row_count_pass": row_count_pass,
        "column_names_pass": column_names_pass,
        "data_types_pass": data_types_pass,
        "nullability_match": nullability_pass,
        "forward_mismatches": forward_mismatches,
        "reverse_mismatches": reverse_mismatches,
        "status": "PASS" if overall_pass else "FAIL"
    })

    print("  Production rows:", production_rows)
    print("  Staging rows:", staging_rows)
    print("  Columns:", len(staging_df.columns))
    print("  Forward mismatches:", forward_mismatches)
    print("  Reverse mismatches:", reverse_mismatches)
    print("  Status:", "PASS" if overall_pass else "FAIL")

# ------------------------------------------------------------
# 4. Build a read-only audit report
# ------------------------------------------------------------

audit_df = spark.createDataFrame(audit_results)

print("\n" + "=" * 75)
print("ASSETFUSION360 — GOLD STAGING AUDIT RESULTS")
print("=" * 75)

display(
    audit_df.orderBy("table_name")
)

# ------------------------------------------------------------
# 5. Check complete Gold staging inventory
# ------------------------------------------------------------

available_staging_tables = {
    table.name
    for table in spark.catalog.listTables(STAGING)
}

missing_inventory = (
    set(GOLD_TABLES.keys())
    - available_staging_tables
)

assert not missing_inventory, (
    f"Missing Gold staging inventory: {missing_inventory}"
)

# Other Silver staging tables may also exist.
# We deliberately do not delete or modify them.

# ------------------------------------------------------------
# 6. Calculate final audit results
# ------------------------------------------------------------

passed_tables = sum(
    row["status"] == "PASS"
    for row in audit_results
)

failed_tables = [
    row["table_name"]
    for row in audit_results
    if row["status"] != "PASS"
]

expected_total_rows = sum(GOLD_TABLES.values())

production_total_rows = sum(
    row["production_rows"]
    for row in audit_results
)

staging_total_rows = sum(
    row["staging_rows"]
    for row in audit_results
)

total_forward_mismatches = sum(
    row["forward_mismatches"]
    for row in audit_results
    if row["forward_mismatches"] is not None
)

total_reverse_mismatches = sum(
    row["reverse_mismatches"]
    for row in audit_results
    if row["reverse_mismatches"] is not None
)

nullability_differences = [
    row["table_name"]
    for row in audit_results
    if not row["nullability_match"]
]

print("\n" + "=" * 75)
print("STEP 9.11AP — FINAL GOLD STAGING AUDIT SUMMARY")
print("=" * 75)

print("Expected Gold tables:", len(GOLD_TABLES))
print("Gold tables passed:", passed_tables)
print("Gold tables failed:", len(failed_tables))

print("\nExpected total rows:", expected_total_rows)
print("Production total rows:", production_total_rows)
print("Staging total rows:", staging_total_rows)

print("\nForward mismatches:", total_forward_mismatches)
print("Reverse mismatches:", total_reverse_mismatches)

print("\nMissing Gold tables:", sorted(missing_inventory))
print("Failed Gold tables:", failed_tables)
print("Nullability metadata differences:", nullability_differences)

# ------------------------------------------------------------
# 7. Final pass/fail gate
# ------------------------------------------------------------

assert passed_tables == 17, (
    f"Gold staging validation failed: {failed_tables}"
)

assert production_total_rows == expected_total_rows
assert staging_total_rows == expected_total_rows

assert total_forward_mismatches == 0
assert total_reverse_mismatches == 0

print("\n" + "=" * 75)
print("STEP 9.11AP — FULL GOLD STAGING AUDIT: PASS")
print("ALL 17 GOLD STAGING TABLES VERIFIED")
print("ALL 17 PRODUCTION COMPARISONS VERIFIED")
print("NO DATA MISMATCHES")
print("PRODUCTION AND STAGING TABLES UNCHANGED")
print("=" * 75)


# COMMAND ----------


# ============================================================
# STEP 9.12A — WORKFLOW ORCHESTRATION READINESS
# AssetFusion360 | Databricks
#
# Read-only:
# - Checks completed staging tables
# - Confirms Gold audit results
# - Defines future workflow task dependencies
# - Does NOT create jobs or modify tables
# ============================================================

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

# ------------------------------------------------------------
# 1. Confirm the previous 17-table audit passed
# ------------------------------------------------------------

try:
    previous_audit = audit_results
except NameError:
    raise RuntimeError(
        "Run Step 9.11AP before workflow preparation."
    )

assert len(previous_audit) == 17

failed = [
    row["table_name"]
    for row in previous_audit
    if row["status"] != "PASS"
]

assert not failed, (
    f"Gold audit has failed tables: {failed}"
)

print("GOLD AUDIT: 17/17 PASS")

# ------------------------------------------------------------
# 2. Define the complete Gold inventory
# ------------------------------------------------------------

gold_core = [
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
    "dim_part"
]

gold_monthly = [
    "gold_workorder_monthly_kpis",
    "gold_labor_monthly_kpis",
    "gold_transaction_monthly_kpis",
    "gold_material_monthly_kpis"
]

all_gold = gold_core + gold_monthly

assert len(all_gold) == 17
assert len(set(all_gold)) == 17

# ------------------------------------------------------------
# 3. Check persistence of all Gold staging tables
# ------------------------------------------------------------

for table in all_gold:
    assert spark.catalog.tableExists(
        f"{STAGING}.{table}"
    ), f"Missing staging table: {table}"

print("GOLD STAGING PERSISTENCE: PASS")

# ------------------------------------------------------------
# 4. Define future workflow stages and dependencies
#
# This is the intended logical orchestration graph.
# It does not yet create Databricks jobs.
# ------------------------------------------------------------

workflow_stages = [
    {
        "task": "validate_sources",
        "depends_on": [],
        "purpose": "Validate upstream source availability"
    },
    {
        "task": "silver_transformations",
        "depends_on": ["validate_sources"],
        "purpose": "Build and validate 5 Silver staging tables"
    },
    {
        "task": "gold_core_transformations",
        "depends_on": ["silver_transformations"],
        "purpose": "Build and validate 13 Gold core tables"
    },
    {
        "task": "gold_monthly_kpis",
        "depends_on": ["gold_core_transformations"],
        "purpose": "Build and validate 4 Gold monthly KPI tables"
    },
    {
        "task": "final_quality_validation",
        "depends_on": ["gold_monthly_kpis"],
        "purpose": "Verify staging row counts, schemas and equality"
    }
]

# Validate dependency names and ordering
known_tasks = set()

for stage in workflow_stages:
    for dependency in stage["depends_on"]:
        assert dependency in known_tasks, (
            f"Missing or out-of-order dependency: {dependency}"
        )

    assert stage["task"] not in known_tasks
    known_tasks.add(stage["task"])

# ------------------------------------------------------------
# 5. Display proposed task order
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("STEP 9.12A — DATABRICKS WORKFLOW READINESS")
print("=" * 70)

for index, stage in enumerate(workflow_stages, 1):
    print(f"\nTask {index}: {stage['task']}")
    print("  Depends on:", stage["depends_on"] or "None")
    print("  Purpose:", stage["purpose"])

print("\nGold core tables:", len(gold_core))
print("Gold monthly KPI tables:", len(gold_monthly))
print("Gold staging tables:", len(all_gold))
print("Gold audit passed:", len(previous_audit))
print("Proposed workflow tasks:", len(workflow_stages))

print("\nSTEP 9.12A WORKFLOW READINESS: PASS")
print("NO JOBS CREATED OR MODIFIED")
print("NO DELTA TABLES MODIFIED")
