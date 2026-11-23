# Databricks notebook source

# Step 9.3A — Initialize AssetFusion360 cloud storage.
# Create the schema and managed volume for automated Parquet uploads.
# Safe to rerun; existing objects will not be overwritten.

catalog = "workspace"
schema = "assetfusion360"
volume = "source_files"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
""")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}
""")

# Verify the schema and volume exist.
schema_row = spark.sql(f"""
SHOW SCHEMAS IN {catalog} LIKE '{schema}'
""").collect()

volume_rows = spark.sql(f"""
SHOW VOLUMES IN {catalog}.{schema}
""").collect()

assert schema_row, "AssetFusion360 schema not found"
assert any(
    row["volume_name"] == volume
    for row in volume_rows
), "AssetFusion360 upload volume not found"

print("Schema:", f"{catalog}.{schema}")
print("Volume:", f"{catalog}.{schema}.{volume}")
print("Upload location:", f"/Volumes/{catalog}/{schema}/{volume}/")

print("\nSTEP 9.3A CLOUD STORAGE SETUP: PASS")


# COMMAND ----------


# Step 9.5A — Automatically import all five Silver datasets into Delta Lake.
# Skip existing tables, preserve source Parquet files, and stop on failures.
# This imports validated Silver data; it does not rerun Bronze cleaning.

from pyspark.sql import functions as F

catalog = "workspace"
schema = "assetfusion360"
volume = "source_files"

datasets = [
    "amps_assets",
    "amps_workorders",
    "amps_labor",
    "amps_transactions",
    "amps_transaction_details"
]

base = f"/Volumes/{catalog}/{schema}/{volume}/silver"

results = []

for dataset in datasets:

    source = f"{base}/{dataset}"
    target = f"{catalog}.{schema}.silver_{dataset}"

    print(f"\nPROCESSING: {dataset}")

    # Step 1: Preserve existing Delta tables
    if spark.catalog.tableExists(target):
        print(f"SKIPPED — Table already exists: {target}")
        results.append((dataset, "SKIPPED", None))
        continue

    # Step 2: Read the uploaded Silver Parquet files
    df = spark.read.parquet(source)

    # Step 3: Register temporary Spark view
    view = f"stage_{dataset}"
    df.createOrReplaceTempView(view)

    # Step 4: Create managed Delta table without overwriting
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target}
        USING DELTA
        AS SELECT * FROM {view}
    """)

    # Step 5: Confirm the table exists
    assert spark.catalog.tableExists(target)

    # Step 6: Capture record count
    count = spark.table(target).count()

    print(f"CREATED: {target}")
    print(f"ROWS: {count:,}")

    results.append((dataset, "CREATED", count))

    spark.catalog.dropTempView(view)

# Step 7: Display execution summary
print("\n" + "=" * 65)
print("ASSETFUSION360 — AUTOMATED SILVER DELTA TABLE CREATION")
print("=" * 65)

for name, status, count in results:
    count_text = f"{count:,}" if count is not None else "Existing"
    print(f"{name:30} {status:10} {count_text}")

print("\nSTEP 9.5A SILVER DELTA AUTOMATION: PASS")
print("SOURCE PARQUET FILES UNCHANGED")


# COMMAND ----------


# Step 9.5B — Automatically import all five Bronze datasets into Delta Lake.
# Skip existing tables and preserve uploaded source Parquet files.
# Report created tables and record counts without overwriting data.

datasets = [
    "amps_assets",
    "amps_workorders",
    "amps_labor",
    "amps_transactions",
    "amps_transaction_details"
]

base = "/Volumes/workspace/assetfusion360/source_files/bronze"
results = []

for dataset in datasets:
    target = f"workspace.assetfusion360.bronze_{dataset}"
    source = f"{base}/{dataset}"

    print(f"\nPROCESSING: {dataset}")

    if spark.catalog.tableExists(target):
        count = spark.table(target).count()
        print(f"SKIPPED: {target} ({count:,} rows)")
        results.append((dataset, "SKIPPED", count))
        continue

    df = spark.read.parquet(source)
    view = f"stage_bronze_{dataset}"

    df.createOrReplaceTempView(view)

    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target}
            USING DELTA
            AS SELECT * FROM {view}
        """)

        count = spark.table(target).count()

        print(f"CREATED: {target}")
        print(f"ROWS: {count:,}")

        results.append((dataset, "CREATED", count))
    finally:
        spark.catalog.dropTempView(view)

print("\n" + "=" * 65)
print("ASSETFUSION360 — AUTOMATED BRONZE DELTA CREATION")
print("=" * 65)

for name, status, count in results:
    print(f"{name:30} {status:10} {count:,}")

assert len(results) == len(datasets)

print("\nSTEP 9.5B BRONZE DELTA AUTOMATION: PASS")
print("SOURCE PARQUET FILES UNCHANGED")


# COMMAND ----------


# Step 9.6D — Import all 17 validated Gold datasets into Delta Lake.
# Discover Gold folders automatically and skip existing Delta tables.
# Preserve the uploaded Parquet files and existing Bronze/Silver tables.

base = "/Volumes/workspace/assetfusion360/source_files/gold"
schema = "workspace.assetfusion360"

# Step 1: Find uploaded Gold dataset folders
folders = [
    item for item in dbutils.fs.ls(base)
    if item.isDir()
]

assert len(folders) == 17, (
    f"Expected 17 Gold datasets, found {len(folders)}"
)

# Step 2: Import each Gold dataset automatically
results = []

for folder in sorted(folders, key=lambda x: x.name):
    dataset = folder.name.rstrip("/")
    target = f"{schema}.{dataset}"

    print(f"\nPROCESSING: {dataset}")

    # Do not overwrite existing tables
    if spark.catalog.tableExists(target):
        count = spark.table(target).count()
        results.append((dataset, "SKIPPED", count))
        print(f"SKIPPED: {target}")
        continue

    # Read existing validated Gold Parquet files
    df = spark.read.parquet(folder.path)

    # Create temporary view
    view = f"stage_gold_{dataset}"
    df.createOrReplaceTempView(view)

    try:
        # Import as a managed Delta table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target}
            USING DELTA
            AS SELECT * FROM {view}
        """)

        # Verify table and capture row count
        count = spark.table(target).count()

        print(f"CREATED: {target}")
        print(f"ROWS: {count:,}")

        results.append((dataset, "CREATED", count))

    finally:
        spark.catalog.dropTempView(view)

# Step 3: Display summary
print("\n" + "=" * 65)
print("ASSETFUSION360 — GOLD DELTA IMPORT SUMMARY")
print("=" * 65)

for dataset, status, count in results:
    print(f"{dataset:35} {status:10} {count:,}")

assert len(results) == 17

print("\nSTEP 9.6D GOLD DELTA AUTOMATION: PASS")
print("SOURCE PARQUET FILES UNCHANGED")


# COMMAND ----------


# Step 9.6E — Validate the complete AssetFusion360 cloud warehouse.
# Verify 27 Delta tables, expected row counts, and Delta storage format.
# Read-only: no existing data or tables will be modified.

datasets = {
    "amps_assets": 148522,
    "amps_workorders": 2231535,
    "amps_labor": 3230423,
    "amps_transactions": 755122,
    "amps_transaction_details": 2301927,
}

gold_expected = {
    "bridge_asset_code": 148521,
    "dim_asset": 148522,
    "dim_date": 9356,
    "dim_detail_type": 6,
    "dim_part": 14423,
    "dim_transaction_status": 3,
    "dim_transaction_type": 6,
    "dim_workorder_priority": 11,
    "dim_workorder_type": 7,
    "fact_labor": 3230423,
    "fact_transaction_details": 2301927,
    "fact_transactions": 755122,
    "fact_workorders": 2231535,
    "gold_labor_monthly_kpis": 232,
    "gold_material_monthly_kpis": 12267,
    "gold_transaction_monthly_kpis": 1689,
    "gold_workorder_monthly_kpis": 1280,
}

expected = {}

# Step 1: Register expected Bronze/Silver counts
for layer in ["bronze", "silver"]:
    for dataset, count in datasets.items():
        expected[f"{layer}_{dataset}"] = count

# Step 2: Register expected Gold counts
expected.update(gold_expected)

# Step 3: Check every table automatically
errors = []
total_rows = 0

for name, expected_count in expected.items():
    table_name = f"workspace.assetfusion360.{name}"

    if not spark.catalog.tableExists(table_name):
        errors.append(f"Missing table: {name}")
        continue

    actual_count = spark.table(table_name).count()

    details = spark.sql(
        f"DESCRIBE DETAIL {table_name}"
    )

    table_format = details.select("format").first()[0]

    if actual_count != expected_count:
        errors.append(
            f"{name}: expected {expected_count}, got {actual_count}"
        )

    if table_format.lower() != "delta":
        errors.append(
            f"{name}: expected Delta, found {table_format}"
        )

    total_rows += actual_count

    print(
        f"{name:38} "
        f"rows={actual_count:>10,} "
        f"format={table_format}"
    )

# Step 4: Validate consolidated results
assert len(expected) == 27
assert not errors, "\n".join(errors)

print("\n" + "=" * 65)
print("ASSETFUSION360 CLOUD WAREHOUSE SUMMARY")
print("=" * 65)

print("Verified Delta tables:", len(expected))
print("Combined table rows:", f"{total_rows:,}")
print("Validation errors:", len(errors))

print("\nSTEP 9.6E CLOUD WAREHOUSE VALIDATION: PASS")
print("READ-ONLY - NO DATA MODIFIED")


# COMMAND ----------


# Step 9.7B — Migrate the production Gold asset transformation to PySpark.
# Reproduce identity, asset-code ambiguity, and business-field rules.
# Compare against the validated Gold Delta table without modifying data.

from pyspark.sql import functions as F

silver = spark.table("workspace.assetfusion360.silver_amps_assets")
existing_gold = spark.table("workspace.assetfusion360.dim_asset")

# Step 1: Normalize business asset codes
assets = silver.withColumn(
    "_asset_code",
    F.when(
        F.length(F.trim(F.col("OBJ_CODE"))) > 0,
        F.trim(F.col("OBJ_CODE"))
    )
)

# Step 2: Verify asset identities
assert assets.count() == 148522
assert assets.filter(
    F.col("asset_identity_int").isNull()
).count() == 0

assert (
    assets.select("asset_identity_int").distinct().count()
    == 148522
)

# Step 3: Count occurrences of each non-missing asset code
code_counts = (
    assets
    .filter(F.col("_asset_code").isNotNull())
    .groupBy("_asset_code")
    .agg(
        F.count("*")
        .cast("int")
        .alias("_code_count")
    )
)

# Step 4: Identify ambiguous codes
ambiguous = code_counts.filter(F.col("_code_count") > 1)

ambiguous_codes = {
    row["_asset_code"]: row["_code_count"]
    for row in ambiguous.collect()
}

assert ambiguous_codes == {"B223PA": 2}

# Step 5: Apply the original Gold business rules
gold_transformed = (
    assets.alias("a")
    .join(
        code_counts.alias("c"),
        F.col("a._asset_code") == F.col("c._asset_code"),
        "left"
    )
    .select(
        F.col("a.asset_identity_int")
         .cast("long")
         .alias("asset_identity"),

        F.col("a._asset_code").alias("asset_code"),

        F.col("a.OBJ_STATUS").alias("asset_status"),

        F.col("a.OBJ_PARENT").alias("parent_asset_code"),

        F.col("a.asset_commissioned_ts")
         .alias("commissioned_at"),

        F.col("a.asset_value_decimal")
         .cast("decimal(20,6)")
         .alias("asset_value"),

        F.coalesce(
            F.col("c._code_count"), F.lit(0)
        ).cast("int").alias("asset_code_match_count"),

        (F.coalesce(
            F.col("c._code_count"), F.lit(0)
        ) > 1).alias("asset_code_ambiguous"),

        (
            F.col("a._asset_code").isNotNull()
            & F.upper(F.col("a._asset_code")).isin(
                "*", "N/A", "NA", "NULL", "UNKNOWN"
            )
        ).alias("asset_code_placeholder"),

        F.col("a._bronze_source_file").alias("source_file")
    )
)

# Step 6: Validate the transformed output
assert gold_transformed.count() == 148522
assert len(gold_transformed.columns) == 10

assert (
    gold_transformed.filter(
        F.col("asset_code_ambiguous")
    ).count() == 2
)

# Step 7: Compare the actual data against production Gold
assert gold_transformed.columns == existing_gold.columns, (
    "Gold column structure mismatch"
)

forward_mismatches = (
    gold_transformed.exceptAll(existing_gold).count()
)

reverse_mismatches = (
    existing_gold.exceptAll(gold_transformed).count()
)

assert forward_mismatches == 0, (
    f"Transformed-to-Gold mismatches: {forward_mismatches}"
)

assert reverse_mismatches == 0, (
    f"Gold-to-transformed mismatches: {reverse_mismatches}"
)

# Step 8: Final summary
print("=" * 65)
print("STEP 9.7B — PRODUCTION GOLD ASSET TRANSFORMATION")
print("=" * 65)

print("Silver source rows:", silver.count())
print("Transformed Gold rows:", gold_transformed.count())
print("Production Gold rows:", existing_gold.count())
print("Gold columns:", len(gold_transformed.columns))
print("Ambiguous asset codes:", ambiguous_codes)
print("Data mismatches:", forward_mismatches + reverse_mismatches)

print("\nSTEP 9.7B GOLD TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")
