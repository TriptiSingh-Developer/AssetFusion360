# Databricks notebook source

# ============================================================
# AssetFusion360 — Workflow 02: Silver Transformations
# Step 9.12C — Silver Source Configuration
#
# READ-ONLY CONFIGURATION
# No tables are created or modified.
# ============================================================

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SILVER_TABLES = [
    "silver_amps_assets",
    "silver_amps_labor",
    "silver_amps_transaction_details",
    "silver_amps_transactions",
    "silver_amps_workorders"
]

assert len(SILVER_TABLES) == 5

# Verify the five production Silver inputs
for table in SILVER_TABLES:
    production_table = f"{PRODUCTION}.{table}"

    assert spark.catalog.tableExists(production_table), (
        f"Production Silver input missing: {production_table}"
    )

# Verify the existing staging outputs
for table in SILVER_TABLES:
    staging_table = f"{STAGING}.{table}"

    assert spark.catalog.tableExists(staging_table), (
        f"Silver staging table missing: {staging_table}"
    )

print("=" * 65)
print("WORKFLOW 02 — SILVER CONFIGURATION")
print("=" * 65)

for table in SILVER_TABLES:
    print("READY:", table)

print("\nSILVER CONFIGURATION: PASS")
print("NO DATA MODIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.12D — SILVER SOURCE DISCOVERY
# AssetFusion360 | Databricks Free Edition
#
# Inspect Bronze inputs and Silver output schemas.
# READ-ONLY — No tables created, deleted or overwritten.
# ============================================================

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SILVER_TABLES = [
    "silver_amps_assets",
    "silver_amps_labor",
    "silver_amps_transaction_details",
    "silver_amps_transactions",
    "silver_amps_workorders"
]

# 1. Discover existing production tables
production_inventory = sorted(
    t.name
    for t in spark.catalog.listTables(PRODUCTION)
    if not t.isTemporary
)

bronze_tables = sorted(
    name for name in production_inventory
    if name.startswith("bronze_")
)

assert len(bronze_tables) == 5, (
    f"Expected 5 Bronze tables, found "
    f"{len(bronze_tables)}: {bronze_tables}"
)

print("=" * 70)
print("STEP 9.12D — BRONZE SOURCE DISCOVERY")
print("=" * 70)

print("\nDiscovered Bronze tables:")

for name in bronze_tables:
    print(" -", name)

# 2. Verify the five existing Silver reference tables
print("\nSILVER REFERENCE TABLES")

for name in SILVER_TABLES:
    assert spark.catalog.tableExists(
        f"{PRODUCTION}.{name}"
    )

    assert spark.catalog.tableExists(
        f"{STAGING}.{name}"
    )

    print("READY:", name)

# 3. Inspect schemas and record counts
# Counts allow us to confirm the source inventory.
inventory = []

for layer, schema_name, table_names in [
    ("BRONZE", PRODUCTION, bronze_tables),
    ("SILVER_PRODUCTION", PRODUCTION, SILVER_TABLES),
    ("SILVER_STAGING", STAGING, SILVER_TABLES)
]:

    for table_name in table_names:

        full_name = f"{schema_name}.{table_name}"
        df = spark.table(full_name)

        metadata = spark.sql(
            f"DESCRIBE DETAIL {full_name}"
        ).first()

        assert metadata["format"].lower() == "delta"

        inventory.append({
            "layer": layer,
            "table_name": table_name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "delta_format": metadata["format"]
        })

inventory_df = spark.createDataFrame(inventory)

print("\nSOURCE AND TARGET INVENTORY")
display(
    inventory_df.orderBy("layer", "table_name")
)

# 4. Inspect Bronze column names and data types
print("\n" + "=" * 70)
print("BRONZE SOURCE SCHEMAS")
print("=" * 70)

for table_name in bronze_tables:

    print(f"\nTABLE: {table_name}")

    df = spark.table(f"{PRODUCTION}.{table_name}")

    for field in df.schema.fields:
        print(
            f"  {field.name}: "
            f"{field.dataType.simpleString()}"
        )

# 5. Report readiness for transformation implementation
print("\n" + "=" * 70)
print("STEP 9.12D — SOURCE DISCOVERY SUMMARY")
print("=" * 70)

print("Bronze input tables:", len(bronze_tables))
print("Silver production references:", len(SILVER_TABLES))
print("Silver staging outputs:", len(SILVER_TABLES))

print("\nSTEP 9.12D SOURCE DISCOVERY: PASS")
print("NO DATA OR TABLES MODIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.12E — BRONZE TO SILVER MAPPING VALIDATION
# AssetFusion360 | Databricks Free Edition
#
# READ ONLY:
# - Discover Bronze-to-Silver mappings
# - Compare Bronze and Silver schemas
# - Verify existing Silver staging data
# - Never create, overwrite, or delete tables
# ============================================================

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SILVER_TABLES = [
    "silver_amps_assets",
    "silver_amps_labor",
    "silver_amps_transaction_details",
    "silver_amps_transactions",
    "silver_amps_workorders"
]

# 1. Discover Bronze source tables
bronze_tables = sorted(
    t.name
    for t in spark.catalog.listTables(PRODUCTION)
    if t.name.startswith("bronze_") and not t.isTemporary
)

assert len(bronze_tables) == 5, (
    f"Expected 5 Bronze sources; found: {bronze_tables}"
)

# 2. Establish mappings using table-name correspondence
# Example: bronze_amps_assets -> silver_amps_assets
BRONZE_TO_SILVER = {}

for silver_name in SILVER_TABLES:

    expected_bronze = silver_name.replace(
        "silver_", "bronze_", 1
    )

    assert expected_bronze in bronze_tables, (
        f"No matching Bronze source for {silver_name}. "
        f"Expected: {expected_bronze}. "
        f"Available: {bronze_tables}"
    )

    BRONZE_TO_SILVER[expected_bronze] = silver_name

assert len(BRONZE_TO_SILVER) == 5

print("=" * 70)
print("STEP 9.12E — BRONZE TO SILVER MAPPINGS")
print("=" * 70)

for bronze, silver in BRONZE_TO_SILVER.items():
    print(f"{bronze}  -->  {silver}")

# 3. Compare schemas and staging data
mapping_report = []

for bronze, silver in BRONZE_TO_SILVER.items():

    bronze_df = spark.table(f"{PRODUCTION}.{bronze}")
    production_df = spark.table(f"{PRODUCTION}.{silver}")
    staging_df = spark.table(f"{STAGING}.{silver}")

    bronze_columns = set(bronze_df.columns)
    silver_columns = set(production_df.columns)

    # Columns introduced by Silver transformations
    added_columns = sorted(
        silver_columns - bronze_columns
    )

    # Bronze columns omitted or renamed in Silver
    removed_or_renamed = sorted(
        bronze_columns - silver_columns
    )

    # Existing Silver staging must equal production
    assert staging_df.schema.names == production_df.schema.names

    for field in production_df.schema.fields:
        assert (
            staging_df.schema[field.name].dataType
            == field.dataType
        ), f"Silver schema mismatch: {silver}.{field.name}"

    forward = staging_df.exceptAll(production_df).count()
    reverse = production_df.exceptAll(staging_df).count()

    assert forward == 0 and reverse == 0, (
        f"Silver staging data mismatch: {silver}"
    )

    mapping_report.append({
        "bronze_table": bronze,
        "silver_table": silver,
        "bronze_rows": bronze_df.count(),
        "silver_rows": production_df.count(),
        "bronze_columns": len(bronze_df.columns),
        "silver_columns": len(production_df.columns),
        "added_columns": ", ".join(added_columns),
        "removed_or_renamed_columns": ", ".join(
            removed_or_renamed
        ),
        "staging_mismatches": forward + reverse,
        "status": "PASS"
    })

# 4. Display mapping report
report_df = spark.createDataFrame(mapping_report)

print("\nBRONZE TO SILVER MAPPING REPORT")

display(report_df.orderBy("bronze_table"))

# 5. Display exact column mappings for further implementation
for bronze, silver in BRONZE_TO_SILVER.items():

    bronze_df = spark.table(f"{PRODUCTION}.{bronze}")
    silver_df = spark.table(f"{PRODUCTION}.{silver}")

    print("\n" + "-" * 70)
    print(f"SOURCE: {bronze}")
    print(f"TARGET: {silver}")
    print("-" * 70)

    bronze_schema = {
        field.name: field.dataType.simpleString()
        for field in bronze_df.schema.fields
    }

    silver_schema = {
        field.name: field.dataType.simpleString()
        for field in silver_df.schema.fields
    }

    all_columns = sorted(
        set(bronze_schema) | set(silver_schema)
    )

    for column in all_columns:
        source_type = bronze_schema.get(column, "<ABSENT>")
        target_type = silver_schema.get(column, "<ABSENT>")

        if source_type == target_type:
            change = "UNCHANGED"
        elif source_type == "<ABSENT>":
            change = "ADDED"
        elif target_type == "<ABSENT>":
            change = "REMOVED / RENAMED"
        else:
            change = "TYPE CHANGED"

        print(
            f"{column}: "
            f"{source_type} -> {target_type} "
            f"[{change}]"
        )

# 6. Final readiness result
print("\n" + "=" * 70)
print("STEP 9.12E — SILVER MAPPING VALIDATION SUMMARY")
print("=" * 70)

print("Bronze source tables:", len(BRONZE_TO_SILVER))
print("Silver target tables:", len(SILVER_TABLES))
print("Validated staging comparisons:", len(mapping_report))
print("Total staging mismatches:", sum(
    r["staging_mismatches"] for r in mapping_report
))

print("\nSTEP 9.12E SILVER MAPPINGS: PASS")
print("NO TABLES MODIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.12F — INDEPENDENT SILVER ASSETS TRANSFORMATION
# AssetFusion360 | Databricks
#
# Source: Production Bronze assets
# Reference: Production Silver assets
#
# READ-ONLY:
# - Rebuild transformation in memory
# - Preserve every Bronze record and source column
# - Validate against production Silver
# - Never overwrite existing Delta tables
# ============================================================

import re

from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql import types as T

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

BRONZE = f"{PRODUCTION}.bronze_amps_assets"
REFERENCE = f"{PRODUCTION}.silver_amps_assets"
STAGING_REFERENCE = f"{STAGING}.silver_amps_assets"

EXPECTED_ROWS = 148522
EXPECTED_BRONZE_COLUMNS = 32
EXPECTED_SILVER_COLUMNS = 43

# ------------------------------------------------------------
# 1. Verify source and reference tables
# ------------------------------------------------------------

for table in [BRONZE, REFERENCE, STAGING_REFERENCE]:
    assert spark.catalog.tableExists(table), (
        f"Required table missing: {table}"
    )

bronze = spark.table(BRONZE)
reference = spark.table(REFERENCE)

assert bronze.count() == EXPECTED_ROWS
assert len(bronze.columns) == EXPECTED_BRONZE_COLUMNS
assert len(reference.columns) == EXPECTED_SILVER_COLUMNS

required = [
    "OBJ_CODE",
    "OBJ_SQLIDENTITY",
    "OBJ_COMMISS",
    "OBJ_CREATED",
    "OBJ_VALUE"
]

assert all(name in bronze.columns for name in required)

assert all(
    isinstance(field.dataType, T.StringType)
    for field in bronze.schema.fields
), "Unexpected Bronze field type."

# ------------------------------------------------------------
# 2. Reproduce the original Python parsing functions
# ------------------------------------------------------------

def is_missing(value):
    return value is None or str(value).strip() == ""


def parse_integer(value):
    if is_missing(value):
        return None, False

    text = str(value).strip()

    if not re.fullmatch(r"[+-]?\d+", text):
        return None, True

    try:
        result = int(text)
    except ValueError:
        return None, True

    if not -(2**63) <= result < 2**63:
        return None, True

    return result, False


def parse_decimal(value):
    if is_missing(value):
        return None, False

    try:
        result = Decimal(str(value).strip())

        if not result.is_finite():
            return None, True

        result = result.quantize(
            Decimal("0.000001")
        )

        if abs(result) >= Decimal("100000000000000"):
            return None, True

        return result, False

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


DATE_FORMATS = [
    "%b %d %Y %I:%M%p",
    "%b %d %Y %I:%M:%S%p",
    "%b %d %Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d"
]


def parse_timestamp(value):
    if is_missing(value):
        return None, False

    text = re.sub(
        r"\s+",
        " ",
        str(value).strip()
    )

    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(
                text, date_format
            ), False
        except ValueError:
            continue

    return None, True

# ------------------------------------------------------------
# 3. Reproduce original per-record Silver transformation
#
# A Python UDF preserves the original parser behavior,
# including malformed values and decimal rounding.
# ------------------------------------------------------------

parsed_schema = T.StructType([
    T.StructField("asset_identity_int", T.LongType()),
    T.StructField("asset_commissioned_ts", T.TimestampType()),
    T.StructField("asset_created_ts", T.TimestampType()),
    T.StructField("asset_value_decimal", T.DecimalType(20, 6)),
    T.StructField("dq_asset_code_missing", T.BooleanType()),
    T.StructField("dq_asset_code_placeholder", T.BooleanType()),
    T.StructField("dq_asset_identity_invalid", T.BooleanType()),
    T.StructField("dq_commission_date_invalid", T.BooleanType()),
    T.StructField("dq_created_date_invalid", T.BooleanType()),
    T.StructField("dq_asset_value_invalid", T.BooleanType()),
    T.StructField("dq_any_issue", T.BooleanType())
])


@F.udf(returnType=parsed_schema)
def transform_asset(
    asset_code,
    raw_identity,
    raw_commissioned,
    raw_created,
    raw_value
):
    identity, identity_invalid = parse_integer(raw_identity)
    commissioned, commissioned_invalid = parse_timestamp(
        raw_commissioned
    )
    created, created_invalid = parse_timestamp(raw_created)
    value, value_invalid = parse_decimal(raw_value)

    code_missing = is_missing(asset_code)

    code_placeholder = (
        not code_missing
        and str(asset_code).strip().upper()
        in {"*", "N/A", "NA", "NULL", "UNKNOWN"}
    )

    identity_missing = is_missing(raw_identity)

    any_issue = any([
        code_missing,
        code_placeholder,
        identity_invalid,
        identity_missing,
        commissioned_invalid,
        created_invalid,
        value_invalid
    ])

    return (
        identity,
        commissioned,
        created,
        value,
        code_missing,
        code_placeholder,
        identity_invalid or identity_missing,
        commissioned_invalid,
        created_invalid,
        value_invalid,
        any_issue
    )

# ------------------------------------------------------------
# 4. Transform Bronze into the Silver schema
# ------------------------------------------------------------

transformed = bronze.withColumn(
    "_silver_parsed",
    transform_asset(
        F.col("OBJ_CODE"),
        F.col("OBJ_SQLIDENTITY"),
        F.col("OBJ_COMMISS"),
        F.col("OBJ_CREATED"),
        F.col("OBJ_VALUE")
    )
)

new_field_names = [
    field.name for field in parsed_schema.fields
]

silver_assets_rebuilt = transformed.select(
    *[F.col(name) for name in bronze.columns],
    *[
        F.col(f"_silver_parsed.{name}").alias(name)
        for name in new_field_names
    ]
)

# Align column order and types with reference.
silver_assets_rebuilt = silver_assets_rebuilt.select(
    *[
        F.col(name)
        .cast(reference.schema[name].dataType)
        .alias(name)
        for name in reference.columns
    ]
)

# ------------------------------------------------------------
# 5. Validate source preservation and transformation
# ------------------------------------------------------------

assert silver_assets_rebuilt.count() == EXPECTED_ROWS

assert len(silver_assets_rebuilt.columns) == 43

assert (
    set(bronze.columns)
    .issubset(set(silver_assets_rebuilt.columns))
)

# Every original Bronze record must remain.
# exceptAll preserves duplicate multiplicity.
bronze_preserved = silver_assets_rebuilt.select(
    *bronze.columns
)

assert bronze_preserved.exceptAll(bronze).count() == 0
assert bronze.exceptAll(bronze_preserved).count() == 0

# ------------------------------------------------------------
# 6. Validate the exact production Silver schema
# ------------------------------------------------------------

assert (
    silver_assets_rebuilt.schema.names
    == reference.schema.names
)

for name in reference.columns:
    assert (
        silver_assets_rebuilt.schema[name].dataType
        == reference.schema[name].dataType
    ), f"Silver data type mismatch: {name}"

# ------------------------------------------------------------
# 7. Compare rebuilt Silver with existing production Silver
# ------------------------------------------------------------

forward = silver_assets_rebuilt.exceptAll(reference).count()
reverse = reference.exceptAll(silver_assets_rebuilt).count()

# ------------------------------------------------------------
# 8. Report independent transformation results
# ------------------------------------------------------------

print("\n" + "=" * 68)
print("STEP 9.12F — INDEPENDENT SILVER ASSETS VALIDATION")
print("=" * 68)

print("Bronze records:", bronze.count())
print("Bronze columns:", len(bronze.columns))
print("Rebuilt Silver records:", silver_assets_rebuilt.count())
print("Rebuilt Silver columns:", len(silver_assets_rebuilt.columns))
print("Original Bronze fields preserved: PASS")
print("Forward mismatches:", forward)
print("Reverse mismatches:", reverse)

assert forward == 0, (
    f"Rebuilt Silver differs from production: {forward} rows"
)

assert reverse == 0, (
    f"Production differs from rebuilt Silver: {reverse} rows"
)

print("\nSTEP 9.12F SILVER ASSETS TRANSFORMATION: PASS")
print("READ-ONLY — NO DELTA TABLES MODIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.12G — SILVER ASSETS WORKFLOW VALIDATION
# AssetFusion360 | Databricks Free Edition
#
# Verifies:
# 1. Rebuilt Silver matches production Silver
# 2. Rebuilt Silver matches existing staging Silver
# 3. All original Bronze records are preserved
# 4. Existing staging table remains unchanged
#
# READ-ONLY — NO TABLES CREATED OR OVERWRITTEN
# ============================================================

from pyspark.sql import functions as F

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

BRONZE_TABLE = f"{PRODUCTION}.bronze_amps_assets"
PRODUCTION_TABLE = f"{PRODUCTION}.silver_amps_assets"
STAGING_TABLE = f"{STAGING}.silver_amps_assets"

EXPECTED_ROWS = 148522
EXPECTED_BRONZE_COLUMNS = 32
EXPECTED_SILVER_COLUMNS = 43

# ------------------------------------------------------------
# 1. Verify independent transformation is available
# ------------------------------------------------------------

try:
    rebuilt = silver_assets_rebuilt
except NameError:
    raise RuntimeError(
        "Run Step 9.12F before this validation."
    )

bronze = spark.table(BRONZE_TABLE)
production = spark.table(PRODUCTION_TABLE)
staging = spark.table(STAGING_TABLE)

# ------------------------------------------------------------
# 2. Validate row counts and schemas
# ------------------------------------------------------------

assert bronze.count() == EXPECTED_ROWS
assert production.count() == EXPECTED_ROWS
assert staging.count() == EXPECTED_ROWS
assert rebuilt.count() == EXPECTED_ROWS

assert len(bronze.columns) == EXPECTED_BRONZE_COLUMNS
assert len(rebuilt.columns) == EXPECTED_SILVER_COLUMNS

assert rebuilt.schema.names == production.schema.names
assert staging.schema.names == production.schema.names

for column in production.columns:

    expected_type = production.schema[column].dataType

    assert rebuilt.schema[column].dataType == expected_type, (
        f"Rebuilt schema mismatch: {column}"
    )

    assert staging.schema[column].dataType == expected_type, (
        f"Staging schema mismatch: {column}"
    )

print("ROW COUNT AND SCHEMA VALIDATION: PASS")

# ------------------------------------------------------------
# 3. Verify original Bronze fields are preserved
# ------------------------------------------------------------

rebuilt_bronze_fields = rebuilt.select(*bronze.columns)

bronze_forward = (
    rebuilt_bronze_fields.exceptAll(bronze).count()
)

bronze_reverse = (
    bronze.exceptAll(rebuilt_bronze_fields).count()
)

assert bronze_forward == 0
assert bronze_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 4. Compare rebuilt Silver against production Silver
# ------------------------------------------------------------

production_forward = (
    rebuilt.exceptAll(production).count()
)

production_reverse = (
    production.exceptAll(rebuilt).count()
)

assert production_forward == 0
assert production_reverse == 0

print("REBUILT VS PRODUCTION SILVER: PASS")

# ------------------------------------------------------------
# 5. Compare rebuilt Silver against existing staging Silver
# ------------------------------------------------------------

staging_forward = (
    rebuilt.exceptAll(staging).count()
)

staging_reverse = (
    staging.exceptAll(rebuilt).count()
)

assert staging_forward == 0
assert staging_reverse == 0

print("REBUILT VS STAGING SILVER: PASS")

# ------------------------------------------------------------
# 6. Verify staging Delta table format
# ------------------------------------------------------------

metadata = spark.sql(
    f"DESCRIBE DETAIL {STAGING_TABLE}"
).first()

assert metadata["format"].lower() == "delta"

print("STAGING DELTA FORMAT: PASS")

# ------------------------------------------------------------
# 7. Display source data-quality indicators
# ------------------------------------------------------------

quality_columns = [
    column
    for column in rebuilt.columns
    if column.startswith("dq_")
]

quality_summary = rebuilt.agg(
    *[
        F.sum(
            F.when(F.col(column) == True, 1)
             .otherwise(0)
        ).alias(column)
        for column in quality_columns
    ]
)

print("\nSILVER ASSETS QUALITY FLAG COUNTS")

display(quality_summary)

# ------------------------------------------------------------
# 8. Final workflow validation report
# ------------------------------------------------------------

print("\n" + "=" * 68)
print("STEP 9.12G — SILVER ASSETS WORKFLOW VALIDATION")
print("=" * 68)

print("Bronze records:", bronze.count())
print("Production Silver records:", production.count())
print("Staging Silver records:", staging.count())
print("Rebuilt Silver records:", rebuilt.count())

print("Bronze columns:", len(bronze.columns))
print("Silver columns:", len(rebuilt.columns))

print("\nBronze preservation mismatches:",
      bronze_forward + bronze_reverse)

print("Production comparison mismatches:",
      production_forward + production_reverse)

print("Staging comparison mismatches:",
      staging_forward + staging_reverse)

print("\nSTEP 9.12G SILVER ASSETS WORKFLOW: PASS")
print("ALL EXISTING DELTA TABLES PRESERVED")
print("NO DATA MODIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.12H — SILVER WORK ORDERS TRANSFORMATION (CORRECTED)
# AssetFusion360 | Databricks Free Edition
#
# Source:
#   workspace.assetfusion360.bronze_amps_workorders
#
# Production reference:
#   workspace.assetfusion360.silver_amps_workorders
#
# Staging reference:
#   workspace.assetfusion360_staging.silver_amps_workorders
#
# FIX:
#   Removed cached date parser causing UDF serialization failure.
#   All parsing dependencies are defined inside the worker UDF.
#
# SAFETY:
#   READ-ONLY
#   NO TABLE CREATION
#   NO OVERWRITE
#   NO DELETION
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ------------------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------------------

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

BRONZE_TABLE = f"{PRODUCTION}.bronze_amps_workorders"

PRODUCTION_TABLE = (
    f"{PRODUCTION}.silver_amps_workorders"
)

STAGING_TABLE = (
    f"{STAGING}.silver_amps_workorders"
)

EXPECTED_ROWS = 2231535
EXPECTED_BRONZE_COLUMNS = 59
EXPECTED_SILVER_COLUMNS = 82

# ------------------------------------------------------------
# 2. VERIFY SOURCE TABLES
# ------------------------------------------------------------

for table_name in [
    BRONZE_TABLE,
    PRODUCTION_TABLE,
    STAGING_TABLE
]:
    assert spark.catalog.tableExists(table_name), (
        f"Required table missing: {table_name}"
    )

bronze_wo = spark.table(BRONZE_TABLE)

production_wo = spark.table(PRODUCTION_TABLE)

staging_wo = spark.table(STAGING_TABLE)

assert bronze_wo.count() == EXPECTED_ROWS
assert production_wo.count() == EXPECTED_ROWS
assert staging_wo.count() == EXPECTED_ROWS

assert len(bronze_wo.columns) == EXPECTED_BRONZE_COLUMNS

assert (
    len(production_wo.columns)
    == EXPECTED_SILVER_COLUMNS
)

assert (
    len(staging_wo.columns)
    == EXPECTED_SILVER_COLUMNS
)

print("SOURCE TABLE VALIDATION: PASS")

# ------------------------------------------------------------
# 3. DEFINE ORIGINAL SOURCE FIELDS
# ------------------------------------------------------------

DATE_FIELDS = [
    "evt_date",
    "evt_reported",
    "evt_start",
    "evt_due",
    "evt_completed",
    "evt_created",
    "evt_updated"
]

NUMERIC_FIELDS = [
    "evt_sqlidentity",
    "evt_duration",
    "evt_updatecount"
]

REQUIRED_FIELDS = [
    "evt_code",
    "evt_object",
    *DATE_FIELDS,
    *NUMERIC_FIELDS
]

for name in REQUIRED_FIELDS:
    assert name in bronze_wo.columns, (
        f"Missing Bronze column: {name}"
    )

for field in bronze_wo.schema.fields:
    assert isinstance(
        field.dataType,
        T.StringType
    ), f"Unexpected Bronze type: {field.name}"

# ------------------------------------------------------------
# 4. DEFINE EXACT SILVER ADDITIONAL FIELDS
# ------------------------------------------------------------

ADDED_SCHEMA = T.StructType(

    [
        T.StructField(
            f"{field}_ts",
            T.TimestampType(),
            True
        )
        for field in DATE_FIELDS
    ]

    + [
        T.StructField(
            "wo_identity_int",
            T.LongType(),
            True
        ),

        T.StructField(
            "wo_duration_decimal",
            T.DecimalType(20, 6),
            True
        ),

        T.StructField(
            "wo_updatecount_int",
            T.LongType(),
            True
        )
    ]

    + [
        T.StructField(
            f"dq_{field}_invalid",
            T.BooleanType(),
            True
        )
        for field in DATE_FIELDS
    ]

    + [
        T.StructField(
            "dq_identity_invalid",
            T.BooleanType(),
            True
        ),

        T.StructField(
            "dq_duration_invalid",
            T.BooleanType(),
            True
        ),

        T.StructField(
            "dq_updatecount_invalid",
            T.BooleanType(),
            True
        ),

        T.StructField(
            "dq_workorder_code_missing",
            T.BooleanType(),
            True
        ),

        T.StructField(
            "dq_asset_ref_missing",
            T.BooleanType(),
            True
        ),

        T.StructField(
            "dq_any_issue",
            T.BooleanType(),
            True
        )
    ]
)

assert len(ADDED_SCHEMA.fields) == 23

for field in ADDED_SCHEMA.fields:
    assert field.name not in bronze_wo.columns, (
        f"Silver field already exists in Bronze: {field.name}"
    )

# ------------------------------------------------------------
# 5. SELF-CONTAINED PYTHON UDF
#
# Important:
# No @lru_cache.
# No dependency on notebook-level helper functions.
# Python worker imports and parsing logic are self-contained.
# ------------------------------------------------------------

@F.udf(returnType=ADDED_SCHEMA)
def transform_workorder_record(
    evt_code,
    evt_object,
    evt_sqlidentity,
    evt_duration,
    evt_updatecount,
    *date_values
):

    import re

    from datetime import datetime

    from decimal import (
        Decimal,
        InvalidOperation
    )

    # --------------------------------------------------------
    # 5A. Missing-value detection
    # --------------------------------------------------------

    def missing(value):
        return (
            value is None
            or str(value).strip() == ""
        )

    # --------------------------------------------------------
    # 5B. Integer parsing
    # --------------------------------------------------------

    def parse_int(value):

        if missing(value):
            return None, False

        text = str(value).strip()

        if not re.fullmatch(r"[+-]?\d+", text):
            return None, True

        try:
            result = int(text)

        except ValueError:
            return None, True

        if not -(2**63) <= result < 2**63:
            return None, True

        return result, False

    # --------------------------------------------------------
    # 5C. Decimal(20,6) parsing
    # --------------------------------------------------------

    def parse_decimal(value):

        if missing(value):
            return None, False

        try:
            result = Decimal(
                str(value).strip()
            )

            if not result.is_finite():
                return None, True

            result = result.quantize(
                Decimal("0.000001")
            )

            if abs(result) >= Decimal(
                "100000000000000"
            ):
                return None, True

            return result, False

        except (
            InvalidOperation,
            ValueError,
            OverflowError
        ):
            return None, True

    # --------------------------------------------------------
    # 5D. Date parsing — corrected
    # --------------------------------------------------------

    date_formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y %H:%M:%S"
    ]

    def parse_date(value):

        if missing(value):
            return None, False

        normalized = re.sub(
            r"\s+",
            " ",
            str(value).strip()
        )

        for fmt in date_formats:

            try:
                parsed = datetime.strptime(
                    normalized,
                    fmt
                )

                return parsed, False

            except ValueError:
                continue

        return None, True

    # --------------------------------------------------------
    # 5E. Apply original transformation rules
    # --------------------------------------------------------

    result = {}
    flags = {}

    assert len(date_values) == 7

    for field, value in zip(
        [
            "evt_date",
            "evt_reported",
            "evt_start",
            "evt_due",
            "evt_completed",
            "evt_created",
            "evt_updated"
        ],
        date_values
    ):

        converted, invalid = parse_date(value)

        result[f"{field}_ts"] = converted

        flags[f"dq_{field}_invalid"] = invalid

    identity, identity_invalid = parse_int(
        evt_sqlidentity
    )

    duration, duration_invalid = parse_decimal(
        evt_duration
    )

    updatecount, updatecount_invalid = parse_int(
        evt_updatecount
    )

    result["wo_identity_int"] = identity

    result["wo_duration_decimal"] = duration

    result["wo_updatecount_int"] = updatecount

    flags["dq_identity_invalid"] = (
        identity_invalid
        or missing(evt_sqlidentity)
    )

    flags["dq_duration_invalid"] = duration_invalid

    flags["dq_updatecount_invalid"] = (
        updatecount_invalid
    )

    flags["dq_workorder_code_missing"] = missing(
        evt_code
    )

    flags["dq_asset_ref_missing"] = missing(
        evt_object
    )

    flags["dq_any_issue"] = any(
        flags.values()
    )

    result.update(flags)

    # Return in the exact original Silver field order
    output_names = (
        [
            f"{field}_ts"
            for field in [
                "evt_date",
                "evt_reported",
                "evt_start",
                "evt_due",
                "evt_completed",
                "evt_created",
                "evt_updated"
            ]
        ]
        + [
            "wo_identity_int",
            "wo_duration_decimal",
            "wo_updatecount_int"
        ]
        + [
            f"dq_{field}_invalid"
            for field in [
                "evt_date",
                "evt_reported",
                "evt_start",
                "evt_due",
                "evt_completed",
                "evt_created",
                "evt_updated"
            ]
        ]
        + [
            "dq_identity_invalid",
            "dq_duration_invalid",
            "dq_updatecount_invalid",
            "dq_workorder_code_missing",
            "dq_asset_ref_missing",
            "dq_any_issue"
        ]
    )

    return tuple(
        result[name]
        for name in output_names
    )

# ------------------------------------------------------------
# 6. BUILD SILVER TRANSFORMATION FROM BRONZE
# ------------------------------------------------------------

silver_workorders_rebuilt = (

    bronze_wo

    .withColumn(
        "_silver_parsed",

        transform_workorder_record(

            F.col("evt_code"),
            F.col("evt_object"),
            F.col("evt_sqlidentity"),
            F.col("evt_duration"),
            F.col("evt_updatecount"),

            *[
                F.col(field)
                for field in DATE_FIELDS
            ]

        )
    )

    .select(

        *[
            F.col(name)
            for name in bronze_wo.columns
        ],

        *[
            F.col(
                f"_silver_parsed.{field.name}"
            ).alias(field.name)
            for field in ADDED_SCHEMA.fields
        ]

    )
)

# ------------------------------------------------------------
# 7. ALIGN WITH PRODUCTION SILVER SCHEMA
# ------------------------------------------------------------

assert (
    set(silver_workorders_rebuilt.columns)
    == set(production_wo.columns)
), "Rebuilt Silver column set differs from production."

silver_workorders_rebuilt = (
    silver_workorders_rebuilt.select(

        *[
            F.col(name)
            .cast(
                production_wo.schema[name].dataType
            )
            .alias(name)

            for name in production_wo.columns
        ]

    )
)

assert (
    len(silver_workorders_rebuilt.columns)
    == EXPECTED_SILVER_COLUMNS
)

# ------------------------------------------------------------
# 8. VALIDATE ROW COUNT
# ------------------------------------------------------------

rebuilt_count = silver_workorders_rebuilt.count()

assert rebuilt_count == EXPECTED_ROWS, (
    f"Unexpected rebuilt rows: {rebuilt_count}"
)

print("REBUILT SILVER ROW COUNT: PASS")

# ------------------------------------------------------------
# 9. VERIFY ALL BRONZE SOURCE FIELDS ARE PRESERVED
# ------------------------------------------------------------

rebuilt_bronze_fields = (
    silver_workorders_rebuilt.select(
        *bronze_wo.columns
    )
)

bronze_forward = (
    rebuilt_bronze_fields
    .exceptAll(bronze_wo)
    .count()
)

bronze_reverse = (
    bronze_wo
    .exceptAll(rebuilt_bronze_fields)
    .count()
)

assert bronze_forward == 0
assert bronze_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 10. VALIDATE PRODUCTION AND STAGING SCHEMAS
# ------------------------------------------------------------

assert (
    silver_workorders_rebuilt.schema.names
    == production_wo.schema.names
)

assert (
    staging_wo.schema.names
    == production_wo.schema.names
)

for name in production_wo.columns:

    expected_type = (
        production_wo.schema[name].dataType
    )

    assert (
        silver_workorders_rebuilt.schema[name].dataType
        == expected_type
    ), f"Rebuilt schema mismatch: {name}"

    assert (
        staging_wo.schema[name].dataType
        == expected_type
    ), f"Staging schema mismatch: {name}"

print("SILVER SCHEMA VALIDATION: PASS")

# ------------------------------------------------------------
# 11. VERIFY REBUILT SILVER VS PRODUCTION
# ------------------------------------------------------------

print("CHECKING REBUILT VS PRODUCTION...")

production_forward = (
    silver_workorders_rebuilt
    .exceptAll(production_wo)
    .count()
)

production_reverse = (
    production_wo
    .exceptAll(silver_workorders_rebuilt)
    .count()
)

print(
    "Production forward mismatches:",
    production_forward
)

print(
    "Production reverse mismatches:",
    production_reverse
)

assert production_forward == 0, (
    f"Rebuilt Silver has {production_forward} "
    "records not matching production."
)

assert production_reverse == 0, (
    f"Production has {production_reverse} "
    "records not matching rebuilt Silver."
)

print("REBUILT VS PRODUCTION: PASS")

# ------------------------------------------------------------
# 12. VERIFY REBUILT SILVER VS STAGING
# ------------------------------------------------------------

print("CHECKING REBUILT VS STAGING...")

staging_forward = (
    silver_workorders_rebuilt
    .exceptAll(staging_wo)
    .count()
)

staging_reverse = (
    staging_wo
    .exceptAll(silver_workorders_rebuilt)
    .count()
)

print(
    "Staging forward mismatches:",
    staging_forward
)

print(
    "Staging reverse mismatches:",
    staging_reverse
)

assert staging_forward == 0, (
    f"Rebuilt Silver has {staging_forward} "
    "records not matching staging."
)

assert staging_reverse == 0, (
    f"Staging has {staging_reverse} "
    "records not matching rebuilt Silver."
)

print("REBUILT VS STAGING: PASS")

# ------------------------------------------------------------
# 13. VERIFY STAGING DELTA FORMAT
# ------------------------------------------------------------

staging_metadata = spark.sql(
    f"DESCRIBE DETAIL {STAGING_TABLE}"
).first()

assert (
    staging_metadata["format"].lower()
    == "delta"
)

print("STAGING DELTA FORMAT: PASS")

# ------------------------------------------------------------
# 14. FINAL VALIDATION REPORT
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("STEP 9.12H — SILVER WORK ORDERS VALIDATION")
print("=" * 70)

print("Bronze records:", EXPECTED_ROWS)
print("Bronze columns:", EXPECTED_BRONZE_COLUMNS)

print("Rebuilt Silver records:", rebuilt_count)
print("Rebuilt Silver columns:", EXPECTED_SILVER_COLUMNS)

print(
    "Bronze preservation mismatches:",
    bronze_forward + bronze_reverse
)

print(
    "Production comparison mismatches:",
    production_forward + production_reverse
)

print(
    "Staging comparison mismatches:",
    staging_forward + staging_reverse
)

assert (
    bronze_forward == 0
    and bronze_reverse == 0
    and production_forward == 0
    and production_reverse == 0
    and staging_forward == 0
    and staging_reverse == 0
)

print("\nSTEP 9.12H SILVER WORK ORDERS: PASS")
print("PYTHON UDF SERIALIZATION: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")


# COMMAND ----------


# ============================================================
# STEP 9.12I — INDEPENDENT SILVER TRANSACTIONS TRANSFORMATION
# AssetFusion360 | Databricks Free Edition
#
# Source:
#   workspace.assetfusion360.bronze_amps_transactions
#
# Production reference:
#   workspace.assetfusion360.silver_amps_transactions
#
# Staging reference:
#   workspace.assetfusion360_staging.silver_amps_transactions
#
# SAFETY:
# - Read-only transformation and validation
# - No Delta creation, overwrite, deletion or update
# - All original Bronze columns and records preserved
# - No caching decorators or external UDF dependencies
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

BRONZE_TABLE = f"{PRODUCTION}.bronze_amps_transactions"
PRODUCTION_TABLE = f"{PRODUCTION}.silver_amps_transactions"
STAGING_TABLE = f"{STAGING}.silver_amps_transactions"

EXPECTED_ROWS = 755122
EXPECTED_BRONZE_COLUMNS = 18
EXPECTED_SILVER_COLUMNS = 33

# ------------------------------------------------------------
# 1. Verify source and reference Delta tables
# ------------------------------------------------------------

for table_name in [
    BRONZE_TABLE,
    PRODUCTION_TABLE,
    STAGING_TABLE
]:
    assert spark.catalog.tableExists(table_name), (
        f"Required table missing: {table_name}"
    )

bronze_tx = spark.table(BRONZE_TABLE)
production_tx = spark.table(PRODUCTION_TABLE)
staging_tx = spark.table(STAGING_TABLE)

assert bronze_tx.count() == EXPECTED_ROWS
assert production_tx.count() == EXPECTED_ROWS
assert staging_tx.count() == EXPECTED_ROWS

assert len(bronze_tx.columns) == EXPECTED_BRONZE_COLUMNS
assert len(production_tx.columns) == EXPECTED_SILVER_COLUMNS
assert len(staging_tx.columns) == EXPECTED_SILVER_COLUMNS

print("SOURCE TABLE VALIDATION: PASS")

# ------------------------------------------------------------
# 2. Define the original transaction source fields
# ------------------------------------------------------------

INTEGER_FIELDS = {
    "tra_code": "transaction_code_int",
    "tra_sqlidentity": "transaction_identity_int",
    "tra_req": "transaction_request_int",
    "tra_updatecount": "transaction_updatecount_int"
}

DATE_FIELDS = {
    "tra_date": "transaction_date_ts",
    "tra_created": "transaction_created_ts",
    "tra_updated": "transaction_updated_ts"
}

INTEGER_FLAGS = {
    "tra_code": "dq_transaction_code_invalid",
    "tra_sqlidentity": "dq_transaction_identity_invalid",
    "tra_req": "dq_transaction_request_invalid",
    "tra_updatecount": "dq_transaction_updatecount_invalid"
}

DATE_FLAGS = {
    "tra_date": "dq_transaction_date_invalid",
    "tra_created": "dq_transaction_created_invalid",
    "tra_updated": "dq_transaction_updated_invalid"
}

QUALITY_FLAGS = (
    list(INTEGER_FLAGS.values())
    + list(DATE_FLAGS.values())
    + ["dq_any_issue"]
)

REQUIRED_FIELDS = [
    *INTEGER_FIELDS.keys(),
    *DATE_FIELDS.keys(),
    "tra_type",
    "tra_status",
    "tra_fromentity",
    "tra_fromcode",
    "tra_toentity",
    "tra_tocode"
]

for name in REQUIRED_FIELDS:
    assert name in bronze_tx.columns, (
        f"Missing Bronze column: {name}"
    )

for field in bronze_tx.schema.fields:
    assert isinstance(
        field.dataType,
        T.StringType
    ), f"Unexpected Bronze type: {field.name}"

print("BRONZE SOURCE SCHEMA: PASS")

# ------------------------------------------------------------
# 3. Define the exact original Silver additions
# ------------------------------------------------------------

ADDED_SCHEMA = T.StructType(

    [
        T.StructField(name, T.LongType(), True)
        for name in INTEGER_FIELDS.values()
    ]

    + [
        T.StructField(name, T.TimestampType(), True)
        for name in DATE_FIELDS.values()
    ]

    + [
        T.StructField(name, T.BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

assert len(ADDED_SCHEMA.fields) == 15

for field in ADDED_SCHEMA.fields:
    assert field.name not in bronze_tx.columns, (
        f"Silver field collision: {field.name}"
    )

# ------------------------------------------------------------
# 4. Self-contained Python UDF
#
# Reproduces original parsing rules.
# No lru_cache and no notebook-level helper dependencies.
# ------------------------------------------------------------

@F.udf(returnType=ADDED_SCHEMA)
def transform_transaction_record(
    tra_code,
    tra_sqlidentity,
    tra_req,
    tra_updatecount,
    tra_date,
    tra_created,
    tra_updated
):

    import re
    from datetime import datetime

    def missing(value):
        return (
            value is None
            or str(value).strip() == ""
        )

    # Original strict integer conversion
    def parse_integer(value):

        if missing(value):
            return None, False

        text = str(value).strip()

        if not re.fullmatch(r"[+-]?\d+", text):
            return None, True

        try:
            result = int(text)
        except ValueError:
            return None, True

        if not -(2**63) <= result < 2**63:
            return None, True

        return result, False

    # Original supported date formats
    date_formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    def parse_date(value):

        if missing(value):
            return None, False

        normalized = re.sub(
            r"\s+",
            " ",
            str(value).strip()
        )

        for fmt in date_formats:
            try:
                return datetime.strptime(
                    normalized,
                    fmt
                ), False
            except ValueError:
                continue

        return None, True

    result = {}
    flags = {}

    # Convert four integer fields
    integer_inputs = {
        "tra_code": tra_code,
        "tra_sqlidentity": tra_sqlidentity,
        "tra_req": tra_req,
        "tra_updatecount": tra_updatecount
    }

    integer_outputs = {
        "tra_code": (
            "transaction_code_int",
            "dq_transaction_code_invalid"
        ),
        "tra_sqlidentity": (
            "transaction_identity_int",
            "dq_transaction_identity_invalid"
        ),
        "tra_req": (
            "transaction_request_int",
            "dq_transaction_request_invalid"
        ),
        "tra_updatecount": (
            "transaction_updatecount_int",
            "dq_transaction_updatecount_invalid"
        )
    }

    for source_name, raw_value in integer_inputs.items():

        converted, invalid = parse_integer(raw_value)

        target, flag = integer_outputs[source_name]

        result[target] = converted
        flags[flag] = invalid

    # Convert three date fields
    date_inputs = {
        "tra_date": tra_date,
        "tra_created": tra_created,
        "tra_updated": tra_updated
    }

    date_outputs = {
        "tra_date": (
            "transaction_date_ts",
            "dq_transaction_date_invalid"
        ),
        "tra_created": (
            "transaction_created_ts",
            "dq_transaction_created_invalid"
        ),
        "tra_updated": (
            "transaction_updated_ts",
            "dq_transaction_updated_invalid"
        )
    }

    for source_name, raw_value in date_inputs.items():

        converted, invalid = parse_date(raw_value)

        target, flag = date_outputs[source_name]

        result[target] = converted
        flags[flag] = invalid

    # Required identifiers must be flagged when missing.
    # tra_req remains optional.
    for source_name in ["tra_code", "tra_sqlidentity"]:

        if missing(integer_inputs[source_name]):
            flag = integer_outputs[source_name][1]
            flags[flag] = True

    flags["dq_any_issue"] = any(flags.values())

    result.update(flags)

    # Return fields in the exact original order
    output_names = [
        "transaction_code_int",
        "transaction_identity_int",
        "transaction_request_int",
        "transaction_updatecount_int",
        "transaction_date_ts",
        "transaction_created_ts",
        "transaction_updated_ts",
        "dq_transaction_code_invalid",
        "dq_transaction_identity_invalid",
        "dq_transaction_request_invalid",
        "dq_transaction_updatecount_invalid",
        "dq_transaction_date_invalid",
        "dq_transaction_created_invalid",
        "dq_transaction_updated_invalid",
        "dq_any_issue"
    ]

    return tuple(result[name] for name in output_names)

# ------------------------------------------------------------
# 5. Rebuild Silver Transactions from Bronze
# ------------------------------------------------------------

silver_transactions_rebuilt = (

    bronze_tx

    .withColumn(

        "_silver_parsed",

        transform_transaction_record(
            F.col("tra_code"),
            F.col("tra_sqlidentity"),
            F.col("tra_req"),
            F.col("tra_updatecount"),
            F.col("tra_date"),
            F.col("tra_created"),
            F.col("tra_updated")
        )
    )

    .select(

        *[
            F.col(name)
            for name in bronze_tx.columns
        ],

        *[
            F.col(f"_silver_parsed.{field.name}")
            .alias(field.name)
            for field in ADDED_SCHEMA.fields
        ]
    )
)

assert (
    len(silver_transactions_rebuilt.columns)
    == EXPECTED_SILVER_COLUMNS
)

# ------------------------------------------------------------
# 6. Align column order and types with production
# ------------------------------------------------------------

assert (
    set(silver_transactions_rebuilt.columns)
    == set(production_tx.columns)
), "Rebuilt Silver column set differs from production."

silver_transactions_rebuilt = (
    silver_transactions_rebuilt.select(

        *[
            F.col(name)
            .cast(production_tx.schema[name].dataType)
            .alias(name)

            for name in production_tx.columns
        ]
    )
)

# ------------------------------------------------------------
# 7. Verify rebuilt Silver record count
# ------------------------------------------------------------

rebuilt_count = silver_transactions_rebuilt.count()

assert rebuilt_count == EXPECTED_ROWS

print("REBUILT SILVER ROW COUNT: PASS")

# ------------------------------------------------------------
# 8. Verify every original Bronze value is preserved
# ------------------------------------------------------------

original_fields = silver_transactions_rebuilt.select(
    *bronze_tx.columns
)

bronze_forward = (
    original_fields.exceptAll(bronze_tx).count()
)

bronze_reverse = (
    bronze_tx.exceptAll(original_fields).count()
)

assert bronze_forward == 0
assert bronze_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 9. Verify exact Silver column names and data types
# ------------------------------------------------------------

assert (
    silver_transactions_rebuilt.schema.names
    == production_tx.schema.names
)

assert (
    staging_tx.schema.names
    == production_tx.schema.names
)

for name in production_tx.columns:

    expected_type = production_tx.schema[name].dataType

    assert (
        silver_transactions_rebuilt.schema[name].dataType
        == expected_type
    ), f"Rebuilt type mismatch: {name}"

    assert (
        staging_tx.schema[name].dataType
        == expected_type
    ), f"Staging type mismatch: {name}"

print("SILVER SCHEMA VALIDATION: PASS")

# ------------------------------------------------------------
# 10. Compare rebuilt data with production Silver
# ------------------------------------------------------------

print("CHECKING REBUILT VS PRODUCTION...")

production_forward = (
    silver_transactions_rebuilt
    .exceptAll(production_tx)
    .count()
)

production_reverse = (
    production_tx
    .exceptAll(silver_transactions_rebuilt)
    .count()
)

print("Production forward mismatches:", production_forward)
print("Production reverse mismatches:", production_reverse)

assert production_forward == 0
assert production_reverse == 0

print("REBUILT VS PRODUCTION: PASS")

# ------------------------------------------------------------
# 11. Compare rebuilt data with staging Silver
# ------------------------------------------------------------

print("CHECKING REBUILT VS STAGING...")

staging_forward = (
    silver_transactions_rebuilt
    .exceptAll(staging_tx)
    .count()
)

staging_reverse = (
    staging_tx
    .exceptAll(silver_transactions_rebuilt)
    .count()
)

print("Staging forward mismatches:", staging_forward)
print("Staging reverse mismatches:", staging_reverse)

assert staging_forward == 0
assert staging_reverse == 0

print("REBUILT VS STAGING: PASS")

# ------------------------------------------------------------
# 12. Reconcile data-quality flag counts
# ------------------------------------------------------------

quality_summary = silver_transactions_rebuilt.agg(

    *[
        F.sum(
            F.when(F.col(name) == True, 1)
            .otherwise(0)
        ).alias(name)

        for name in QUALITY_FLAGS
    ]

)

print("\nTRANSACTION QUALITY FLAG SUMMARY")

display(quality_summary)

# ------------------------------------------------------------
# 13. Verify staging Delta format
# ------------------------------------------------------------

metadata = spark.sql(
    f"DESCRIBE DETAIL {STAGING_TABLE}"
).first()

assert metadata["format"].lower() == "delta"

print("STAGING DELTA FORMAT: PASS")

# ------------------------------------------------------------
# 14. Final independent validation report
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("STEP 9.12I — SILVER TRANSACTIONS VALIDATION")
print("=" * 70)

print("Bronze records:", bronze_tx.count())
print("Bronze columns:", len(bronze_tx.columns))
print("Rebuilt Silver records:", rebuilt_count)
print("Rebuilt Silver columns:", len(silver_transactions_rebuilt.columns))

print(
    "Bronze preservation mismatches:",
    bronze_forward + bronze_reverse
)

print(
    "Production comparison mismatches:",
    production_forward + production_reverse
)

print(
    "Staging comparison mismatches:",
    staging_forward + staging_reverse
)

assert (
    bronze_forward == 0
    and bronze_reverse == 0
    and production_forward == 0
    and production_reverse == 0
    and staging_forward == 0
    and staging_reverse == 0
)

print("\nSTEP 9.12I SILVER TRANSACTIONS: PASS")
print("PYTHON UDF SERIALIZATION: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")


# COMMAND ----------


# ============================================================
# STEP 9.12J — SILVER TRANSACTION DETAILS TRANSFORMATION
# AssetFusion360 | Databricks Free Edition
#
# Source: Production Bronze Transaction Details
# Reference: Production and Staging Silver
#
# READ-ONLY — NO DELTA TABLES MODIFIED
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

BRONZE_TABLE = (
    f"{PRODUCTION}.bronze_amps_transaction_details"
)
PRODUCTION_TABLE = (
    f"{PRODUCTION}.silver_amps_transaction_details"
)
STAGING_TABLE = (
    f"{STAGING}.silver_amps_transaction_details"
)

EXPECTED_ROWS = 2301927
EXPECTED_BRONZE_COLUMNS = 35
EXPECTED_SILVER_COLUMNS = 68

# ------------------------------------------------------------
# 1. VERIFY SOURCE AND REFERENCE TABLES
# ------------------------------------------------------------

for name in [BRONZE_TABLE, PRODUCTION_TABLE, STAGING_TABLE]:
    assert spark.catalog.tableExists(name), (
        f"Required table missing: {name}"
    )

bronze_td = spark.table(BRONZE_TABLE)
production_td = spark.table(PRODUCTION_TABLE)
staging_td = spark.table(STAGING_TABLE)

assert bronze_td.count() == EXPECTED_ROWS
assert production_td.count() == EXPECTED_ROWS
assert staging_td.count() == EXPECTED_ROWS

assert len(bronze_td.columns) == EXPECTED_BRONZE_COLUMNS
assert len(production_td.columns) == EXPECTED_SILVER_COLUMNS
assert len(staging_td.columns) == EXPECTED_SILVER_COLUMNS

print("SOURCE TABLE VALIDATION: PASS")

# ------------------------------------------------------------
# 2. ORIGINAL FIELD DEFINITIONS
# ------------------------------------------------------------

INTEGER_FIELDS = {
    "trl_trans": "detail_transaction_id_int",
    "trl_event": "detail_workorder_id_int",
    "trl_sqlidentity": "detail_identity_int",
    "par_sqlidentity": "part_identity_int",
    "par_updatecount": "part_updatecount_int"
}

DATE_FIELDS = {
    "trl_date": "detail_date_ts",
    "trl_created": "detail_created_ts",
    "trl_updated": "detail_updated_ts",
    "par_created": "part_created_ts",
    "par_updated": "part_updated_ts",
    "par_laststatusupdate": "part_laststatusupdate_ts"
}

DECIMAL_FIELDS = {
    "trl_qty": "detail_quantity_decimal",
    "trl_price": "detail_price_decimal",
    "trl_avgprice": "detail_avgprice_decimal"
}

INTEGER_FLAGS = {
    source: f"dq_{source}_invalid"
    for source in INTEGER_FIELDS
}

DATE_FLAGS = {
    source: f"dq_{source}_invalid"
    for source in DATE_FIELDS
}

DECIMAL_FLAGS = {
    source: f"dq_{source}_invalid"
    for source in DECIMAL_FIELDS
}

INFORMATION_FLAGS = [
    "dq_negative_quantity",
    "dq_negative_price",
    "dq_zero_quantity",
    "dq_zero_price"
]

QUALITY_FLAGS = (
    list(INTEGER_FLAGS.values())
    + list(DATE_FLAGS.values())
    + list(DECIMAL_FLAGS.values())
    + INFORMATION_FLAGS
    + ["dq_any_issue"]
)

REQUIRED_FIELDS = (
    list(INTEGER_FIELDS)
    + list(DATE_FIELDS)
    + list(DECIMAL_FIELDS)
)

for name in REQUIRED_FIELDS:
    assert name in bronze_td.columns, (
        f"Missing Bronze field: {name}"
    )

assert all(
    isinstance(field.dataType, T.StringType)
    for field in bronze_td.schema.fields
), "Original Bronze fields must be strings."

# ------------------------------------------------------------
# 3. DEFINE EXACT ADDITIONAL SILVER SCHEMA
# ------------------------------------------------------------

ADDED_SCHEMA = T.StructType(
    [
        T.StructField(name, T.LongType(), True)
        for name in INTEGER_FIELDS.values()
    ]
    + [
        T.StructField(name, T.TimestampType(), True)
        for name in DATE_FIELDS.values()
    ]
    + [
        T.StructField(name, T.DecimalType(20, 6), True)
        for name in DECIMAL_FIELDS.values()
    ]
    + [
        T.StructField(name, T.BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

assert len(INTEGER_FIELDS) == 5
assert len(DATE_FIELDS) == 6
assert len(DECIMAL_FIELDS) == 3
assert len(QUALITY_FLAGS) == 19
assert len(ADDED_SCHEMA.fields) == 33

for field in ADDED_SCHEMA.fields:
    assert field.name not in bronze_td.columns, (
        f"Silver field collision: {field.name}"
    )

print("BRONZE SCHEMA VALIDATION: PASS")

# ------------------------------------------------------------
# 4. SELF-CONTAINED PYTHON UDF
#
# Avoid cached helper functions and external references.
# This prevents the earlier Databricks serialization issue.
# ------------------------------------------------------------

@F.udf(returnType=ADDED_SCHEMA)
def transform_transaction_detail_record(*values):

    import re
    from datetime import datetime
    from decimal import Decimal, InvalidOperation

    def missing(value):
        return value is None or str(value).strip() == ""

    def parse_integer(value):
        if missing(value):
            return None, False

        text = str(value).strip()

        if not re.fullmatch(r"[+-]?\d+", text):
            return None, True

        try:
            number = int(text)
        except ValueError:
            return None, True

        if not -(2**63) <= number < 2**63:
            return None, True

        return number, False

    def parse_decimal(value):
        if missing(value):
            return None, False

        try:
            number = Decimal(str(value).strip())

            if not number.is_finite():
                return None, True

            number = number.quantize(
                Decimal("0.000001")
            )

            if abs(number) >= Decimal("100000000000000"):
                return None, True

            return number, False

        except (
            InvalidOperation,
            ValueError,
            OverflowError
        ):
            return None, True

    date_formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    def parse_date(value):
        if missing(value):
            return None, False

        normalized = re.sub(
            r"\s+",
            " ",
            str(value).strip()
        )

        for fmt in date_formats:
            try:
                return datetime.strptime(
                    normalized, fmt
                ), False
            except ValueError:
                continue

        return None, True

    # Source field order is defined explicitly.
    input_names = [
        "trl_trans",
        "trl_event",
        "trl_sqlidentity",
        "par_sqlidentity",
        "par_updatecount",
        "trl_date",
        "trl_created",
        "trl_updated",
        "par_created",
        "par_updated",
        "par_laststatusupdate",
        "trl_qty",
        "trl_price",
        "trl_avgprice"
    ]

    assert len(values) == len(input_names)

    source = dict(zip(input_names, values))
    result = {}
    flags = {}

    integer_map = {
        "trl_trans": "detail_transaction_id_int",
        "trl_event": "detail_workorder_id_int",
        "trl_sqlidentity": "detail_identity_int",
        "par_sqlidentity": "part_identity_int",
        "par_updatecount": "part_updatecount_int"
    }

    date_map = {
        "trl_date": "detail_date_ts",
        "trl_created": "detail_created_ts",
        "trl_updated": "detail_updated_ts",
        "par_created": "part_created_ts",
        "par_updated": "part_updated_ts",
        "par_laststatusupdate": "part_laststatusupdate_ts"
    }

    decimal_map = {
        "trl_qty": "detail_quantity_decimal",
        "trl_price": "detail_price_decimal",
        "trl_avgprice": "detail_avgprice_decimal"
    }

    # Parse five nullable integers
    for source_name, target_name in integer_map.items():
        converted, invalid = parse_integer(
            source[source_name]
        )

        result[target_name] = converted
        flags[f"dq_{source_name}_invalid"] = invalid

    # Parse six nullable timestamps
    for source_name, target_name in date_map.items():
        converted, invalid = parse_date(
            source[source_name]
        )

        result[target_name] = converted
        flags[f"dq_{source_name}_invalid"] = invalid

    # Parse three decimal(20,6) values
    for source_name, target_name in decimal_map.items():
        converted, invalid = parse_decimal(
            source[source_name]
        )

        result[target_name] = converted
        flags[f"dq_{source_name}_invalid"] = invalid

    # Required identifiers:
    # trl_event and part references remain optional.
    for source_name in ["trl_trans", "trl_sqlidentity"]:
        if missing(source[source_name]):
            flags[f"dq_{source_name}_invalid"] = True

    qty = result["detail_quantity_decimal"]
    price = result["detail_price_decimal"]

    # Informational flags preserve negative/zero values.
    flags["dq_negative_quantity"] = (
        qty is not None and qty < 0
    )
    flags["dq_negative_price"] = (
        price is not None and price < 0
    )
    flags["dq_zero_quantity"] = (
        qty is not None and qty == 0
    )
    flags["dq_zero_price"] = (
        price is not None and price == 0
    )

    # Only parsing/required-field flags contribute
    # to dq_any_issue.
    error_flag_names = (
        [f"dq_{name}_invalid" for name in integer_map]
        + [f"dq_{name}_invalid" for name in date_map]
        + [f"dq_{name}_invalid" for name in decimal_map]
    )

    flags["dq_any_issue"] = any(
        flags[name] for name in error_flag_names
    )

    result.update(flags)

    output_names = (
        list(integer_map.values())
        + list(date_map.values())
        + list(decimal_map.values())
        + error_flag_names
        + [
            "dq_negative_quantity",
            "dq_negative_price",
            "dq_zero_quantity",
            "dq_zero_price",
            "dq_any_issue"
        ]
    )

    return tuple(result[name] for name in output_names)

# ------------------------------------------------------------
# 5. BUILD SILVER TRANSACTION DETAILS
# ------------------------------------------------------------

silver_transaction_details_rebuilt = (
    bronze_td
    .withColumn(
        "_silver_parsed",
        transform_transaction_detail_record(
            *[F.col(name) for name in REQUIRED_FIELDS]
        )
    )
    .select(
        *[F.col(name) for name in bronze_td.columns],
        *[
            F.col(f"_silver_parsed.{field.name}")
                .alias(field.name)
            for field in ADDED_SCHEMA.fields
        ]
    )
)

assert (
    len(silver_transaction_details_rebuilt.columns)
    == EXPECTED_SILVER_COLUMNS
)

# ------------------------------------------------------------
# 6. ALIGN WITH PRODUCTION SILVER SCHEMA
# ------------------------------------------------------------

assert (
    set(silver_transaction_details_rebuilt.columns)
    == set(production_td.columns)
), "Rebuilt Silver columns differ from production."

silver_transaction_details_rebuilt = (
    silver_transaction_details_rebuilt.select(
        *[
            F.col(name)
                .cast(production_td.schema[name].dataType)
                .alias(name)
            for name in production_td.columns
        ]
    )
)

assert (
    silver_transaction_details_rebuilt.schema.names
    == production_td.schema.names
)

for name in production_td.columns:
    assert (
        silver_transaction_details_rebuilt.schema[name].dataType
        == production_td.schema[name].dataType
    ), f"Rebuilt schema mismatch: {name}"

    assert (
        staging_td.schema[name].dataType
        == production_td.schema[name].dataType
    ), f"Staging schema mismatch: {name}"

print("SILVER SCHEMA VALIDATION: PASS")

# ------------------------------------------------------------
# 7. VALIDATE ROW COUNTS
# ------------------------------------------------------------

rebuilt_rows = silver_transaction_details_rebuilt.count()

assert rebuilt_rows == EXPECTED_ROWS

print("REBUILT SILVER ROW COUNT: PASS")

# ------------------------------------------------------------
# 8. VERIFY COMPLETE BRONZE SOURCE PRESERVATION
# ------------------------------------------------------------

rebuilt_bronze = (
    silver_transaction_details_rebuilt
    .select(*bronze_td.columns)
)

bronze_forward = (
    rebuilt_bronze.exceptAll(bronze_td).count()
)

bronze_reverse = (
    bronze_td.exceptAll(rebuilt_bronze).count()
)

assert bronze_forward == 0
assert bronze_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 9. COMPARE REBUILT SILVER WITH PRODUCTION
# ------------------------------------------------------------

print("CHECKING REBUILT VS PRODUCTION...")

production_forward = (
    silver_transaction_details_rebuilt
    .exceptAll(production_td)
    .count()
)

production_reverse = (
    production_td
    .exceptAll(silver_transaction_details_rebuilt)
    .count()
)

print("Production forward mismatches:", production_forward)
print("Production reverse mismatches:", production_reverse)

assert production_forward == 0
assert production_reverse == 0

print("REBUILT VS PRODUCTION: PASS")

# ------------------------------------------------------------
# 10. COMPARE REBUILT SILVER WITH STAGING
# ------------------------------------------------------------

print("CHECKING REBUILT VS STAGING...")

staging_forward = (
    silver_transaction_details_rebuilt
    .exceptAll(staging_td)
    .count()
)

staging_reverse = (
    staging_td
    .exceptAll(silver_transaction_details_rebuilt)
    .count()
)

print("Staging forward mismatches:", staging_forward)
print("Staging reverse mismatches:", staging_reverse)

assert staging_forward == 0
assert staging_reverse == 0

print("REBUILT VS STAGING: PASS")

# ------------------------------------------------------------
# 11. VERIFY QUALITY FLAG RECONCILIATION
# ------------------------------------------------------------

quality_totals = (
    silver_transaction_details_rebuilt
    .agg(
        *[
            F.sum(
                F.when(F.col(name) == True, 1)
                .otherwise(0)
            ).alias(name)
            for name in QUALITY_FLAGS
        ]
    )
    .first()
)

print("\nTRANSACTION DETAILS QUALITY FLAGS")

for name in QUALITY_FLAGS:
    print(f"{name}: {quality_totals[name]}")

# Cross-check financial flags against original totals.
assert quality_totals["dq_negative_quantity"] == 44053
assert quality_totals["dq_negative_price"] == 5501
assert quality_totals["dq_zero_quantity"] == 164449
assert quality_totals["dq_zero_price"] == 61515

# ------------------------------------------------------------
# 12. VALIDATE STAGING DELTA FORMAT
# ------------------------------------------------------------

metadata = spark.sql(
    f"DESCRIBE DETAIL {STAGING_TABLE}"
).first()

assert metadata["format"].lower() == "delta"

print("\nSTAGING DELTA FORMAT: PASS")

# ------------------------------------------------------------
# 13. FINAL VALIDATION SUMMARY
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("STEP 9.12J — SILVER TRANSACTION DETAILS VALIDATION")
print("=" * 70)

print("Bronze records:", bronze_td.count())
print("Bronze columns:", len(bronze_td.columns))
print("Rebuilt Silver records:", rebuilt_rows)

print(
    "Rebuilt Silver columns:",
    len(silver_transaction_details_rebuilt.columns)
)

print(
    "Bronze preservation mismatches:",
    bronze_forward + bronze_reverse
)

print(
    "Production comparison mismatches:",
    production_forward + production_reverse
)

print(
    "Staging comparison mismatches:",
    staging_forward + staging_reverse
)

print("Negative quantity flags:",
      quality_totals["dq_negative_quantity"])
print("Negative price flags:",
      quality_totals["dq_negative_price"])
print("Zero quantity flags:",
      quality_totals["dq_zero_quantity"])
print("Zero price flags:",
      quality_totals["dq_zero_price"])

print("\nSTEP 9.12J SILVER TRANSACTION DETAILS: PASS")
print("NO DELTA TABLES MODIFIED")


# COMMAND ----------


# ============================================================
# STEP 9.12K — INDEPENDENT SILVER LABOR TRANSFORMATION
# AssetFusion360 | Databricks Free Edition
#
# Source: Production Bronze Labor
# References: Production Silver and Staging Silver
#
# READ-ONLY:
# - No Delta table creation
# - No overwrite, updates, or deletion
# - Preserve all Bronze rows and fields
# - Reproduce original parsing and DQ rules
# - Validate reconstructed data against both references
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ------------------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------------------

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

BRONZE_TABLE = f"{PRODUCTION}.bronze_amps_labor"
PRODUCTION_TABLE = f"{PRODUCTION}.silver_amps_labor"
STAGING_TABLE = f"{STAGING}.silver_amps_labor"

EXPECTED_ROWS = 3230423
EXPECTED_BRONZE_COLUMNS = 22
EXPECTED_SILVER_COLUMNS = 38

# ------------------------------------------------------------
# 2. VERIFY EXISTING TABLES
# ------------------------------------------------------------

for table_name in [
    BRONZE_TABLE,
    PRODUCTION_TABLE,
    STAGING_TABLE
]:
    assert spark.catalog.tableExists(table_name), (
        f"Required table missing: {table_name}"
    )

bronze_labor = spark.table(BRONZE_TABLE)
production_labor = spark.table(PRODUCTION_TABLE)
staging_labor = spark.table(STAGING_TABLE)

assert bronze_labor.count() == EXPECTED_ROWS
assert production_labor.count() == EXPECTED_ROWS
assert staging_labor.count() == EXPECTED_ROWS

assert len(bronze_labor.columns) == EXPECTED_BRONZE_COLUMNS
assert len(production_labor.columns) == EXPECTED_SILVER_COLUMNS
assert len(staging_labor.columns) == EXPECTED_SILVER_COLUMNS

print("SOURCE TABLE VALIDATION: PASS")

# ------------------------------------------------------------
# 3. VERIFY ORIGINAL BRONZE FIELDS
# ------------------------------------------------------------

REQUIRED_FIELDS = [
    "boo_sqlidentity",
    "boo_event",
    "boo_date",
    "boo_hours",
    "boo_rate",
    "boo_cost"
]

for column in REQUIRED_FIELDS:
    assert column in bronze_labor.columns, (
        f"Missing Bronze field: {column}"
    )

for field in bronze_labor.schema.fields:
    assert isinstance(
        field.dataType,
        T.StringType
    ), f"Unexpected Bronze field type: {field.name}"

# ------------------------------------------------------------
# 4. DEFINE ORIGINAL SILVER SCHEMA
# ------------------------------------------------------------

TYPED_SCHEMA = [
    T.StructField("labor_identity_int", T.LongType(), True),
    T.StructField("labor_workorder_id_int", T.LongType(), True),
    T.StructField("labor_date_ts", T.TimestampType(), True),
    T.StructField(
        "labor_hours_decimal",
        T.DecimalType(20, 6),
        True
    ),
    T.StructField(
        "labor_rate_decimal",
        T.DecimalType(20, 6),
        True
    ),
    T.StructField(
        "labor_cost_decimal",
        T.DecimalType(20, 6),
        True
    )
]

QUALITY_FLAGS = [
    "dq_labor_identity_invalid",
    "dq_workorder_reference_invalid",
    "dq_labor_date_invalid",
    "dq_labor_hours_invalid",
    "dq_labor_rate_invalid",
    "dq_labor_cost_invalid",
    "dq_negative_labor_hours",
    "dq_negative_labor_cost",
    "dq_zero_labor_rate",
    "dq_any_issue"
]

ADDED_SCHEMA = T.StructType(
    TYPED_SCHEMA
    + [
        T.StructField(name, T.BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

assert len(ADDED_SCHEMA.fields) == 16

for field in ADDED_SCHEMA.fields:
    assert field.name not in bronze_labor.columns, (
        f"Silver field collision: {field.name}"
    )

print("BRONZE SOURCE SCHEMA: PASS")

# ------------------------------------------------------------
# 5. SELF-CONTAINED LABOR TRANSFORMATION UDF
#
# Prevents the lru_cache serialization issue encountered
# during the Work Orders transformation.
# ------------------------------------------------------------

@F.udf(returnType=ADDED_SCHEMA)
def transform_labor_record(
    boo_sqlidentity,
    boo_event,
    boo_date,
    boo_hours,
    boo_rate,
    boo_cost
):

    import re

    from datetime import datetime

    from decimal import Decimal, InvalidOperation

    # --------------------------------------------------------
    # 5A. Missing-value detection
    # --------------------------------------------------------

    def missing(value):
        return (
            value is None
            or str(value).strip() == ""
        )

    # --------------------------------------------------------
    # 5B. Strict integer parsing
    # --------------------------------------------------------

    def parse_integer(value):

        if missing(value):
            return None, False

        text = str(value).strip()

        if not re.fullmatch(r"[+-]?\d+", text):
            return None, True

        try:
            number = int(text)
        except ValueError:
            return None, True

        if not -(2**63) <= number < 2**63:
            return None, True

        return number, False

    # --------------------------------------------------------
    # 5C. Decimal(20,6) parsing
    # Preserve signed hours, rates and costs.
    # --------------------------------------------------------

    def parse_decimal(value):

        if missing(value):
            return None, False

        try:
            number = Decimal(
                str(value).strip()
            )

            if not number.is_finite():
                return None, True

            number = number.quantize(
                Decimal("0.000001")
            )

            if abs(number) >= Decimal(
                "100000000000000"
            ):
                return None, True

            return number, False

        except (
            InvalidOperation,
            ValueError,
            OverflowError
        ):
            return None, True

    # --------------------------------------------------------
    # 5D. Date parsing
    # Exact formats from original Silver transformation.
    # --------------------------------------------------------

    date_formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    def parse_date(value):

        if missing(value):
            return None, False

        normalized = re.sub(
            r"\s+",
            " ",
            str(value).strip()
        )

        for fmt in date_formats:

            try:
                return datetime.strptime(
                    normalized,
                    fmt
                ), False

            except ValueError:
                continue

        return None, True

    # --------------------------------------------------------
    # 5E. Parse source values
    # --------------------------------------------------------

    identity, identity_invalid = parse_integer(
        boo_sqlidentity
    )

    event, event_invalid = parse_integer(
        boo_event
    )

    labor_date, date_invalid = parse_date(
        boo_date
    )

    hours, hours_invalid = parse_decimal(
        boo_hours
    )

    rate, rate_invalid = parse_decimal(
        boo_rate
    )

    cost, cost_invalid = parse_decimal(
        boo_cost
    )

    # --------------------------------------------------------
    # 5F. Original data-quality flags
    # --------------------------------------------------------

    flags = {
        "dq_labor_identity_invalid": (
            identity_invalid or missing(boo_sqlidentity)
        ),

        "dq_workorder_reference_invalid": (
            event_invalid or missing(boo_event)
        ),

        "dq_labor_date_invalid": date_invalid,

        "dq_labor_hours_invalid": hours_invalid,

        "dq_labor_rate_invalid": rate_invalid,

        "dq_labor_cost_invalid": cost_invalid,

        "dq_negative_labor_hours": (
            hours is not None and hours < 0
        ),

        "dq_negative_labor_cost": (
            cost is not None and cost < 0
        ),

        "dq_zero_labor_rate": (
            rate is not None and rate == 0
        )
    }

    # Informational financial flags do not contribute
    # to the overall parsing/required-field issue flag.

    error_flags = [
        "dq_labor_identity_invalid",
        "dq_workorder_reference_invalid",
        "dq_labor_date_invalid",
        "dq_labor_hours_invalid",
        "dq_labor_rate_invalid",
        "dq_labor_cost_invalid"
    ]

    flags["dq_any_issue"] = any(
        flags[name] for name in error_flags
    )

    return (
        identity,
        event,
        labor_date,
        hours,
        rate,
        cost,
        *[flags[name] for name in [
            "dq_labor_identity_invalid",
            "dq_workorder_reference_invalid",
            "dq_labor_date_invalid",
            "dq_labor_hours_invalid",
            "dq_labor_rate_invalid",
            "dq_labor_cost_invalid",
            "dq_negative_labor_hours",
            "dq_negative_labor_cost",
            "dq_zero_labor_rate",
            "dq_any_issue"
        ]]
    )

# ------------------------------------------------------------
# 6. REBUILD SILVER FROM BRONZE
# ------------------------------------------------------------

silver_labor_rebuilt = (
    bronze_labor
    .withColumn(
        "_silver_parsed",
        transform_labor_record(
            F.col("boo_sqlidentity"),
            F.col("boo_event"),
            F.col("boo_date"),
            F.col("boo_hours"),
            F.col("boo_rate"),
            F.col("boo_cost")
        )
    )
    .select(
        *[
            F.col(name)
            for name in bronze_labor.columns
        ],
        *[
            F.col(f"_silver_parsed.{field.name}")
            .alias(field.name)
            for field in ADDED_SCHEMA.fields
        ]
    )
)

assert len(silver_labor_rebuilt.columns) == 38

# ------------------------------------------------------------
# 7. ALIGN WITH EXACT PRODUCTION SILVER SCHEMA
# ------------------------------------------------------------

assert (
    set(silver_labor_rebuilt.columns)
    == set(production_labor.columns)
), "Rebuilt Silver column set differs from production."

silver_labor_rebuilt = (
    silver_labor_rebuilt.select(
        *[
            F.col(name)
            .cast(production_labor.schema[name].dataType)
            .alias(name)
            for name in production_labor.columns
        ]
    )
)

assert silver_labor_rebuilt.schema.names == (
    production_labor.schema.names
)

for name in production_labor.columns:

    expected_type = (
        production_labor.schema[name].dataType
    )

    assert (
        silver_labor_rebuilt.schema[name].dataType
        == expected_type
    ), f"Rebuilt schema mismatch: {name}"

    assert (
        staging_labor.schema[name].dataType
        == expected_type
    ), f"Staging schema mismatch: {name}"

print("SILVER SCHEMA VALIDATION: PASS")

# ------------------------------------------------------------
# 8. VALIDATE REBUILT ROW COUNT
# ------------------------------------------------------------

rebuilt_count = silver_labor_rebuilt.count()

assert rebuilt_count == EXPECTED_ROWS

print("REBUILT SILVER ROW COUNT: PASS")

# ------------------------------------------------------------
# 9. VERIFY ORIGINAL BRONZE SOURCE PRESERVATION
# ------------------------------------------------------------

original_fields = (
    silver_labor_rebuilt.select(
        *bronze_labor.columns
    )
)

bronze_forward = (
    original_fields
    .exceptAll(bronze_labor)
    .count()
)

bronze_reverse = (
    bronze_labor
    .exceptAll(original_fields)
    .count()
)

assert bronze_forward == 0
assert bronze_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 10. COMPARE REBUILT SILVER WITH PRODUCTION
# ------------------------------------------------------------

print("CHECKING REBUILT VS PRODUCTION...")

production_forward = (
    silver_labor_rebuilt
    .exceptAll(production_labor)
    .count()
)

production_reverse = (
    production_labor
    .exceptAll(silver_labor_rebuilt)
    .count()
)

print("Production forward mismatches:", production_forward)
print("Production reverse mismatches:", production_reverse)

assert production_forward == 0
assert production_reverse == 0

print("REBUILT VS PRODUCTION: PASS")

# ------------------------------------------------------------
# 11. COMPARE REBUILT SILVER WITH STAGING
# ------------------------------------------------------------

print("CHECKING REBUILT VS STAGING...")

staging_forward = (
    silver_labor_rebuilt
    .exceptAll(staging_labor)
    .count()
)

staging_reverse = (
    staging_labor
    .exceptAll(silver_labor_rebuilt)
    .count()
)

print("Staging forward mismatches:", staging_forward)
print("Staging reverse mismatches:", staging_reverse)

assert staging_forward == 0
assert staging_reverse == 0

print("REBUILT VS STAGING: PASS")

# ------------------------------------------------------------
# 12. VERIFY ORIGINAL LABOR QUALITY METRICS
# ------------------------------------------------------------

quality_totals = (
    silver_labor_rebuilt
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(name) == True, 1
                ).otherwise(0)
            ).alias(name)
            for name in QUALITY_FLAGS
        ]
    )
    .first()
)

assert quality_totals["dq_negative_labor_hours"] == 74609
assert quality_totals["dq_negative_labor_cost"] == 71374
assert quality_totals["dq_zero_labor_rate"] == 188165

print("LABOR QUALITY RECONCILIATION: PASS")

# ------------------------------------------------------------
# 13. VERIFY STAGING DELTA FORMAT
# ------------------------------------------------------------

metadata = spark.sql(
    f"DESCRIBE DETAIL {STAGING_TABLE}"
).first()

assert metadata["format"].lower() == "delta"

print("STAGING DELTA FORMAT: PASS")

# ------------------------------------------------------------
# 14. FINAL VALIDATION REPORT
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("STEP 9.12K — SILVER LABOR VALIDATION")
print("=" * 70)

print("Bronze records:", bronze_labor.count())
print("Bronze columns:", len(bronze_labor.columns))

print("Rebuilt Silver records:", rebuilt_count)
print("Rebuilt Silver columns:", len(silver_labor_rebuilt.columns))

print(
    "Bronze preservation mismatches:",
    bronze_forward + bronze_reverse
)

print(
    "Production comparison mismatches:",
    production_forward + production_reverse
)

print(
    "Staging comparison mismatches:",
    staging_forward + staging_reverse
)

print(
    "Negative labor hours:",
    quality_totals["dq_negative_labor_hours"]
)

print(
    "Negative labor costs:",
    quality_totals["dq_negative_labor_cost"]
)

print(
    "Zero labor rates:",
    quality_totals["dq_zero_labor_rate"]
)

print("\nSTEP 9.12K SILVER LABOR: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")


# COMMAND ----------


# ============================================================
# STEP 9.12L — CONSOLIDATED SILVER VALIDATION
# AssetFusion360 | Databricks Free Edition
#
# Validates:
# 1. Five production Silver tables
# 2. Five staging Silver tables
# 3. Expected row counts
# 4. Exact column names and data types
# 5. Full row-level equality including duplicates
# 6. Delta table format
#
# SAFETY: READ-ONLY
# NO CREATE / INSERT / UPDATE / DELETE / OVERWRITE
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

SILVER_TABLES = {
    "silver_amps_assets": (148522, 43),
    "silver_amps_workorders": (2231535, 82),
    "silver_amps_transactions": (755122, 33),
    "silver_amps_transaction_details": (2301927, 68),
    "silver_amps_labor": (3230423, 38)
}

validation_results = []

print("=" * 70)
print("STEP 9.12L — CONSOLIDATED SILVER VALIDATION")
print("=" * 70)

# ------------------------------------------------------------
# 1. Validate each Silver table
# ------------------------------------------------------------

for table_name, expected in SILVER_TABLES.items():

    expected_rows, expected_columns = expected

    production_table = f"{PRODUCTION}.{table_name}"
    staging_table = f"{STAGING}.{table_name}"

    print(f"\nValidating: {table_name}")

    result = {
        "table_name": table_name,
        "expected_rows": expected_rows,
        "production_rows": None,
        "staging_rows": None,
        "columns": expected_columns,
        "forward_mismatches": None,
        "reverse_mismatches": None,
        "status": "FAIL",
        "error": None
    }

    try:

        # ----------------------------------------------------
        # 2. Check table existence
        # ----------------------------------------------------

        assert spark.catalog.tableExists(production_table), (
            f"Production table missing: {production_table}"
        )

        assert spark.catalog.tableExists(staging_table), (
            f"Staging table missing: {staging_table}"
        )

        prod_df = spark.table(production_table)
        stage_df = spark.table(staging_table)

        # ----------------------------------------------------
        # 3. Validate row counts
        # ----------------------------------------------------

        prod_rows = prod_df.count()
        stage_rows = stage_df.count()

        result["production_rows"] = prod_rows
        result["staging_rows"] = stage_rows

        assert prod_rows == expected_rows, (
            f"Production row mismatch: {prod_rows}"
        )

        assert stage_rows == expected_rows, (
            f"Staging row mismatch: {stage_rows}"
        )

        # ----------------------------------------------------
        # 4. Validate schemas
        # ----------------------------------------------------

        assert len(prod_df.columns) == expected_columns
        assert len(stage_df.columns) == expected_columns

        assert (
            prod_df.schema.names == stage_df.schema.names
        ), "Column order or names differ."

        for column in prod_df.columns:

            assert (
                prod_df.schema[column].dataType
                == stage_df.schema[column].dataType
            ), f"Data type mismatch: {column}"

        # ----------------------------------------------------
        # 5. Check Delta formats
        # ----------------------------------------------------

        prod_metadata = spark.sql(
            f"DESCRIBE DETAIL {production_table}"
        ).first()

        stage_metadata = spark.sql(
            f"DESCRIBE DETAIL {staging_table}"
        ).first()

        assert prod_metadata["format"].lower() == "delta"
        assert stage_metadata["format"].lower() == "delta"

        # ----------------------------------------------------
        # 6. Full bidirectional equality check
        #
        # exceptAll preserves duplicate-row multiplicity.
        # This verifies actual data, not just counts.
        # ----------------------------------------------------

        forward = prod_df.exceptAll(stage_df).count()
        reverse = stage_df.exceptAll(prod_df).count()

        result["forward_mismatches"] = forward
        result["reverse_mismatches"] = reverse

        assert forward == 0, (
            f"Production-to-staging differences: {forward}"
        )

        assert reverse == 0, (
            f"Staging-to-production differences: {reverse}"
        )

        result["status"] = "PASS"

        print("  Row counts: PASS")
        print("  Schema: PASS")
        print("  Delta format: PASS")
        print("  Full data comparison: PASS")

    except Exception as error:

        result["error"] = str(error)

        print("  VALIDATION FAILED:", str(error))

    validation_results.append(result)

# ------------------------------------------------------------
# 7. Display the consolidated results
# ------------------------------------------------------------

results_df = spark.createDataFrame(
    validation_results,
    schema=T.StructType([
        T.StructField("table_name", T.StringType()),
        T.StructField("expected_rows", T.LongType()),
        T.StructField("production_rows", T.LongType()),
        T.StructField("staging_rows", T.LongType()),
        T.StructField("columns", T.IntegerType()),
        T.StructField("forward_mismatches", T.LongType()),
        T.StructField("reverse_mismatches", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("error", T.StringType())
    ])
)

print("\nCONSOLIDATED SILVER VALIDATION RESULTS")

display(results_df)

# ------------------------------------------------------------
# 8. Final validation summary
# ------------------------------------------------------------

passed = sum(
    1 for result in validation_results
    if result["status"] == "PASS"
)

failed = len(validation_results) - passed

expected_total = sum(
    item[0] for item in SILVER_TABLES.values()
)

actual_production_total = sum(
    result["production_rows"] or 0
    for result in validation_results
)

actual_staging_total = sum(
    result["staging_rows"] or 0
    for result in validation_results
)

print("\n" + "=" * 70)
print("FINAL SILVER CONSOLIDATION SUMMARY")
print("=" * 70)

print("Tables checked:", len(validation_results))
print("Tables passed:", passed)
print("Tables failed:", failed)

print("Expected total records:", expected_total)
print("Production total records:", actual_production_total)
print("Staging total records:", actual_staging_total)

print("\nProduction tables modified: NO")
print("Staging tables modified: NO")

assert failed == 0, (
    f"{failed} Silver tables failed validation. "
    "Review the results table before proceeding."
)

assert actual_production_total == expected_total
assert actual_staging_total == expected_total

print("\nSTEP 9.12L: PASS")
print("ALL FIVE SILVER TABLE PAIRS VALIDATED")
print("EXISTING PRODUCTION AND STAGING DATA PRESERVED")


# COMMAND ----------


# ============================================================
# STEP 9.12M — INDEPENDENT ASSETS TASK COMPLETION GUARD
#
# Run after copied Steps 9.12F and 9.12G.
# This cell performs no writes.
# ============================================================

from pyspark.sql import functions as F

EXPECTED_ROWS = 148522

required_variables = [
    "silver_assets_rebuilt",
    "bronze_forward",
    "bronze_reverse",
    "production_forward",
    "production_reverse",
    "staging_forward",
    "staging_reverse"
]

missing_variables = [
    name for name in required_variables
    if name not in globals()
]

assert not missing_variables, (
    "Transformation or validation did not complete: "
    + ", ".join(missing_variables)
)

assert (
    silver_assets_rebuilt.count()
    == EXPECTED_ROWS
), "Silver Assets row count failed."

assert all(
    globals()[name] == 0
    for name in required_variables[1:]
), "One or more Silver Assets comparisons failed."

print("=" * 60)
print("INDEPENDENT SILVER ASSETS WORKFLOW TASK")
print("=" * 60)

print("Dataset: silver_amps_assets")
print("Expected records:", EXPECTED_ROWS)
print("Bronze preservation: PASS")
print("Production comparison: PASS")
print("Staging comparison: PASS")
print("Delta tables modified: NO")

print("\nWORKFLOW TASK STATUS: SUCCESS")


# COMMAND ----------


# ============================================================
# STEP 9.12N — CORRECTED WORK ORDERS COMPLETION GUARD
#
# Avoids shared variables such as rebuilt_count.
# Explicitly validates the Work Orders DataFrame.
#
# READ-ONLY — NO EXISTING DELTA TABLES MODIFIED
# ============================================================

from pyspark.sql import functions as F

EXPECTED_ROWS = 2231535
EXPECTED_COLUMNS = 82

PRODUCTION_TABLE = (
    "workspace.assetfusion360.silver_amps_workorders"
)

STAGING_TABLE = (
    "workspace.assetfusion360_staging.silver_amps_workorders"
)

BRONZE_TABLE = (
    "workspace.assetfusion360.bronze_amps_workorders"
)

# ------------------------------------------------------------
# 1. Ensure the Work Orders transformation has run
# ------------------------------------------------------------

assert "silver_workorders_rebuilt" in globals(), (
    "silver_workorders_rebuilt is missing. "
    "Run the corrected Step 9.12H first."
)

rebuilt_wo = silver_workorders_rebuilt

# ------------------------------------------------------------
# 2. Read Work Orders source/reference tables
# ------------------------------------------------------------

bronze_wo_check = spark.table(BRONZE_TABLE)
production_wo_check = spark.table(PRODUCTION_TABLE)
staging_wo_check = spark.table(STAGING_TABLE)

# ------------------------------------------------------------
# 3. Validate records and columns
# ------------------------------------------------------------

actual_rows = rebuilt_wo.count()
actual_columns = len(rebuilt_wo.columns)

assert actual_rows == EXPECTED_ROWS, (
    f"Work Orders row mismatch: {actual_rows}"
)

assert actual_columns == EXPECTED_COLUMNS, (
    f"Work Orders column mismatch: {actual_columns}"
)

assert bronze_wo_check.count() == EXPECTED_ROWS
assert production_wo_check.count() == EXPECTED_ROWS
assert staging_wo_check.count() == EXPECTED_ROWS

print("WORK ORDERS ROW COUNTS: PASS")

# ------------------------------------------------------------
# 4. Validate schema
# ------------------------------------------------------------

assert (
    rebuilt_wo.schema.names
    == production_wo_check.schema.names
)

assert (
    staging_wo_check.schema.names
    == production_wo_check.schema.names
)

for column in production_wo_check.columns:

    expected_type = (
        production_wo_check.schema[column].dataType
    )

    assert (
        rebuilt_wo.schema[column].dataType
        == expected_type
    )

    assert (
        staging_wo_check.schema[column].dataType
        == expected_type
    )

print("WORK ORDERS SCHEMA: PASS")

# ------------------------------------------------------------
# 5. Validate original Bronze preservation
# ------------------------------------------------------------

original_wo = rebuilt_wo.select(
    *bronze_wo_check.columns
)

bronze_forward_wo = (
    original_wo.exceptAll(bronze_wo_check).count()
)

bronze_reverse_wo = (
    bronze_wo_check.exceptAll(original_wo).count()
)

assert bronze_forward_wo == 0
assert bronze_reverse_wo == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 6. Validate rebuilt versus production
# ------------------------------------------------------------

production_forward_wo = (
    rebuilt_wo.exceptAll(production_wo_check).count()
)

production_reverse_wo = (
    production_wo_check.exceptAll(rebuilt_wo).count()
)

assert production_forward_wo == 0
assert production_reverse_wo == 0

print("PRODUCTION COMPARISON: PASS")

# ------------------------------------------------------------
# 7. Validate rebuilt versus staging
# ------------------------------------------------------------

staging_forward_wo = (
    rebuilt_wo.exceptAll(staging_wo_check).count()
)

staging_reverse_wo = (
    staging_wo_check.exceptAll(rebuilt_wo).count()
)

assert staging_forward_wo == 0
assert staging_reverse_wo == 0

print("STAGING COMPARISON: PASS")

# ------------------------------------------------------------
# 8. Final independent task summary
# ------------------------------------------------------------

print("\n" + "=" * 65)
print("INDEPENDENT SILVER WORK ORDERS WORKFLOW TASK")
print("=" * 65)

print("Dataset: silver_amps_workorders")
print("Expected records:", EXPECTED_ROWS)
print("Actual records:", actual_rows)
print("Columns:", actual_columns)

print(
    "Bronze preservation mismatches:",
    bronze_forward_wo + bronze_reverse_wo
)

print(
    "Production mismatches:",
    production_forward_wo + production_reverse_wo
)

print(
    "Staging mismatches:",
    staging_forward_wo + staging_reverse_wo
)

print("\nWORKFLOW TASK STATUS: SUCCESS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")
