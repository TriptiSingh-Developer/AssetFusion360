# Databricks notebook source

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
