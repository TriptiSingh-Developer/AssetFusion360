# Databricks notebook source

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
# STEP 9.12P — TRANSACTION DETAILS WORKFLOW COMPLETION GUARD
# AssetFusion360 | Databricks Free Edition
#
# Independent validation of Step 9.12J.
#
# READ-ONLY:
# No CREATE / INSERT / UPDATE / DELETE / OVERWRITE
# ============================================================

EXPECTED_ROWS_TD = 2301927
EXPECTED_COLUMNS_TD = 68

BRONZE_TD_TABLE = (
    "workspace.assetfusion360.bronze_amps_transaction_details"
)

PRODUCTION_TD_TABLE = (
    "workspace.assetfusion360.silver_amps_transaction_details"
)

STAGING_TD_TABLE = (
    "workspace.assetfusion360_staging.silver_amps_transaction_details"
)

# ------------------------------------------------------------
# 1. Verify the Transaction Details transformation executed
# ------------------------------------------------------------

assert "silver_transaction_details_rebuilt" in globals(), (
    "Silver Transaction Details transformation missing. "
    "Run Step 9.12J first."
)

rebuilt_td_check = silver_transaction_details_rebuilt

# ------------------------------------------------------------
# 2. Read the existing reference tables
# ------------------------------------------------------------

bronze_td_check = spark.table(BRONZE_TD_TABLE)

production_td_check = spark.table(PRODUCTION_TD_TABLE)

staging_td_check = spark.table(STAGING_TD_TABLE)

# ------------------------------------------------------------
# 3. Verify record counts and columns
# ------------------------------------------------------------

actual_td_rows = rebuilt_td_check.count()
actual_td_columns = len(rebuilt_td_check.columns)

assert actual_td_rows == EXPECTED_ROWS_TD, (
    f"Transaction Details row mismatch: {actual_td_rows}"
)

assert actual_td_columns == EXPECTED_COLUMNS_TD, (
    f"Unexpected column count: {actual_td_columns}"
)

assert bronze_td_check.count() == EXPECTED_ROWS_TD
assert production_td_check.count() == EXPECTED_ROWS_TD
assert staging_td_check.count() == EXPECTED_ROWS_TD

print("TRANSACTION DETAILS ROW COUNTS: PASS")

# ------------------------------------------------------------
# 4. Verify complete production and staging schemas
# ------------------------------------------------------------

assert (
    rebuilt_td_check.schema.names
    == production_td_check.schema.names
), "Rebuilt and production column names differ."

assert (
    staging_td_check.schema.names
    == production_td_check.schema.names
), "Staging and production column names differ."

for column in production_td_check.columns:

    expected_type = production_td_check.schema[
        column
    ].dataType

    assert (
        rebuilt_td_check.schema[column].dataType
        == expected_type
    ), f"Rebuilt type mismatch: {column}"

    assert (
        staging_td_check.schema[column].dataType
        == expected_type
    ), f"Staging type mismatch: {column}"

print("TRANSACTION DETAILS SCHEMA: PASS")

# ------------------------------------------------------------
# 5. Verify original Bronze preservation
# ------------------------------------------------------------

original_td_fields = rebuilt_td_check.select(
    *bronze_td_check.columns
)

bronze_td_forward = (
    original_td_fields
    .exceptAll(bronze_td_check)
    .count()
)

bronze_td_reverse = (
    bronze_td_check
    .exceptAll(original_td_fields)
    .count()
)

assert bronze_td_forward == 0
assert bronze_td_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 6. Verify rebuilt Silver against production
# ------------------------------------------------------------

production_td_forward = (
    rebuilt_td_check
    .exceptAll(production_td_check)
    .count()
)

production_td_reverse = (
    production_td_check
    .exceptAll(rebuilt_td_check)
    .count()
)

assert production_td_forward == 0
assert production_td_reverse == 0

print("PRODUCTION COMPARISON: PASS")

# ------------------------------------------------------------
# 7. Verify rebuilt Silver against staging
# ------------------------------------------------------------

staging_td_forward = (
    rebuilt_td_check
    .exceptAll(staging_td_check)
    .count()
)

staging_td_reverse = (
    staging_td_check
    .exceptAll(rebuilt_td_check)
    .count()
)

assert staging_td_forward == 0
assert staging_td_reverse == 0

print("STAGING COMPARISON: PASS")

# ------------------------------------------------------------
# 8. Validate informational financial flags
# ------------------------------------------------------------

from pyspark.sql import functions as F

FINANCIAL_FLAGS = [
    "dq_negative_quantity",
    "dq_negative_price",
    "dq_zero_quantity",
    "dq_zero_price"
]

financial_metrics = (
    rebuilt_td_check
    .agg(
        *[
            F.sum(
                F.when(F.col(name) == True, 1)
                .otherwise(0)
            ).alias(name)
            for name in FINANCIAL_FLAGS
        ]
    )
    .first()
)

# Compare the rebuilt flags with actual production flags.
production_financial_metrics = (
    production_td_check
    .agg(
        *[
            F.sum(
                F.when(F.col(name) == True, 1)
                .otherwise(0)
            ).alias(name)
            for name in FINANCIAL_FLAGS
        ]
    )
    .first()
)

for name in FINANCIAL_FLAGS:

    assert (
        financial_metrics[name]
        == production_financial_metrics[name]
    ), f"Financial flag mismatch: {name}"

print("FINANCIAL QUALITY FLAGS: PASS")

# ------------------------------------------------------------
# 9. Verify Delta format
# ------------------------------------------------------------

for table in [
    PRODUCTION_TD_TABLE,
    STAGING_TD_TABLE
]:

    metadata = spark.sql(
        f"DESCRIBE DETAIL {table}"
    ).first()

    assert metadata["format"].lower() == "delta"

print("DELTA TABLE FORMAT: PASS")

# ------------------------------------------------------------
# 10. Final independent workflow report
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("INDEPENDENT SILVER TRANSACTION DETAILS WORKFLOW")
print("=" * 70)

print("Dataset: silver_amps_transaction_details")
print("Expected records:", EXPECTED_ROWS_TD)
print("Actual records:", actual_td_rows)
print("Silver columns:", actual_td_columns)

print(
    "Bronze preservation mismatches:",
    bronze_td_forward + bronze_td_reverse
)

print(
    "Production mismatches:",
    production_td_forward + production_td_reverse
)

print(
    "Staging mismatches:",
    staging_td_forward + staging_td_reverse
)

print("\nFinancial quality flags:")

for name in FINANCIAL_FLAGS:
    print(f"  {name}: {financial_metrics[name]}")

print("\nWORKFLOW TASK STATUS: SUCCESS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")
