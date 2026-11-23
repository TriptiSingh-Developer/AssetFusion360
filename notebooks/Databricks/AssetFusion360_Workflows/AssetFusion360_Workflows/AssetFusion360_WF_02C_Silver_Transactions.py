# Databricks notebook source

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
# STEP 9.12O — INDEPENDENT SILVER TRANSACTIONS TASK GUARD
# AssetFusion360 | Databricks Free Edition
#
# Validates the Step 9.12I transformation independently.
#
# READ-ONLY — NO EXISTING DELTA TABLES MODIFIED
# ============================================================

EXPECTED_ROWS_TX = 755122
EXPECTED_COLUMNS_TX = 33

BRONZE_TX_TABLE = (
    "workspace.assetfusion360.bronze_amps_transactions"
)

PRODUCTION_TX_TABLE = (
    "workspace.assetfusion360.silver_amps_transactions"
)

STAGING_TX_TABLE = (
    "workspace.assetfusion360_staging.silver_amps_transactions"
)

# ------------------------------------------------------------
# 1. Verify the transaction transformation is available
# ------------------------------------------------------------

assert "silver_transactions_rebuilt" in globals(), (
    "Silver Transactions transformation was not executed. "
    "Run Step 9.12I first."
)

rebuilt_tx_check = silver_transactions_rebuilt

# ------------------------------------------------------------
# 2. Read source and reference tables
# ------------------------------------------------------------

bronze_tx_check = spark.table(BRONZE_TX_TABLE)

production_tx_check = spark.table(
    PRODUCTION_TX_TABLE
)

staging_tx_check = spark.table(
    STAGING_TX_TABLE
)

# ------------------------------------------------------------
# 3. Validate record counts
# ------------------------------------------------------------

rebuilt_tx_rows = rebuilt_tx_check.count()

assert rebuilt_tx_rows == EXPECTED_ROWS_TX

assert bronze_tx_check.count() == EXPECTED_ROWS_TX

assert production_tx_check.count() == EXPECTED_ROWS_TX

assert staging_tx_check.count() == EXPECTED_ROWS_TX

assert (
    len(rebuilt_tx_check.columns)
    == EXPECTED_COLUMNS_TX
)

print("TRANSACTIONS ROW COUNTS: PASS")

# ------------------------------------------------------------
# 4. Validate schema
# ------------------------------------------------------------

assert (
    rebuilt_tx_check.schema.names
    == production_tx_check.schema.names
)

assert (
    staging_tx_check.schema.names
    == production_tx_check.schema.names
)

for name in production_tx_check.columns:

    expected_type = (
        production_tx_check.schema[name].dataType
    )

    assert (
        rebuilt_tx_check.schema[name].dataType
        == expected_type
    )

    assert (
        staging_tx_check.schema[name].dataType
        == expected_type
    )

print("TRANSACTIONS SCHEMA: PASS")

# ------------------------------------------------------------
# 5. Verify Bronze source preservation
# ------------------------------------------------------------

original_tx_fields = rebuilt_tx_check.select(
    *bronze_tx_check.columns
)

bronze_tx_forward = (
    original_tx_fields
    .exceptAll(bronze_tx_check)
    .count()
)

bronze_tx_reverse = (
    bronze_tx_check
    .exceptAll(original_tx_fields)
    .count()
)

assert bronze_tx_forward == 0
assert bronze_tx_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ------------------------------------------------------------
# 6. Verify rebuilt versus production
# ------------------------------------------------------------

production_tx_forward = (
    rebuilt_tx_check
    .exceptAll(production_tx_check)
    .count()
)

production_tx_reverse = (
    production_tx_check
    .exceptAll(rebuilt_tx_check)
    .count()
)

assert production_tx_forward == 0
assert production_tx_reverse == 0

print("PRODUCTION COMPARISON: PASS")

# ------------------------------------------------------------
# 7. Verify rebuilt versus staging
# ------------------------------------------------------------

staging_tx_forward = (
    rebuilt_tx_check
    .exceptAll(staging_tx_check)
    .count()
)

staging_tx_reverse = (
    staging_tx_check
    .exceptAll(rebuilt_tx_check)
    .count()
)

assert staging_tx_forward == 0
assert staging_tx_reverse == 0

print("STAGING COMPARISON: PASS")

# ------------------------------------------------------------
# 8. Check staging Delta format
# ------------------------------------------------------------

staging_tx_metadata = spark.sql(
    f"DESCRIBE DETAIL {STAGING_TX_TABLE}"
).first()

assert (
    staging_tx_metadata["format"].lower()
    == "delta"
)

print("STAGING DELTA FORMAT: PASS")

# ------------------------------------------------------------
# 9. Final completion report
# ------------------------------------------------------------

print("\n" + "=" * 65)
print("INDEPENDENT SILVER TRANSACTIONS WORKFLOW TASK")
print("=" * 65)

print("Dataset: silver_amps_transactions")
print("Expected records:", EXPECTED_ROWS_TX)
print("Actual records:", rebuilt_tx_rows)
print("Columns:", len(rebuilt_tx_check.columns))

print(
    "Bronze preservation mismatches:",
    bronze_tx_forward + bronze_tx_reverse
)

print(
    "Production mismatches:",
    production_tx_forward + production_tx_reverse
)

print(
    "Staging mismatches:",
    staging_tx_forward + staging_tx_reverse
)

print("\nWORKFLOW TASK STATUS: SUCCESS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")
