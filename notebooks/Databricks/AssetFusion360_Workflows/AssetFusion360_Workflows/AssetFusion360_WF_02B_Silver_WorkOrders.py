# Databricks notebook source
# DBTITLE 1,Step 9.12H — Silver Work Orders Transformation

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
