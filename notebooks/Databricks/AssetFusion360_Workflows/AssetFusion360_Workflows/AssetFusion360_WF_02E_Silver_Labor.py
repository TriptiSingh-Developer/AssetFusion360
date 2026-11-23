# Databricks notebook source

# ============================================================
# ASSETFUSION360
# STEP 9.12Q — COMPLETE INDEPENDENT SILVER LABOR WORKFLOW
# Databricks Free Edition
#
# Notebook:
# AssetFusion360_WF_02E_Silver_Labor
#
# Source:
# workspace.assetfusion360.bronze_amps_labor
#
# References:
# workspace.assetfusion360.silver_amps_labor
# workspace.assetfusion360_staging.silver_amps_labor
#
# PURPOSE:
# 1. Independently rebuild Silver Labor from Bronze
# 2. Preserve all original records and columns
# 3. Apply original parsing and quality rules
# 4. Verify production and staging equality
# 5. Complete workflow validation
#
# SAFETY:
# READ-ONLY
# NO CREATE / INSERT / UPDATE / DELETE / OVERWRITE
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ============================================================
# 1. CONFIGURATION
# ============================================================

BRONZE_LABOR_TABLE = (
    "workspace.assetfusion360.bronze_amps_labor"
)

PRODUCTION_LABOR_TABLE = (
    "workspace.assetfusion360.silver_amps_labor"
)

STAGING_LABOR_TABLE = (
    "workspace.assetfusion360_staging.silver_amps_labor"
)

EXPECTED_ROWS_LABOR = 3230423
EXPECTED_BRONZE_COLUMNS_LABOR = 22
EXPECTED_SILVER_COLUMNS_LABOR = 38

print("=" * 70)
print("ASSETFUSION360 — INDEPENDENT SILVER LABOR WORKFLOW")
print("=" * 70)

# ============================================================
# 2. VERIFY SOURCE AND REFERENCE TABLES
# ============================================================

for table_name in [
    BRONZE_LABOR_TABLE,
    PRODUCTION_LABOR_TABLE,
    STAGING_LABOR_TABLE
]:

    assert spark.catalog.tableExists(table_name), (
        f"Required Delta table missing: {table_name}"
    )

bronze_labor = spark.table(
    BRONZE_LABOR_TABLE
)

production_labor = spark.table(
    PRODUCTION_LABOR_TABLE
)

staging_labor = spark.table(
    STAGING_LABOR_TABLE
)

bronze_count = bronze_labor.count()
production_count = production_labor.count()
staging_count = staging_labor.count()

assert bronze_count == EXPECTED_ROWS_LABOR, (
    f"Bronze expected {EXPECTED_ROWS_LABOR}, "
    f"found {bronze_count}"
)

assert production_count == EXPECTED_ROWS_LABOR, (
    f"Production expected {EXPECTED_ROWS_LABOR}, "
    f"found {production_count}"
)

assert staging_count == EXPECTED_ROWS_LABOR, (
    f"Staging expected {EXPECTED_ROWS_LABOR}, "
    f"found {staging_count}"
)

assert (
    len(bronze_labor.columns)
    == EXPECTED_BRONZE_COLUMNS_LABOR
)

assert (
    len(production_labor.columns)
    == EXPECTED_SILVER_COLUMNS_LABOR
)

assert (
    len(staging_labor.columns)
    == EXPECTED_SILVER_COLUMNS_LABOR
)

print("SOURCE TABLE VALIDATION: PASS")

# ============================================================
# 3. VERIFY ORIGINAL BRONZE FIELDS
# ============================================================

LABOR_SOURCE_FIELDS = [
    "boo_sqlidentity",
    "boo_event",
    "boo_date",
    "boo_hours",
    "boo_rate",
    "boo_cost"
]

for column_name in LABOR_SOURCE_FIELDS:

    assert column_name in bronze_labor.columns, (
        f"Missing Bronze field: {column_name}"
    )

for field in bronze_labor.schema.fields:

    assert isinstance(
        field.dataType,
        T.StringType
    ), (
        f"Unexpected Bronze column type: {field.name}"
    )

print("BRONZE SOURCE SCHEMA: PASS")

# ============================================================
# 4. DEFINE ORIGINAL SILVER OUTPUT SCHEMA
# ============================================================

LABOR_TYPED_FIELDS = [

    T.StructField(
        "labor_identity_int",
        T.LongType(),
        True
    ),

    T.StructField(
        "labor_workorder_id_int",
        T.LongType(),
        True
    ),

    T.StructField(
        "labor_date_ts",
        T.TimestampType(),
        True
    ),

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

LABOR_QUALITY_FLAGS = [
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

LABOR_ADDED_SCHEMA = T.StructType(

    LABOR_TYPED_FIELDS

    + [
        T.StructField(
            name,
            T.BooleanType(),
            True
        )
        for name in LABOR_QUALITY_FLAGS
    ]
)

assert len(LABOR_ADDED_SCHEMA.fields) == 16

for field in LABOR_ADDED_SCHEMA.fields:

    assert field.name not in bronze_labor.columns, (
        f"Unexpected Bronze/Silver column collision: "
        f"{field.name}"
    )

print("SILVER OUTPUT SCHEMA CONFIGURATION: PASS")

# ============================================================
# 5. SELF-CONTAINED TRANSFORMATION UDF
#
# All imports and parsing functions are inside this UDF.
# This avoids the earlier cached-helper serialization issue.
# ============================================================

@F.udf(returnType=LABOR_ADDED_SCHEMA)
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
    # A. Check missing values
    # --------------------------------------------------------

    def missing(value):
        return (
            value is None
            or str(value).strip() == ""
        )

    # --------------------------------------------------------
    # B. Parse signed 64-bit integers
    # --------------------------------------------------------

    def parse_integer(value):

        if missing(value):
            return None, False

        text = str(value).strip()

        if not re.fullmatch(
            r"[+-]?\d+",
            text
        ):
            return None, True

        try:
            number = int(text)

        except ValueError:
            return None, True

        if not -(2**63) <= number < 2**63:
            return None, True

        return number, False

    # --------------------------------------------------------
    # C. Parse decimal(20,6)
    #
    # Preserve negative hours, rates, and costs.
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
    # D. Parse original Labor date formats
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

                parsed = datetime.strptime(
                    normalized,
                    fmt
                )

                return parsed, False

            except ValueError:
                continue

        return None, True

    # --------------------------------------------------------
    # E. Parse original Labor source values
    # --------------------------------------------------------

    identity, identity_invalid = parse_integer(
        boo_sqlidentity
    )

    workorder, workorder_invalid = parse_integer(
        boo_event
    )

    labor_date, labor_date_invalid = parse_date(
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
    # F. Generate original quality flags
    # --------------------------------------------------------

    flags = {

        "dq_labor_identity_invalid": (
            identity_invalid
            or missing(boo_sqlidentity)
        ),

        "dq_workorder_reference_invalid": (
            workorder_invalid
            or missing(boo_event)
        ),

        "dq_labor_date_invalid": labor_date_invalid,

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

    # Financial reversals and zero rates are informational.
    # Only parsing and required-field errors count as issues.

    error_flags = [
        "dq_labor_identity_invalid",
        "dq_workorder_reference_invalid",
        "dq_labor_date_invalid",
        "dq_labor_hours_invalid",
        "dq_labor_rate_invalid",
        "dq_labor_cost_invalid"
    ]

    flags["dq_any_issue"] = any(
        flags[name]
        for name in error_flags
    )

    return (

        identity,
        workorder,
        labor_date,
        hours,
        rate,
        cost,

        *[
            flags[name]
            for name in [
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
        ]
    )

print("LABOR TRANSFORMATION UDF: DEFINED")

# ============================================================
# 6. REBUILD SILVER LABOR FROM BRONZE
# ============================================================

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
            F.col(
                f"_silver_parsed.{field.name}"
            ).alias(field.name)

            for field in LABOR_ADDED_SCHEMA.fields
        ]
    )
)

assert (
    len(silver_labor_rebuilt.columns)
    == EXPECTED_SILVER_COLUMNS_LABOR
)

print("SILVER LABOR TRANSFORMATION: DEFINED")

# ============================================================
# 7. ALIGN WITH PRODUCTION SILVER COLUMN ORDER
# ============================================================

assert (
    set(silver_labor_rebuilt.columns)
    == set(production_labor.columns)
), "Rebuilt Silver column set differs from production."

silver_labor_rebuilt = (

    silver_labor_rebuilt

    .select(

        *[
            F.col(name)
            .cast(
                production_labor.schema[name].dataType
            )
            .alias(name)

            for name in production_labor.columns
        ]
    )
)

assert (
    silver_labor_rebuilt.schema.names
    == production_labor.schema.names
)

for column_name in production_labor.columns:

    expected_type = (
        production_labor.schema[column_name].dataType
    )

    assert (
        silver_labor_rebuilt.schema[
            column_name
        ].dataType == expected_type
    ), f"Rebuilt type mismatch: {column_name}"

    assert (
        staging_labor.schema[
            column_name
        ].dataType == expected_type
    ), f"Staging type mismatch: {column_name}"

print("SILVER SCHEMA VALIDATION: PASS")

# ============================================================
# 8. VALIDATE REBUILT ROW COUNT
# ============================================================

rebuilt_labor_count = (
    silver_labor_rebuilt.count()
)

assert rebuilt_labor_count == EXPECTED_ROWS_LABOR, (
    f"Expected {EXPECTED_ROWS_LABOR} Labor records, "
    f"found {rebuilt_labor_count}"
)

print("REBUILT SILVER ROW COUNT: PASS")

# ============================================================
# 9. VERIFY COMPLETE BRONZE PRESERVATION
# ============================================================

original_labor_fields = (
    silver_labor_rebuilt.select(
        *bronze_labor.columns
    )
)

bronze_labor_forward = (
    original_labor_fields
    .exceptAll(bronze_labor)
    .count()
)

bronze_labor_reverse = (
    bronze_labor
    .exceptAll(original_labor_fields)
    .count()
)

assert bronze_labor_forward == 0
assert bronze_labor_reverse == 0

print("BRONZE SOURCE PRESERVATION: PASS")

# ============================================================
# 10. VALIDATE AGAINST PRODUCTION SILVER
# ============================================================

print("\nCHECKING REBUILT VS PRODUCTION...")

production_labor_forward = (

    silver_labor_rebuilt

    .exceptAll(production_labor)

    .count()
)

production_labor_reverse = (

    production_labor

    .exceptAll(silver_labor_rebuilt)

    .count()
)

print(
    "Production forward mismatches:",
    production_labor_forward
)

print(
    "Production reverse mismatches:",
    production_labor_reverse
)

assert production_labor_forward == 0
assert production_labor_reverse == 0

print("REBUILT VS PRODUCTION: PASS")

# ============================================================
# 11. VALIDATE AGAINST STAGING SILVER
# ============================================================

print("\nCHECKING REBUILT VS STAGING...")

staging_labor_forward = (

    silver_labor_rebuilt

    .exceptAll(staging_labor)

    .count()
)

staging_labor_reverse = (

    staging_labor

    .exceptAll(silver_labor_rebuilt)

    .count()
)

print(
    "Staging forward mismatches:",
    staging_labor_forward
)

print(
    "Staging reverse mismatches:",
    staging_labor_reverse
)

assert staging_labor_forward == 0
assert staging_labor_reverse == 0

print("REBUILT VS STAGING: PASS")

# ============================================================
# 12. VERIFY ORIGINAL LABOR QUALITY FLAGS
# ============================================================

LABOR_INFORMATION_FLAGS = [
    "dq_negative_labor_hours",
    "dq_negative_labor_cost",
    "dq_zero_labor_rate"
]

rebuilt_quality = (
    silver_labor_rebuilt
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(name) == True,
                    1
                ).otherwise(0)
            ).alias(name)
            for name in LABOR_INFORMATION_FLAGS
        ]
    )
    .first()
)

production_quality = (
    production_labor
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(name) == True,
                    1
                ).otherwise(0)
            ).alias(name)
            for name in LABOR_INFORMATION_FLAGS
        ]
    )
    .first()
)

for name in LABOR_INFORMATION_FLAGS:

    assert (
        rebuilt_quality[name]
        == production_quality[name]
    ), f"Labor quality flag mismatch: {name}"

print("LABOR QUALITY FLAG COMPARISON: PASS")

# ============================================================
# 13. VALIDATE DELTA TABLE FORMATS
# ============================================================

for table_name in [
    PRODUCTION_LABOR_TABLE,
    STAGING_LABOR_TABLE
]:

    metadata = spark.sql(
        f"DESCRIBE DETAIL {table_name}"
    ).first()

    assert (
        metadata["format"].lower()
        == "delta"
    ), f"Expected Delta format: {table_name}"

print("DELTA TABLE FORMAT: PASS")

# ============================================================
# 14. FINAL WORKFLOW COMPLETION GUARD
# ============================================================

assert (
    silver_labor_rebuilt.count()
    == EXPECTED_ROWS_LABOR
)

assert (
    len(silver_labor_rebuilt.columns)
    == EXPECTED_SILVER_COLUMNS_LABOR
)

assert all(
    value == 0
    for value in [
        bronze_labor_forward,
        bronze_labor_reverse,
        production_labor_forward,
        production_labor_reverse,
        staging_labor_forward,
        staging_labor_reverse
    ]
)

print("\n" + "=" * 70)
print("STEP 9.12Q — SILVER LABOR WORKFLOW VALIDATION")
print("=" * 70)

print("Dataset: silver_amps_labor")

print("Bronze records:", bronze_count)
print("Production Silver records:", production_count)
print("Staging Silver records:", staging_count)
print("Rebuilt Silver records:", rebuilt_labor_count)

print(
    "Original Bronze columns:",
    EXPECTED_BRONZE_COLUMNS_LABOR
)

print(
    "Final Silver columns:",
    EXPECTED_SILVER_COLUMNS_LABOR
)

print(
    "\nBronze preservation mismatches:",
    bronze_labor_forward + bronze_labor_reverse
)

print(
    "Production comparison mismatches:",
    production_labor_forward + production_labor_reverse
)

print(
    "Staging comparison mismatches:",
    staging_labor_forward + staging_labor_reverse
)

print("\nLabor informational flags:")

for name in LABOR_INFORMATION_FLAGS:
    print(
        f"  {name}: {rebuilt_quality[name]}"
    )

print("\nSTEP 9.12Q: PASS")
print("INDEPENDENT SILVER LABOR WORKFLOW: SUCCESS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
print("STAGING DELTA TABLES: UNCHANGED")
print("=" * 70)
