# Databricks notebook source

# Step 9.9C — Rebuild Assets Silver from Bronze Delta.
# Correct timezone-free timestamps and preserve all 148,522 source records.
# Validate complete equivalence without changing existing Delta tables.

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    TimestampNTZType,
    DecimalType,
    BooleanType
)

BASE = "workspace.assetfusion360"

BRONZE_TABLE = f"{BASE}.bronze_amps_assets"
SILVER_TABLE = f"{BASE}.silver_amps_assets"

bronze = spark.table(BRONZE_TABLE)
existing = spark.table(SILVER_TABLE)


# 1. Preserve the original Python parsing behavior

def missing(value):
    return value is None or str(value).strip() == ""


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


def parse_decimal(value):
    if missing(value):
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


def parse_timestamp(value):
    if missing(value):
        return None, False

    text = re.sub(
        r"\s+",
        " ",
        str(value).strip()
    )

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%b %d %Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt), False
        except ValueError:
            continue

    return None, True


# 2. Define the correct Silver schema

added_schema = StructType([
    StructField(
        "asset_identity_int",
        LongType(),
        True
    ),
    StructField(
        "asset_commissioned_ts",
        TimestampNTZType(),
        True
    ),
    StructField(
        "asset_created_ts",
        TimestampNTZType(),
        True
    ),
    StructField(
        "asset_value_decimal",
        DecimalType(20, 6),
        True
    ),
    StructField(
        "dq_asset_code_missing",
        BooleanType(),
        True
    ),
    StructField(
        "dq_asset_code_placeholder",
        BooleanType(),
        True
    ),
    StructField(
        "dq_asset_identity_invalid",
        BooleanType(),
        True
    ),
    StructField(
        "dq_commission_date_invalid",
        BooleanType(),
        True
    ),
    StructField(
        "dq_created_date_invalid",
        BooleanType(),
        True
    ),
    StructField(
        "dq_asset_value_invalid",
        BooleanType(),
        True
    ),
    StructField(
        "dq_any_issue",
        BooleanType(),
        True
    )
])


# 3. Reproduce the original Assets transformation rules

@F.udf(returnType=added_schema)
def transform_asset(
    asset_code,
    identity_raw,
    commissioned_raw,
    created_raw,
    value_raw
):

    identity, identity_invalid = parse_integer(
        identity_raw
    )

    commissioned, commissioned_invalid = parse_timestamp(
        commissioned_raw
    )

    created, created_invalid = parse_timestamp(
        created_raw
    )

    value, value_invalid = parse_decimal(
        value_raw
    )

    code_missing = missing(asset_code)

    code_placeholder = (
        not code_missing
        and str(asset_code).strip().upper()
        in {"*", "N/A", "NA", "NULL", "UNKNOWN"}
    )

    identity_issue = (
        identity_invalid or missing(identity_raw)
    )

    any_issue = any([
        code_missing,
        code_placeholder,
        identity_issue,
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
        identity_issue,
        commissioned_invalid,
        created_invalid,
        value_invalid,
        any_issue
    )


# 4. Validate Bronze input and perform transformation

required_columns = [
    "OBJ_CODE",
    "OBJ_SQLIDENTITY",
    "OBJ_COMMISS",
    "OBJ_CREATED",
    "OBJ_VALUE"
]

assert len(bronze.columns) == 32

for name in required_columns:
    assert name in bronze.columns, (
        f"Missing Bronze column: {name}"
    )

parsed = bronze.withColumn(
    "_silver_parsed",
    transform_asset(
        *[F.col(name) for name in required_columns]
    )
)

bronze_names = set(bronze.columns)

silver_transformed = parsed.select(
    *[
        (
            F.col(name)
            if name in bronze_names
            else F.col(f"_silver_parsed.{name}")
        ).alias(name)
        for name in existing.columns
    ]
)


# 5. Compare actual transformation schema with Silver Delta

expected_schema = existing.schema
actual_schema = silver_transformed.schema

assert len(actual_schema.fields) == 43
assert actual_schema.names == expected_schema.names

type_mismatches = [
    (
        expected.name,
        str(expected.dataType),
        str(actual.dataType)
    )
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if expected.dataType != actual.dataType
]

metadata_differences = [
    expected.name
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if (
        expected.nullable != actual.nullable
        or expected.metadata != actual.metadata
    )
]

print("BRONZE SOURCE VALIDATION: PASS")

print("\nSCHEMA VALIDATION")
print("-" * 55)
print("Expected columns:", len(expected_schema.fields))
print("Actual columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)

if type_mismatches:
    print("Differences:", type_mismatches)

assert not type_mismatches, (
    f"Silver data type mismatches: {type_mismatches}"
)


# 6. Reconcile all records and original Bronze fields

bronze_count = bronze.count()
transformed_count = silver_transformed.count()
existing_count = existing.count()

assert bronze_count == 148522
assert transformed_count == 148522
assert existing_count == 148522

original_columns = bronze.columns

original_forward = bronze.select(
    *original_columns
).exceptAll(
    silver_transformed.select(*original_columns)
).count()

original_reverse = silver_transformed.select(
    *original_columns
).exceptAll(
    bronze.select(*original_columns)
).count()

assert original_forward == 0
assert original_reverse == 0


# 7. Compare all transformed values against existing Silver

forward = silver_transformed.exceptAll(
    existing
).count()

reverse = existing.exceptAll(
    silver_transformed
).count()

print("\n" + "=" * 65)
print("STEP 9.9C — BRONZE TO SILVER ASSETS TRANSFORMATION")
print("=" * 65)

print("Bronze records:", bronze_count)
print("Silver transformed records:", transformed_count)
print("Existing Silver records:", existing_count)

print("Original Bronze columns:", len(bronze.columns))
print("Silver columns:", len(silver_transformed.columns))

print("Data type mismatches:", len(type_mismatches))
print(
    "Original field mismatches:",
    original_forward + original_reverse
)
print("Forward data mismatches:", forward)
print("Reverse data mismatches:", reverse)

assert forward == 0, (
    f"Forward mismatches: {forward}"
)

assert reverse == 0, (
    f"Reverse mismatches: {reverse}"
)

print("\nSTEP 9.9C SILVER ASSETS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.9D — Rebuild Work Orders Silver from Bronze Delta.
# Preserve all 2,231,535 records, original fields, and data-quality rules.
# Compare complete results without modifying existing Delta tables.

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType,
    TimestampNTZType, DecimalType, BooleanType
)

BASE = "workspace.assetfusion360"

bronze_wo = spark.table(f"{BASE}.bronze_amps_workorders")
existing_wo = spark.table(f"{BASE}.silver_amps_workorders")

DATE_FIELDS = [
    "evt_date",
    "evt_reported",
    "evt_start",
    "evt_due",
    "evt_completed",
    "evt_created",
    "evt_updated"
]

DATE_OUTPUTS = {
    field: f"{field}_ts"
    for field in DATE_FIELDS
}

DATE_FLAGS = {
    field: f"dq_{field}_invalid"
    for field in DATE_FIELDS
}

NUMERIC_FIELDS = {
    "evt_sqlidentity": (
        "wo_identity_int",
        "dq_identity_invalid"
    ),
    "evt_duration": (
        "wo_duration_decimal",
        "dq_duration_invalid"
    ),
    "evt_updatecount": (
        "wo_updatecount_int",
        "dq_updatecount_invalid"
    )
}

OTHER_FLAGS = [
    "dq_workorder_code_missing",
    "dq_asset_ref_missing",
    "dq_any_issue"
]

ALL_FLAGS = (
    list(DATE_FLAGS.values())
    + [config[1] for config in NUMERIC_FIELDS.values()]
    + OTHER_FLAGS
)

# Reuse the exact parsing logic from Step 9.9C
required_functions = [
    "missing",
    "parse_integer",
    "parse_decimal",
    "parse_timestamp"
]

for name in required_functions:
    assert name in globals(), (
        f"Run Cell 1 first; missing helper: {name}"
    )

# The original Work Orders parser accepts these same formats.
# Its numeric conversions also use int64 and decimal(20,6).

added_wo_schema = StructType(
    [
        StructField(
            name,
            TimestampNTZType(),
            True
        )
        for name in DATE_OUTPUTS.values()
    ]
    + [
        StructField("wo_identity_int", LongType(), True),
        StructField(
            "wo_duration_decimal",
            DecimalType(20, 6),
            True
        ),
        StructField("wo_updatecount_int", LongType(), True)
    ]
    + [
        StructField(name, BooleanType(), True)
        for name in ALL_FLAGS
    ]
)

@F.udf(returnType=added_wo_schema)
def transform_workorder(*values):
    source = dict(zip(
        [
            "evt_code",
            "evt_object",
            *DATE_FIELDS,
            *NUMERIC_FIELDS.keys()
        ],
        values
    ))

    converted_dates = {}
    flags = {}

    for field in DATE_FIELDS:
        value, invalid = parse_timestamp(source.get(field))
        converted_dates[DATE_OUTPUTS[field]] = value
        flags[DATE_FLAGS[field]] = invalid

    identity, identity_invalid = parse_integer(
        source.get("evt_sqlidentity")
    )

    duration, duration_invalid = parse_decimal(
        source.get("evt_duration")
    )

    updatecount, updatecount_invalid = parse_integer(
        source.get("evt_updatecount")
    )

    flags["dq_identity_invalid"] = (
        identity_invalid
        or missing(source.get("evt_sqlidentity"))
    )

    flags["dq_duration_invalid"] = duration_invalid
    flags["dq_updatecount_invalid"] = updatecount_invalid

    flags["dq_workorder_code_missing"] = missing(
        source.get("evt_code")
    )

    flags["dq_asset_ref_missing"] = missing(
        source.get("evt_object")
    )

    flags["dq_any_issue"] = any(flags.values())

    return tuple(
        [converted_dates[name] for name in DATE_OUTPUTS.values()]
        + [identity, duration, updatecount]
        + [flags[name] for name in ALL_FLAGS]
    )

# Validate source layout and required fields
required_columns = [
    "evt_code",
    "evt_object",
    *DATE_FIELDS,
    *NUMERIC_FIELDS.keys()
]

assert len(bronze_wo.columns) == 59

for name in required_columns:
    assert name in bronze_wo.columns, (
        f"Missing Bronze column: {name}"
    )

assert len(existing_wo.columns) == 82

print("BRONZE WORK ORDERS SCHEMA: PASS")

# Perform the full transformation using the existing Bronze data
parsed_wo = bronze_wo.withColumn(
    "_silver_parsed",
    transform_workorder(
        *[F.col(name) for name in required_columns]
    )
)

bronze_column_names = set(bronze_wo.columns)

silver_wo_transformed = parsed_wo.select(
    *[
        (
            F.col(name)
            if name in bronze_column_names
            else F.col(f"_silver_parsed.{name}")
        ).alias(name)
        for name in existing_wo.columns
    ]
)

# Compare actual schema names, types, nullability and metadata
expected_schema = existing_wo.schema
actual_schema = silver_wo_transformed.schema

expected_names = [
    field.name for field in expected_schema.fields
]

actual_names = [
    field.name for field in actual_schema.fields
]

assert expected_names == actual_names, (
    "Work Orders column names/order mismatch"
)

type_mismatches = [
    (
        expected.name,
        str(expected.dataType),
        str(actual.dataType)
    )
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if expected.dataType != actual.dataType
]

metadata_differences = [
    expected.name
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if (
        expected.nullable != actual.nullable
        or expected.metadata != actual.metadata
    )
]

print("\nSCHEMA VALIDATION")
print("-" * 55)
print("Expected columns:", len(expected_names))
print("Actual columns:", len(actual_names))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)

if type_mismatches:
    print("Differences:", type_mismatches)

assert len(type_mismatches) == 0, (
    "Work Orders Silver data type mismatch"
)

# Validate all records
bronze_count = bronze_wo.count()
transformed_count = silver_wo_transformed.count()
existing_count = existing_wo.count()

assert bronze_count == 2231535
assert transformed_count == 2231535
assert existing_count == 2231535

# Verify that every original Bronze field is preserved
original_columns = bronze_wo.columns

original_forward = bronze_wo.select(
    *original_columns
).exceptAll(
    silver_wo_transformed.select(*original_columns)
).count()

original_reverse = silver_wo_transformed.select(
    *original_columns
).exceptAll(
    bronze_wo.select(*original_columns)
).count()

assert original_forward == 0
assert original_reverse == 0

# Verify every transformed row against existing Silver Delta
forward = silver_wo_transformed.exceptAll(
    existing_wo
).count()

reverse = existing_wo.exceptAll(
    silver_wo_transformed
).count()

print("\n" + "=" * 65)
print("STEP 9.9D — WORK ORDERS BRONZE TO SILVER")
print("=" * 65)

print("Bronze records:", bronze_count)
print("Silver transformed records:", transformed_count)
print("Existing Silver records:", existing_count)
print("Bronze columns:", len(original_columns))
print("Silver columns:", len(actual_names))
print("Data type mismatches:", len(type_mismatches))
print(
    "Original field mismatches:",
    original_forward + original_reverse
)
print("Forward data mismatches:", forward)
print("Reverse data mismatches:", reverse)

assert forward == 0, (
    f"Forward mismatches: {forward}"
)

assert reverse == 0, (
    f"Reverse mismatches: {reverse}"
)

print("\nSTEP 9.9D SILVER WORK ORDERS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11C — Publish Work Orders Silver independently to staging.
# Validate the Bronze-derived output against production before publication.
# Never overwrite any existing staging or production Delta table.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TABLE = "silver_amps_workorders"
TARGET = f"{STAGING}.{TABLE}"
REFERENCE = f"{PRODUCTION}.{TABLE}"
EXPECTED_ROWS = 2231535

# Require the Work Orders transformation from Cell 2.
try:
    source = silver_wo_transformed
except NameError:
    raise RuntimeError(
        "Run Cell 2 — Work Orders Transformation first."
    )

reference = spark.table(REFERENCE)

# Check whether a previous publication already exists.
if spark.catalog.tableExists(TARGET):
    raise RuntimeError(
        f"Existing staging table detected: {TARGET}. "
        "No write attempted."
    )

# Validate transformed schema.
assert source.schema.names == reference.schema.names

type_mismatches = [
    name
    for name in source.schema.names
    if source.schema[name].dataType != reference.schema[name].dataType
]

assert not type_mismatches, (
    f"Schema mismatches: {type_mismatches}"
)

# Validate row counts and complete contents.
source_count = source.count()
reference_count = reference.count()

assert source_count == EXPECTED_ROWS
assert reference_count == EXPECTED_ROWS

forward = source.exceptAll(reference).count()
reverse = reference.exceptAll(source).count()

assert forward == 0 and reverse == 0, (
    f"Source mismatches: {forward + reverse}"
)

print("WORK ORDERS TRANSFORMATION: VERIFIED")
print("STAGING OUTPUT SAFETY: PASS")

# Create only a new staging table. No overwrite mode.
source.write.format("delta").saveAsTable(TARGET)

published = spark.table(TARGET)

published_count = published.count()

assert published_count == EXPECTED_ROWS
assert published.schema.names == reference.schema.names

published_forward = published.exceptAll(reference).count()
published_reverse = reference.exceptAll(published).count()

assert published_forward == 0
assert published_reverse == 0

print("\n" + "=" * 65)
print("STEP 9.11C — WORK ORDERS SILVER STAGING")
print("=" * 65)
print("Source records:", source_count)
print("Published records:", published_count)
print("Published table:", TARGET)
print("Data mismatches:", published_forward + published_reverse)

print("\nSTEP 9.11C WORK ORDERS STAGING: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.9E — Rebuild Labor Silver from existing Bronze Delta.
# Preserve 3,230,423 records, financial values, and original quality rules.
# Validate every transformed row; do not modify existing Delta tables.

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType,
    TimestampNTZType, DecimalType, BooleanType
)

BASE = "workspace.assetfusion360"

bronze_labor = spark.table(f"{BASE}.bronze_amps_labor")
existing_labor = spark.table(f"{BASE}.silver_amps_labor")

SOURCE_FIELDS = [
    "boo_sqlidentity",
    "boo_event",
    "boo_date",
    "boo_hours",
    "boo_rate",
    "boo_cost"
]

TYPED_FIELDS = [
    StructField("labor_identity_int", LongType(), True),
    StructField("labor_workorder_id_int", LongType(), True),
    StructField("labor_date_ts", TimestampNTZType(), True),
    StructField("labor_hours_decimal", DecimalType(20, 6), True),
    StructField("labor_rate_decimal", DecimalType(20, 6), True),
    StructField("labor_cost_decimal", DecimalType(20, 6), True)
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

added_labor_schema = StructType(
    TYPED_FIELDS + [
        StructField(name, BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

# Original integer and decimal parsing rules
def labor_missing(value):
    return value is None or str(value).strip() == ""


def labor_parse_integer(value):
    if labor_missing(value):
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


def labor_parse_decimal(value):
    if labor_missing(value):
        return None, False

    try:
        number = Decimal(str(value).strip())

        if not number.is_finite():
            return None, True

        number = number.quantize(Decimal("0.000001"))

        if abs(number) >= Decimal("100000000000000"):
            return None, True

        return number, False

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


# Original Labor date parser: six accepted formats
def labor_parse_date(value):
    if labor_missing(value):
        return None, False

    normalized = re.sub(
        r"\s+", " ", str(value).strip()
    )

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt), False
        except ValueError:
            continue

    return None, True


# Transform one Labor record using the original business rules
@F.udf(returnType=added_labor_schema)
def transform_labor_record(
    identity_raw,
    event_raw,
    date_raw,
    hours_raw,
    rate_raw,
    cost_raw
):
    identity, identity_invalid = labor_parse_integer(identity_raw)
    event, event_invalid = labor_parse_integer(event_raw)
    labor_date, date_invalid = labor_parse_date(date_raw)

    hours, hours_invalid = labor_parse_decimal(hours_raw)
    rate, rate_invalid = labor_parse_decimal(rate_raw)
    cost, cost_invalid = labor_parse_decimal(cost_raw)

    flags = {
        "dq_labor_identity_invalid": (
            identity_invalid or labor_missing(identity_raw)
        ),
        "dq_workorder_reference_invalid": (
            event_invalid or labor_missing(event_raw)
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
        *[flags[name] for name in QUALITY_FLAGS]
    )


# Validate Bronze input and original field types
assert len(bronze_labor.columns) == 22

for name in SOURCE_FIELDS:
    assert name in bronze_labor.columns, (
        f"Missing Bronze column: {name}"
    )

assert all(
    field.dataType.simpleString() == "string"
    for field in bronze_labor.schema.fields
), "Unexpected Bronze field type"

assert len(existing_labor.columns) == 38

print("BRONZE LABOR SCHEMA: PASS")


# Rebuild all Silver fields from Bronze
parsed_labor = bronze_labor.withColumn(
    "_silver_parsed",
    transform_labor_record(
        *[F.col(name) for name in SOURCE_FIELDS]
    )
)

bronze_field_names = set(bronze_labor.columns)

silver_labor_transformed = parsed_labor.select(
    *[
        (
            F.col(name)
            if name in bronze_field_names
            else F.col(f"_silver_parsed.{name}")
        ).alias(name)
        for name in existing_labor.columns
    ]
)


# Compare actual schema against existing Silver Delta
expected_schema = existing_labor.schema
actual_schema = silver_labor_transformed.schema

assert actual_schema.names == expected_schema.names, (
    "Silver Labor column names or order mismatch"
)

type_mismatches = [
    (
        expected.name,
        str(expected.dataType),
        str(actual.dataType)
    )
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if expected.dataType != actual.dataType
]

metadata_differences = [
    expected.name
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if (
        expected.nullable != actual.nullable
        or expected.metadata != actual.metadata
    )
]

print("\nSCHEMA VALIDATION")
print("-" * 55)
print("Expected columns:", len(expected_schema.fields))
print("Actual columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)

if type_mismatches:
    print("Differences:", type_mismatches)

assert len(type_mismatches) == 0, (
    "Labor Silver data type mismatch"
)


# Reconcile all source and output records
bronze_count = bronze_labor.count()
transformed_count = silver_labor_transformed.count()
existing_count = existing_labor.count()

assert bronze_count == 3230423
assert transformed_count == 3230423
assert existing_count == 3230423


# Compare every original Bronze value
original_columns = bronze_labor.columns

original_forward = bronze_labor.select(
    *original_columns
).exceptAll(
    silver_labor_transformed.select(*original_columns)
).count()

original_reverse = silver_labor_transformed.select(
    *original_columns
).exceptAll(
    bronze_labor.select(*original_columns)
).count()

assert original_forward == 0
assert original_reverse == 0


# Compare all Silver records against existing Delta
forward = silver_labor_transformed.exceptAll(
    existing_labor
).count()

reverse = existing_labor.exceptAll(
    silver_labor_transformed
).count()

print("\n" + "=" * 65)
print("STEP 9.9E — LABOR BRONZE TO SILVER")
print("=" * 65)

print("Bronze records:", bronze_count)
print("Silver transformed records:", transformed_count)
print("Existing Silver records:", existing_count)
print("Bronze columns:", len(original_columns))
print("Silver columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Original field mismatches:",
    original_forward + original_reverse
)
print("Forward data mismatches:", forward)
print("Reverse data mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.9E SILVER LABOR TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.9F — Rebuild Transactions Silver from Bronze Delta.
# Preserve all 755,122 records, original values, and transaction quality rules.
# Validate complete equivalence without modifying existing Delta tables.

import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType,
    TimestampNTZType, BooleanType
)

BASE = "workspace.assetfusion360"

bronze_tx = spark.table(f"{BASE}.bronze_amps_transactions")
existing_tx = spark.table(f"{BASE}.silver_amps_transactions")

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

SOURCE_FIELDS = list(INTEGER_FIELDS) + list(DATE_FIELDS)

ADDED_SCHEMA = StructType(
    [
        StructField(name, LongType(), True)
        for name in INTEGER_FIELDS.values()
    ]
    + [
        StructField(name, TimestampNTZType(), True)
        for name in DATE_FIELDS.values()
    ]
    + [
        StructField(name, BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

# Preserve the original missing-value and integer-validation behavior.
def tx_missing(value):
    return value is None or str(value).strip() == ""

def tx_parse_integer(value):
    if tx_missing(value):
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

def tx_parse_date(value):
    if tx_missing(value):
        return None, False

    text = re.sub(r"\s+", " ", str(value).strip())

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt), False
        except ValueError:
            continue

    return None, True

# Transform all seven typed values and eight original quality flags.
@F.udf(returnType=ADDED_SCHEMA)
def transform_transaction(*values):
    source = dict(zip(SOURCE_FIELDS, values))
    converted = {}
    flags = {}

    for original, target in INTEGER_FIELDS.items():
        value, invalid = tx_parse_integer(source[original])
        converted[target] = value
        flags[INTEGER_FLAGS[original]] = invalid

    for original, target in DATE_FIELDS.items():
        value, invalid = tx_parse_date(source[original])
        converted[target] = value
        flags[DATE_FLAGS[original]] = invalid

    # Transaction code and identity are required.
    # Transaction request is optional, so a null request is valid.
    for field in ["tra_code", "tra_sqlidentity"]:
        if tx_missing(source[field]):
            flags[INTEGER_FLAGS[field]] = True

    flags["dq_any_issue"] = any(flags.values())

    return tuple(
        [converted[name] for name in INTEGER_FIELDS.values()]
        + [converted[name] for name in DATE_FIELDS.values()]
        + [flags[name] for name in QUALITY_FLAGS]
    )

# Validate Bronze inputs and preserve every original source field.
REQUIRED_COLUMNS = SOURCE_FIELDS + [
    "tra_type",
    "tra_status",
    "tra_fromentity",
    "tra_fromcode",
    "tra_toentity",
    "tra_tocode"
]

assert len(bronze_tx.columns) == 18
assert len(existing_tx.columns) == 33

for name in REQUIRED_COLUMNS:
    assert name in bronze_tx.columns, (
        f"Missing Bronze field: {name}"
    )

assert all(
    field.dataType.simpleString() == "string"
    for field in bronze_tx.schema.fields
), "Unexpected Bronze field type"

print("BRONZE TRANSACTIONS SCHEMA: PASS")

# Rebuild Silver using Bronze, not imported Silver values.
parsed_tx = bronze_tx.withColumn(
    "_silver_parsed",
    transform_transaction(
        *[F.col(name) for name in SOURCE_FIELDS]
    )
)

bronze_names = set(bronze_tx.columns)

silver_tx_transformed = parsed_tx.select(
    *[
        (
            F.col(name)
            if name in bronze_names
            else F.col(f"_silver_parsed.{name}")
        ).alias(name)
        for name in existing_tx.columns
    ]
)

# Compare the actual transformed schema with production Silver.
expected_schema = existing_tx.schema
actual_schema = silver_tx_transformed.schema

assert actual_schema.names == expected_schema.names, (
    "Silver column names or order mismatch"
)

type_mismatches = [
    (
        expected.name,
        str(expected.dataType),
        str(actual.dataType)
    )
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if expected.dataType != actual.dataType
]

metadata_differences = [
    expected.name
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if (
        expected.nullable != actual.nullable
        or expected.metadata != actual.metadata
    )
]

print("\nSCHEMA VALIDATION")
print("-" * 55)
print("Expected columns:", len(expected_schema.fields))
print("Actual columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)

if type_mismatches:
    print("Differences:", type_mismatches)

assert not type_mismatches, (
    "Transactions Silver schema data-type mismatch"
)

# Reconcile every record.
bronze_count = bronze_tx.count()
transformed_count = silver_tx_transformed.count()
existing_count = existing_tx.count()

assert bronze_count == 755122
assert transformed_count == 755122
assert existing_count == 755122

# Compare every original source field and retain duplicates.
original_columns = bronze_tx.columns

original_forward = bronze_tx.select(
    *original_columns
).exceptAll(
    silver_tx_transformed.select(*original_columns)
).count()

original_reverse = silver_tx_transformed.select(
    *original_columns
).exceptAll(
    bronze_tx.select(*original_columns)
).count()

assert original_forward == 0
assert original_reverse == 0

# Full Silver data reconciliation in both directions.
forward = silver_tx_transformed.exceptAll(
    existing_tx
).count()

reverse = existing_tx.exceptAll(
    silver_tx_transformed
).count()

print("\n" + "=" * 65)
print("STEP 9.9F — TRANSACTIONS BRONZE TO SILVER")
print("=" * 65)

print("Bronze records:", bronze_count)
print("Silver transformed records:", transformed_count)
print("Existing Silver records:", existing_count)
print("Bronze columns:", len(original_columns))
print("Silver columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)
print(
    "Original field mismatches:",
    original_forward + original_reverse
)
print("Forward data mismatches:", forward)
print("Reverse data mismatches:", reverse)

assert forward == 0, f"Forward mismatches: {forward}"
assert reverse == 0, f"Reverse mismatches: {reverse}"

print("\nSTEP 9.9F SILVER TRANSACTIONS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11D — Publish Transactions Silver to staging.
# Revalidate the actual Bronze-derived transformation before publication.
# Never overwrite existing staging or production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TABLE = "silver_amps_transactions"
TARGET = f"{STAGING}.{TABLE}"
REFERENCE = f"{PRODUCTION}.{TABLE}"
EXPECTED_ROWS = 755122

reference = spark.table(REFERENCE)

# 1. Verify whether staging already exists.
if spark.catalog.tableExists(TARGET):
    published = spark.table(TARGET)

    assert published.count() == EXPECTED_ROWS
    assert published.schema.names == reference.schema.names

    forward = published.exceptAll(reference).count()
    reverse = reference.exceptAll(published).count()

    assert forward == 0 and reverse == 0

    print("TRANSACTIONS STAGING: ALREADY EXISTS — VERIFIED")

else:
    # 2. Require the freshly executed Transactions transformation.
    try:
        source = silver_tx_transformed
    except NameError:
        raise RuntimeError(
            "Run Step 9.9F Transactions transformation first."
        )

    assert source.count() == EXPECTED_ROWS
    assert reference.count() == EXPECTED_ROWS

    assert source.schema.names == reference.schema.names

    assert all(
        source.schema[c].dataType == reference.schema[c].dataType
        for c in reference.columns
    )

    # 3. Reconcile complete transformed data.
    forward = source.exceptAll(reference).count()
    reverse = reference.exceptAll(source).count()

    assert forward == 0 and reverse == 0, (
        f"Transactions source mismatches: {forward + reverse}"
    )

    # 4. Publish without overwriting existing tables.
    if spark.catalog.tableExists(TARGET):
        raise RuntimeError(
            f"Staging table appeared during validation: {TARGET}"
        )

    source.write.format("delta").saveAsTable(TARGET)

    # 5. Verify newly published Delta data.
    published = spark.table(TARGET)

    assert published.count() == EXPECTED_ROWS

    published_forward = published.exceptAll(reference).count()
    published_reverse = reference.exceptAll(published).count()

    assert published_forward == 0
    assert published_reverse == 0

    print("TRANSACTIONS STAGING: NEWLY PUBLISHED — VERIFIED")

print("Published table:", TARGET)
print("Expected records:", EXPECTED_ROWS)
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.9G — Rebuild Transaction Details Silver from Bronze Delta.
# Preserve 2,301,927 records, all source fields, and original quality rules.
# Compare complete results without modifying existing Delta tables.

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType,
    TimestampNTZType, DecimalType, BooleanType
)

BASE = "workspace.assetfusion360"

bronze_detail = spark.table(
    f"{BASE}.bronze_amps_transaction_details"
)

existing_detail = spark.table(
    f"{BASE}.silver_amps_transaction_details"
)

# Original source-to-Silver typed field mappings
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
    name: f"dq_{name}_invalid"
    for name in INTEGER_FIELDS
}

DATE_FLAGS = {
    name: f"dq_{name}_invalid"
    for name in DATE_FIELDS
}

DECIMAL_FLAGS = {
    name: f"dq_{name}_invalid"
    for name in DECIMAL_FIELDS
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

SOURCE_FIELDS = (
    list(INTEGER_FIELDS)
    + list(DATE_FIELDS)
    + list(DECIMAL_FIELDS)
)

assert len(QUALITY_FLAGS) == 19
assert len(SOURCE_FIELDS) == 14

# Schema matches the original PyArrow Silver output.
# TimestampNTZ preserves timezone-free source timestamps.
ADDED_SCHEMA = StructType(
    [
        StructField(name, LongType(), True)
        for name in INTEGER_FIELDS.values()
    ]
    + [
        StructField(name, TimestampNTZType(), True)
        for name in DATE_FIELDS.values()
    ]
    + [
        StructField(name, DecimalType(20, 6), True)
        for name in DECIMAL_FIELDS.values()
    ]
    + [
        StructField(name, BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

# Original source parsing behavior
def detail_missing(value):
    return value is None or str(value).strip() == ""


def detail_parse_integer(value):
    if detail_missing(value):
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


def detail_parse_decimal(value):
    if detail_missing(value):
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

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


def detail_parse_date(value):
    if detail_missing(value):
        return None, False

    text = re.sub(
        r"\s+", " ", str(value).strip()
    )

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt), False
        except ValueError:
            continue

    return None, True


# Apply all 14 typed conversions and 19 quality flags
@F.udf(returnType=ADDED_SCHEMA)
def transform_detail_record(*values):

    source = dict(zip(SOURCE_FIELDS, values))
    converted = {}
    flags = {}

    for original, target in INTEGER_FIELDS.items():
        value, invalid = detail_parse_integer(
            source[original]
        )

        converted[target] = value
        flags[INTEGER_FLAGS[original]] = invalid

    for original, target in DATE_FIELDS.items():
        value, invalid = detail_parse_date(
            source[original]
        )

        converted[target] = value
        flags[DATE_FLAGS[original]] = invalid

    for original, target in DECIMAL_FIELDS.items():
        value, invalid = detail_parse_decimal(
            source[original]
        )

        converted[target] = value
        flags[DECIMAL_FLAGS[original]] = invalid

    # Transaction/detail identifiers are required.
    # Work-order and part references may be null.
    for name in ["trl_trans", "trl_sqlidentity"]:
        if detail_missing(source[name]):
            flags[INTEGER_FLAGS[name]] = True

    quantity = converted["detail_quantity_decimal"]
    price = converted["detail_price_decimal"]

    flags["dq_negative_quantity"] = (
        quantity is not None and quantity < 0
    )

    flags["dq_negative_price"] = (
        price is not None and price < 0
    )

    flags["dq_zero_quantity"] = (
        quantity is not None and quantity == 0
    )

    flags["dq_zero_price"] = (
        price is not None and price == 0
    )

    # Informational financial flags are excluded.
    # Only parsing and required-field issues count.
    error_flags = (
        list(INTEGER_FLAGS.values())
        + list(DATE_FLAGS.values())
        + list(DECIMAL_FLAGS.values())
    )

    flags["dq_any_issue"] = any(
        flags[name] for name in error_flags
    )

    return tuple(
        [converted[name] for name in INTEGER_FIELDS.values()]
        + [converted[name] for name in DATE_FIELDS.values()]
        + [converted[name] for name in DECIMAL_FIELDS.values()]
        + [flags[name] for name in QUALITY_FLAGS]
    )


# Validate Bronze input schema and required fields
assert len(bronze_detail.columns) == 35, (
    "Expected 35 original Bronze columns"
)

assert len(existing_detail.columns) == 68, (
    "Expected 68 Silver columns"
)

for name in SOURCE_FIELDS:
    assert name in bronze_detail.columns, (
        f"Missing Bronze field: {name}"
    )

assert all(
    field.dataType.simpleString() == "string"
    for field in bronze_detail.schema.fields
), "Unexpected Bronze field type"

assert not (
    set(ADDED_SCHEMA.names)
    & set(bronze_detail.columns)
), "Silver field name collision"

print("BRONZE TRANSACTION DETAILS SCHEMA: PASS")


# Transform Bronze into a temporary Silver DataFrame
parsed_detail = bronze_detail.withColumn(
    "_silver_parsed",
    transform_detail_record(
        *[F.col(name) for name in SOURCE_FIELDS]
    )
)

bronze_names = set(bronze_detail.columns)

silver_detail_transformed = parsed_detail.select(
    *[
        (
            F.col(name)
            if name in bronze_names
            else F.col(f"_silver_parsed.{name}")
        ).alias(name)
        for name in existing_detail.columns
    ]
)


# Compare actual and existing Silver schemas
expected_schema = existing_detail.schema
actual_schema = silver_detail_transformed.schema

assert actual_schema.names == expected_schema.names, (
    "Silver column names or order mismatch"
)

type_mismatches = [
    (
        expected.name,
        str(expected.dataType),
        str(actual.dataType)
    )
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if expected.dataType != actual.dataType
]

metadata_differences = [
    expected.name
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if (
        expected.nullable != actual.nullable
        or expected.metadata != actual.metadata
    )
]

print("\nSCHEMA VALIDATION")
print("-" * 55)
print("Expected columns:", len(expected_schema.fields))
print("Actual columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)

if type_mismatches:
    print("Differences:", type_mismatches)

assert not type_mismatches, (
    "Transaction Details Silver data-type mismatch"
)


# Reconcile original and transformed record counts
bronze_count = bronze_detail.count()
transformed_count = silver_detail_transformed.count()
existing_count = existing_detail.count()

assert bronze_count == 2301927, (
    f"Unexpected Bronze records: {bronze_count}"
)

assert transformed_count == 2301927, (
    f"Unexpected transformed records: {transformed_count}"
)

assert existing_count == 2301927, (
    f"Unexpected existing Silver records: {existing_count}"
)


# Verify every original Bronze field is preserved
original_columns = bronze_detail.columns

original_forward = bronze_detail.select(
    *original_columns
).exceptAll(
    silver_detail_transformed.select(*original_columns)
).count()

original_reverse = silver_detail_transformed.select(
    *original_columns
).exceptAll(
    bronze_detail.select(*original_columns)
).count()

assert original_forward == 0, (
    f"Bronze preservation mismatch: {original_forward}"
)

assert original_reverse == 0, (
    f"Reverse Bronze mismatch: {original_reverse}"
)


# Compare the entire transformed Silver dataset
forward = silver_detail_transformed.exceptAll(
    existing_detail
).count()

reverse = existing_detail.exceptAll(
    silver_detail_transformed
).count()

print("\n" + "=" * 65)
print("STEP 9.9G — TRANSACTION DETAILS BRONZE TO SILVER")
print("=" * 65)

print("Bronze records:", bronze_count)
print("Silver transformed records:", transformed_count)
print("Existing Silver records:", existing_count)

print("Bronze columns:", len(original_columns))
print("Silver columns:", len(actual_schema.fields))

print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)
print(
    "Original field mismatches:",
    original_forward + original_reverse
)

print("Forward data mismatches:", forward)
print("Reverse data mismatches:", reverse)

assert forward == 0, (
    f"Forward mismatches: {forward}"
)

assert reverse == 0, (
    f"Reverse mismatches: {reverse}"
)

print("\nSTEP 9.9G SILVER TRANSACTION DETAILS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11E — Publish Transaction Details Silver into Delta staging.
# Validate all 2,301,927 records against the existing Silver reference.
# Never overwrite staging tables or modify production Delta tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

TABLE = "silver_amps_transaction_details"
TARGET = f"{STAGING}.{TABLE}"
REFERENCE = f"{PRODUCTION}.{TABLE}"
EXPECTED_ROWS = 2301927

reference = spark.table(REFERENCE)

# 1. If the table already exists, verify it without modifying it.
if spark.catalog.tableExists(TARGET):
    published = spark.table(TARGET)

    assert published.count() == EXPECTED_ROWS
    assert published.schema.names == reference.schema.names

    forward = published.exceptAll(reference).count()
    reverse = reference.exceptAll(published).count()

    assert forward == 0 and reverse == 0, (
        f"Existing staging mismatches: {forward + reverse}"
    )

    print("TRANSACTION DETAILS STAGING: ALREADY EXISTS — VERIFIED")

else:
    # 2. Use the actual Bronze-derived transformation.
    try:
        source = silver_detail_transformed
    except NameError:
        raise RuntimeError(
            "Run Step 9.9G Transaction Details Transformation first."
        )

    # 3. Validate source schema and complete record counts.
    assert source.schema.names == reference.schema.names

    type_mismatches = [
        name
        for name in reference.columns
        if source.schema[name].dataType != reference.schema[name].dataType
    ]

    assert not type_mismatches, (
        f"Schema mismatches: {type_mismatches}"
    )

    assert source.count() == EXPECTED_ROWS
    assert reference.count() == EXPECTED_ROWS

    # 4. Compare every transformed row before publication.
    source_forward = source.exceptAll(reference).count()
    source_reverse = reference.exceptAll(source).count()

    assert source_forward == 0 and source_reverse == 0, (
        f"Source data mismatches: {source_forward + source_reverse}"
    )

    print("TRANSACTION DETAILS TRANSFORMATION: VERIFIED")
    print("STAGING OUTPUT SAFETY: PASS")

    # 5. Create the staging table only if it remains absent.
    if spark.catalog.tableExists(TARGET):
        raise RuntimeError(
            f"Staging table appeared during validation: {TARGET}"
        )

    source.write.format("delta").saveAsTable(TARGET)

    # 6. Independently validate the published Delta table.
    published = spark.table(TARGET)

    published_count = published.count()

    assert published_count == EXPECTED_ROWS
    assert published.schema.names == reference.schema.names

    published_type_mismatches = [
        name
        for name in reference.columns
        if published.schema[name].dataType != reference.schema[name].dataType
    ]

    assert not published_type_mismatches

    forward = published.exceptAll(reference).count()
    reverse = reference.exceptAll(published).count()

    assert forward == 0 and reverse == 0, (
        f"Published data mismatches: {forward + reverse}"
    )

    print("TRANSACTION DETAILS STAGING: NEWLY PUBLISHED — VERIFIED")

print("\n" + "=" * 65)
print("STEP 9.11E — TRANSACTION DETAILS SILVER STAGING")
print("=" * 65)
print("Published table:", TARGET)
print("Published records:", published.count())
print("Data mismatches:", forward + reverse)
print("\nSTEP 9.11E: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")


# COMMAND ----------


# Step 9.9G — Rebuild Transaction Details Silver from Bronze Delta.
# Preserve 2,301,927 records, all source fields, and original quality rules.
# Compare complete results without modifying existing Delta tables.

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType,
    TimestampNTZType, DecimalType, BooleanType
)

BASE = "workspace.assetfusion360"

bronze_detail = spark.table(
    f"{BASE}.bronze_amps_transaction_details"
)

existing_detail = spark.table(
    f"{BASE}.silver_amps_transaction_details"
)

# Original source-to-Silver typed field mappings
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
    name: f"dq_{name}_invalid"
    for name in INTEGER_FIELDS
}

DATE_FLAGS = {
    name: f"dq_{name}_invalid"
    for name in DATE_FIELDS
}

DECIMAL_FLAGS = {
    name: f"dq_{name}_invalid"
    for name in DECIMAL_FIELDS
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

SOURCE_FIELDS = (
    list(INTEGER_FIELDS)
    + list(DATE_FIELDS)
    + list(DECIMAL_FIELDS)
)

assert len(QUALITY_FLAGS) == 19
assert len(SOURCE_FIELDS) == 14

# Schema matches the original PyArrow Silver output.
# TimestampNTZ preserves timezone-free source timestamps.
ADDED_SCHEMA = StructType(
    [
        StructField(name, LongType(), True)
        for name in INTEGER_FIELDS.values()
    ]
    + [
        StructField(name, TimestampNTZType(), True)
        for name in DATE_FIELDS.values()
    ]
    + [
        StructField(name, DecimalType(20, 6), True)
        for name in DECIMAL_FIELDS.values()
    ]
    + [
        StructField(name, BooleanType(), True)
        for name in QUALITY_FLAGS
    ]
)

# Original source parsing behavior
def detail_missing(value):
    return value is None or str(value).strip() == ""


def detail_parse_integer(value):
    if detail_missing(value):
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


def detail_parse_decimal(value):
    if detail_missing(value):
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

    except (InvalidOperation, ValueError, OverflowError):
        return None, True


def detail_parse_date(value):
    if detail_missing(value):
        return None, False

    text = re.sub(
        r"\s+", " ", str(value).strip()
    )

    formats = [
        "%b %d %Y %I:%M%p",
        "%b %d %Y %I:%M:%S%p",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt), False
        except ValueError:
            continue

    return None, True


# Apply all 14 typed conversions and 19 quality flags
@F.udf(returnType=ADDED_SCHEMA)
def transform_detail_record(*values):

    source = dict(zip(SOURCE_FIELDS, values))
    converted = {}
    flags = {}

    for original, target in INTEGER_FIELDS.items():
        value, invalid = detail_parse_integer(
            source[original]
        )

        converted[target] = value
        flags[INTEGER_FLAGS[original]] = invalid

    for original, target in DATE_FIELDS.items():
        value, invalid = detail_parse_date(
            source[original]
        )

        converted[target] = value
        flags[DATE_FLAGS[original]] = invalid

    for original, target in DECIMAL_FIELDS.items():
        value, invalid = detail_parse_decimal(
            source[original]
        )

        converted[target] = value
        flags[DECIMAL_FLAGS[original]] = invalid

    # Transaction/detail identifiers are required.
    # Work-order and part references may be null.
    for name in ["trl_trans", "trl_sqlidentity"]:
        if detail_missing(source[name]):
            flags[INTEGER_FLAGS[name]] = True

    quantity = converted["detail_quantity_decimal"]
    price = converted["detail_price_decimal"]

    flags["dq_negative_quantity"] = (
        quantity is not None and quantity < 0
    )

    flags["dq_negative_price"] = (
        price is not None and price < 0
    )

    flags["dq_zero_quantity"] = (
        quantity is not None and quantity == 0
    )

    flags["dq_zero_price"] = (
        price is not None and price == 0
    )

    # Informational financial flags are excluded.
    # Only parsing and required-field issues count.
    error_flags = (
        list(INTEGER_FLAGS.values())
        + list(DATE_FLAGS.values())
        + list(DECIMAL_FLAGS.values())
    )

    flags["dq_any_issue"] = any(
        flags[name] for name in error_flags
    )

    return tuple(
        [converted[name] for name in INTEGER_FIELDS.values()]
        + [converted[name] for name in DATE_FIELDS.values()]
        + [converted[name] for name in DECIMAL_FIELDS.values()]
        + [flags[name] for name in QUALITY_FLAGS]
    )


# Validate Bronze input schema and required fields
assert len(bronze_detail.columns) == 35, (
    "Expected 35 original Bronze columns"
)

assert len(existing_detail.columns) == 68, (
    "Expected 68 Silver columns"
)

for name in SOURCE_FIELDS:
    assert name in bronze_detail.columns, (
        f"Missing Bronze field: {name}"
    )

assert all(
    field.dataType.simpleString() == "string"
    for field in bronze_detail.schema.fields
), "Unexpected Bronze field type"

assert not (
    set(ADDED_SCHEMA.names)
    & set(bronze_detail.columns)
), "Silver field name collision"

print("BRONZE TRANSACTION DETAILS SCHEMA: PASS")


# Transform Bronze into a temporary Silver DataFrame
parsed_detail = bronze_detail.withColumn(
    "_silver_parsed",
    transform_detail_record(
        *[F.col(name) for name in SOURCE_FIELDS]
    )
)

bronze_names = set(bronze_detail.columns)

silver_detail_transformed = parsed_detail.select(
    *[
        (
            F.col(name)
            if name in bronze_names
            else F.col(f"_silver_parsed.{name}")
        ).alias(name)
        for name in existing_detail.columns
    ]
)


# Compare actual and existing Silver schemas
expected_schema = existing_detail.schema
actual_schema = silver_detail_transformed.schema

assert actual_schema.names == expected_schema.names, (
    "Silver column names or order mismatch"
)

type_mismatches = [
    (
        expected.name,
        str(expected.dataType),
        str(actual.dataType)
    )
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if expected.dataType != actual.dataType
]

metadata_differences = [
    expected.name
    for expected, actual in zip(
        expected_schema.fields,
        actual_schema.fields
    )
    if (
        expected.nullable != actual.nullable
        or expected.metadata != actual.metadata
    )
]

print("\nSCHEMA VALIDATION")
print("-" * 55)
print("Expected columns:", len(expected_schema.fields))
print("Actual columns:", len(actual_schema.fields))
print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)

if type_mismatches:
    print("Differences:", type_mismatches)

assert not type_mismatches, (
    "Transaction Details Silver data-type mismatch"
)


# Reconcile original and transformed record counts
bronze_count = bronze_detail.count()
transformed_count = silver_detail_transformed.count()
existing_count = existing_detail.count()

assert bronze_count == 2301927, (
    f"Unexpected Bronze records: {bronze_count}"
)

assert transformed_count == 2301927, (
    f"Unexpected transformed records: {transformed_count}"
)

assert existing_count == 2301927, (
    f"Unexpected existing Silver records: {existing_count}"
)


# Verify every original Bronze field is preserved
original_columns = bronze_detail.columns

original_forward = bronze_detail.select(
    *original_columns
).exceptAll(
    silver_detail_transformed.select(*original_columns)
).count()

original_reverse = silver_detail_transformed.select(
    *original_columns
).exceptAll(
    bronze_detail.select(*original_columns)
).count()

assert original_forward == 0, (
    f"Bronze preservation mismatch: {original_forward}"
)

assert original_reverse == 0, (
    f"Reverse Bronze mismatch: {original_reverse}"
)


# Compare the entire transformed Silver dataset
forward = silver_detail_transformed.exceptAll(
    existing_detail
).count()

reverse = existing_detail.exceptAll(
    silver_detail_transformed
).count()

print("\n" + "=" * 65)
print("STEP 9.9G — TRANSACTION DETAILS BRONZE TO SILVER")
print("=" * 65)

print("Bronze records:", bronze_count)
print("Silver transformed records:", transformed_count)
print("Existing Silver records:", existing_count)

print("Bronze columns:", len(original_columns))
print("Silver columns:", len(actual_schema.fields))

print("Data type mismatches:", len(type_mismatches))
print(
    "Nullability/metadata differences:",
    len(metadata_differences)
)
print(
    "Original field mismatches:",
    original_forward + original_reverse
)

print("Forward data mismatches:", forward)
print("Reverse data mismatches:", reverse)

assert forward == 0, (
    f"Forward mismatches: {forward}"
)

assert reverse == 0, (
    f"Reverse mismatches: {reverse}"
)

print("\nSTEP 9.9G SILVER TRANSACTION DETAILS TRANSFORMATION: PASS")
print("READ-ONLY - EXISTING DELTA TABLES UNCHANGED")


# COMMAND ----------


# Step 9.11B — Publish validated Assets Silver into staging Delta.
# Refuse overwrites and verify the published schema, counts, and values.
# Preserve all existing production Bronze, Silver, and Gold tables.

PRODUCTION = "workspace.assetfusion360"
STAGING = "workspace.assetfusion360_staging"

target = f"{STAGING}.silver_amps_assets"
reference = spark.table(f"{PRODUCTION}.silver_amps_assets")

# Require the verified transformation from Cell 1.
assert "silver_transformed" in globals(), (
    "Run the Silver transformation notebook first."
)

source = silver_transformed

# Confirm that this is the original, validated dataset.
assert source.count() == 148522
assert source.schema.names == reference.schema.names

assert all(
    left.dataType == right.dataType
    for left, right in zip(
        source.schema.fields,
        reference.schema.fields
    )
)

assert source.exceptAll(reference).count() == 0
assert reference.exceptAll(source).count() == 0

# Never overwrite an existing staging table.
assert not spark.catalog.tableExists(target), (
    f"Staging table already exists: {target}. "
    "No overwrite was attempted."
)

# Create a new managed Delta table in staging only.
if spark.catalog.tableExists(target):
    raise RuntimeError(
        f"Staging table already exists: {target}. "
        "Publication stopped to protect existing data."
    )

source.write.format("delta").saveAsTable(
    target
)

# Verify that the newly published Delta table matches.
published = spark.table(target)

assert published.count() == 148522
assert published.schema.names == reference.schema.names

forward = published.exceptAll(reference).count()
reverse = reference.exceptAll(published).count()

assert forward == 0 and reverse == 0, (
    f"Published data mismatches: {forward + reverse}"
)

print("=" * 65)
print("STEP 9.11B — ASSETS SILVER STAGING PUBLICATION")
print("=" * 65)
print("Source records:", source.count())
print("Published records:", published.count())
print("Published Delta table:", target)
print("Data mismatches:", forward + reverse)
print("\nSTEP 9.11B: PASS")
print("PRODUCTION DELTA TABLES: UNCHANGED")
