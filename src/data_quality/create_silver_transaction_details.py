
# Creates consistent Silver Transaction Details and safely repairs older date-parsing results.

from pathlib import Path
import os
import json

import pandas as pd
import pyarrow.parquet as pq
from pandas.testing import assert_series_equal, assert_frame_equal


ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data/bronze/amps_transaction_details"
HEADERS = ROOT / "data/silver/amps_transactions"
WORKORDERS = ROOT / "data/gold/fact_workorders"
SILVER = ROOT / "data/silver/amps_transaction_details"
REPORT = ROOT / "docs/methodology/silver_transaction_details_audit.json"

EXPECTED_FILES = 461
EXPECTED_ROWS = 2_301_927
BATCH_SIZE = 5000

DATE_DERIVED_COLUMNS = {
    "TransactionDetailDate",
    "MissingTransactionDetailDate",
    "UnparseableTransactionDetailDate",
}

REQUIRED = {
    "trl_sqlidentity",
    "trl_trans",
    "trl_event",
    "trl_part",
    "trl_type",
    "trl_date",
    "trl_price",
    "trl_qty",
    "trl_avgprice",
}

SILVER.mkdir(parents=True, exist_ok=True)


def clean_identifier(series):
    return (
        series.astype("string")
        .str.strip()
        .replace("", pd.NA)
    )


def load_references(folder, pattern, column, expected_files):
    files = sorted(folder.glob(pattern))

    if len(files) != expected_files:
        raise ValueError(
            f"{folder}: expected {expected_files} files, "
            f"found {len(files)}"
        )

    references = set()

    for path in files:
        frame = pd.read_parquet(
            path,
            columns=[column],
        )

        values = clean_identifier(frame[column])
        references.update(values.dropna().tolist())

    return references


def parse_transaction_dates(series):
    values = clean_identifier(series)

    # Each row may use a different valid date format.
    return pd.to_datetime(
        values,
        format="mixed",
        errors="coerce",
    )


def transform_batch(bronze, transaction_ids, workorder_ids):
    missing = REQUIRED - set(bronze.columns)

    if missing:
        raise ValueError(
            f"Missing source columns: {sorted(missing)}"
        )

    silver = bronze.copy()

    for column in [
        "trl_sqlidentity",
        "trl_trans",
        "trl_event",
        "trl_part",
        "trl_type",
    ]:
        silver[column] = clean_identifier(
            silver[column]
        )

    if silver["trl_sqlidentity"].isna().any():
        raise ValueError("Missing Transaction Detail IDs")

    if silver["trl_sqlidentity"].duplicated().any():
        raise ValueError("Duplicate IDs within batch")

    for column in [
        "trl_price",
        "trl_qty",
        "trl_avgprice",
    ]:
        raw = clean_identifier(silver[column])

        numeric = pd.to_numeric(
            raw,
            errors="coerce",
        )

        invalid = raw.notna() & numeric.isna()

        if invalid.any():
            raise ValueError(
                f"Invalid numeric values in {column}"
            )

        silver[column] = numeric.astype("float64")

    silver["TransactionDetailID"] = silver["trl_sqlidentity"]
    silver["TransactionID"] = silver["trl_trans"]
    silver["WorkOrderID"] = silver["trl_event"]
    silver["PartID"] = silver["trl_part"]
    silver["TransactionDetailType"] = silver["trl_type"]

    raw_dates = clean_identifier(silver["trl_date"])

    silver["TransactionDetailDate"] = (
        parse_transaction_dates(silver["trl_date"])
    )

    silver["UnitPrice"] = silver["trl_price"]
    silver["Quantity"] = silver["trl_qty"]
    silver["AverageUnitPrice"] = silver["trl_avgprice"]

    silver["MissingTransactionReference"] = (
        silver["TransactionID"].isna()
    )

    silver["TransactionReferenceValid"] = (
        silver["TransactionID"].isin(transaction_ids)
    )

    silver["UnmatchedTransactionReference"] = (
        ~silver["MissingTransactionReference"]
        & ~silver["TransactionReferenceValid"]
    )

    silver["MissingWorkOrderReference"] = (
        silver["WorkOrderID"].isna()
    )

    silver["WorkOrderReferenceValid"] = (
        silver["WorkOrderID"].isin(workorder_ids)
    )

    silver["UnmatchedWorkOrderReference"] = (
        ~silver["MissingWorkOrderReference"]
        & ~silver["WorkOrderReferenceValid"]
    )

    silver["NegativePrice"] = silver["UnitPrice"] < 0
    silver["NegativeQuantity"] = silver["Quantity"] < 0
    silver["ZeroQuantity"] = silver["Quantity"] == 0

    silver["MissingPartReference"] = (
        silver["PartID"].isna()
    )

    silver["MissingTransactionDetailDate"] = (
        raw_dates.isna()
    )

    silver["UnparseableTransactionDetailDate"] = (
        raw_dates.notna()
        & silver["TransactionDetailDate"].isna()
    )

    # Arithmetic value only, not yet validated material expense.
    silver["SignedExtendedValue"] = (
        silver["UnitPrice"] * silver["Quantity"]
    )

    return silver


def values_equal(left, right):
    try:
        assert_series_equal(
            left.reset_index(drop=True),
            right.reset_index(drop=True),
            check_dtype=False,
            check_names=False,
            check_freq=False,
            check_exact=False,
            rtol=1e-10,
            atol=1e-10,
        )
        return True

    except AssertionError:
        return False


def validate_existing(saved, expected, filename):
    if len(saved) != len(expected):
        raise ValueError(
            f"Existing row count mismatch: {filename}"
        )

    # Do not accept unknown columns.
    extra = set(saved.columns) - set(expected.columns)

    if extra:
        raise ValueError(
            f"Unexpected columns in {filename}: {sorted(extra)}"
        )

    # Every original source column must still be present.
    source_columns = [
        col for col in expected.columns
        if col.startswith(("trl_", "par_"))
    ]

    missing_source = set(source_columns) - set(saved.columns)

    if missing_source:
        raise ValueError(
            f"Missing source columns in {filename}: "
            f"{sorted(missing_source)}"
        )

    needs_upgrade = (
        list(saved.columns) != list(expected.columns)
    )

    # Compare all previously saved fields.
    for column in saved.columns:
        equal = values_equal(
            saved[column],
            expected[column],
        )

        if equal:
            continue

        if column in DATE_DERIVED_COLUMNS:
            # Older code incorrectly parsed many valid dates as NaT.
            # The corrected values come from the original Bronze data.
            needs_upgrade = True
            continue

        raise ValueError(
            f"Existing data mismatch in {filename}, "
            f"column {column}. Existing file was not changed."
        )

    return needs_upgrade


def write_validated_parquet(frame, path):
    if path.exists():
        raise FileExistsError(
            f"Temporary file already exists: {path}"
        )

    frame.to_parquet(
        path,
        engine="pyarrow",
        index=False,
    )

    metadata = pq.read_metadata(path)

    if metadata.num_rows != len(frame):
        raise ValueError(
            f"Saved row count mismatch: {path.name}"
        )

    if metadata.schema.names != list(frame.columns):
        raise ValueError(
            f"Saved schema mismatch: {path.name}"
        )

    saved = pd.read_parquet(path)

    assert_frame_equal(
        saved,
        frame,
        check_dtype=False,
        check_freq=False,
    )


def save_batch(frame, output):
    temporary = output.with_name(
        output.name + ".tmp"
    )

    backup = output.with_name(
        output.name + ".backup"
    )

    if temporary.exists():
        raise FileExistsError(
            f"Review existing temporary file: {temporary.name}"
        )

    if not output.exists():
        write_validated_parquet(frame, temporary)
        os.replace(temporary, output)
        return "created"

    existing = pd.read_parquet(output)

    needs_upgrade = validate_existing(
        existing,
        frame,
        output.name,
    )

    if not needs_upgrade:
        return "verified"

    # Never replace an existing backup.
    if backup.exists():
        raise FileExistsError(
            f"Backup already exists: {backup.name}. "
            "Review it before proceeding."
        )

    # Validate replacement before touching the original.
    write_validated_parquet(frame, temporary)

    # Keep a backup of the previous Silver file.
    os.replace(output, backup)

    try:
        os.replace(temporary, output)
    except Exception:
        if not output.exists() and backup.exists():
            os.replace(backup, output)
        raise

    return "upgraded"


print("Loading Transaction references...", flush=True)

transaction_ids = load_references(
    HEADERS,
    "transactions_batch_*.parquet",
    "TransactionID",
    152,
)

print("Loading Work Order references...", flush=True)

workorder_ids = load_references(
    WORKORDERS,
    "fact_workorders_batch_*.parquet",
    "WorkOrderID",
    447,
)

print(
    f"Transaction references: {len(transaction_ids):,}",
    flush=True,
)

print(
    f"Work Order references: {len(workorder_ids):,}",
    flush=True,
)

files = sorted(
    BRONZE.glob("transaction_details_batch_*.parquet")
)

if len(files) != EXPECTED_FILES:
    raise ValueError(
        f"Expected {EXPECTED_FILES} Bronze batches, "
        f"found {len(files)}"
    )

total = 0
created = 0
verified = 0
upgraded = 0

matched_transactions = 0
unmatched_transactions = 0
missing_transactions = 0

matched_workorders = 0
unmatched_workorders = 0
missing_workorders = 0

negative_prices = 0
negative_quantities = 0
zero_quantities = 0

missing_parts = 0
missing_dates = 0
unparseable_dates = 0
missing_average_prices = 0

expected_columns = None

for number, source in enumerate(files, start=1):

    expected_name = (
        f"transaction_details_batch_{number:05d}.parquet"
    )

    if source.name != expected_name:
        raise ValueError(
            f"Unexpected Bronze batch: {source.name}"
        )

    bronze = pd.read_parquet(source)

    if len(bronze) == 0 or len(bronze) > BATCH_SIZE:
        raise ValueError(
            f"Invalid batch size: {source.name}"
        )

    if number < EXPECTED_FILES and len(bronze) != BATCH_SIZE:
        raise ValueError(
            f"Incomplete Bronze batch: {source.name}"
        )

    silver = transform_batch(
        bronze,
        transaction_ids,
        workorder_ids,
    )

    if expected_columns is None:
        expected_columns = list(silver.columns)

    elif list(silver.columns) != expected_columns:
        raise ValueError(
            f"Inconsistent Silver schema: {source.name}"
        )

    output = SILVER / source.name

    result = save_batch(silver, output)

    if result == "created":
        created += 1
    elif result == "verified":
        verified += 1
    elif result == "upgraded":
        upgraded += 1

    total += len(silver)

    matched_transactions += int(
        silver["TransactionReferenceValid"].sum()
    )

    unmatched_transactions += int(
        silver["UnmatchedTransactionReference"].sum()
    )

    missing_transactions += int(
        silver["MissingTransactionReference"].sum()
    )

    matched_workorders += int(
        silver["WorkOrderReferenceValid"].sum()
    )

    unmatched_workorders += int(
        silver["UnmatchedWorkOrderReference"].sum()
    )

    missing_workorders += int(
        silver["MissingWorkOrderReference"].sum()
    )

    negative_prices += int(
        silver["NegativePrice"].sum()
    )

    negative_quantities += int(
        silver["NegativeQuantity"].sum()
    )

    zero_quantities += int(
        silver["ZeroQuantity"].sum()
    )

    missing_parts += int(
        silver["MissingPartReference"].sum()
    )

    missing_dates += int(
        silver["MissingTransactionDetailDate"].sum()
    )

    unparseable_dates += int(
        silver["UnparseableTransactionDetailDate"].sum()
    )

    missing_average_prices += int(
        silver["AverageUnitPrice"].isna().sum()
    )

    if number % 50 == 0 or number == EXPECTED_FILES:
        print(
            f"Processed {number}/{EXPECTED_FILES} batches | "
            f"{total:,} records",
            flush=True,
        )


# Final reconciliation.
if total != EXPECTED_ROWS:
    raise ValueError(
        f"Expected {EXPECTED_ROWS:,} records, "
        f"found {total:,}"
    )

if created + verified + upgraded != EXPECTED_FILES:
    raise ValueError(
        "Silver batch counts do not reconcile."
    )

if (
    matched_transactions
    + unmatched_transactions
    + missing_transactions
    != total
):
    raise ValueError(
        "Transaction reference counts do not reconcile."
    )

if (
    matched_workorders
    + unmatched_workorders
    + missing_workorders
    != total
):
    raise ValueError(
        "Work Order reference counts do not reconcile."
    )

if matched_transactions != 2_300_536:
    raise ValueError(
        "Transaction match count differs from raw audit."
    )

if unmatched_transactions != 1_391:
    raise ValueError(
        "Unmatched Transaction count differs from raw audit."
    )

if missing_transactions != 0:
    raise ValueError(
        "Unexpected missing Transaction references."
    )

if negative_prices != 5_501:
    raise ValueError(
        "Negative price count differs from raw audit."
    )

if negative_quantities != 44_053:
    raise ValueError(
        "Negative quantity count differs from raw audit."
    )

report = {
    "total_records": total,
    "silver_batches": EXPECTED_FILES,
    "matched_transaction_references": matched_transactions,
    "unmatched_transaction_references": unmatched_transactions,
    "missing_transaction_references": missing_transactions,
    "matched_workorder_references": matched_workorders,
    "unmatched_workorder_references": unmatched_workorders,
    "missing_workorder_references": missing_workorders,
    "negative_prices": negative_prices,
    "negative_quantities": negative_quantities,
    "zero_quantities": zero_quantities,
    "missing_part_references": missing_parts,
    "missing_dates": missing_dates,
    "unparseable_dates": unparseable_dates,
    "missing_average_prices": missing_average_prices,
}

REPORT.parent.mkdir(
    parents=True,
    exist_ok=True,
)

if REPORT.exists():
    previous = json.loads(
        REPORT.read_text(encoding="utf-8")
    )

    if previous != report:
        raise ValueError(
            "Existing audit report differs. "
            "Review before replacing it."
        )

else:
    REPORT.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

print("\n--- SILVER TRANSACTION DETAILS ---")
print("Total records:", total)
print("New Silver files:", created)
print("Verified existing files:", verified)
print("Upgraded existing files:", upgraded)

print("Matched transaction references:", matched_transactions)
print("Unmatched transaction references:", unmatched_transactions)
print("Missing transaction references:", missing_transactions)

print("Matched Work Order references:", matched_workorders)
print("Unmatched Work Order references:", unmatched_workorders)
print("Missing Work Order references:", missing_workorders)

print("Negative prices:", negative_prices)
print("Negative quantities:", negative_quantities)
print("Zero quantities:", zero_quantities)

print("Missing part references:", missing_parts)
print("Missing dates:", missing_dates)
print("Unparseable dates:", unparseable_dates)
print("Missing average prices:", missing_average_prices)

print("Output:", SILVER)

print("\nSILVER TRANSACTION DETAILS STRUCTURAL VALIDATION PASSED")
