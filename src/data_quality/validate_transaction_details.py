
# Audits AMPS Transaction Details IDs, references, prices, and quantities.

from pathlib import Path
import csv
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/nyc_amps/transaction_details_batches"
TRANSACTIONS = ROOT / "data/raw/nyc_amps/transactions_batches"
REPORT = ROOT / "docs/methodology/transaction_details_audit.json"

EXPECTED_FILES = 461
EXPECTED_ROWS = 2_301_927
BATCH_SIZE = 5000

files = sorted(SOURCE.glob("transaction_details_batch_*.csv"))

if len(files) != EXPECTED_FILES:
    raise ValueError(
        f"Expected {EXPECTED_FILES} files, found {len(files)}"
    )

with files[0].open("r", encoding="utf-8-sig", newline="") as f:
    columns = next(csv.reader(f))

print("\n--- TRANSACTION DETAILS SCHEMA ---")
print("Columns:", len(columns))
for column in columns:
    print(" ", column)

if len(columns) != 31:
    raise ValueError("Unexpected Transaction Details schema.")

# Source field names are retained exactly as published.
wanted = [
    "trl_sqlidentity",
    "trl_code",
    "trl_trans",
    "trl_event",
    "trl_part",
    "trl_price",
    "trl_qty",
]

available = [c for c in wanted if c in columns]

# Transaction header codes provide a reference lookup.
header_files = sorted(
    TRANSACTIONS.glob("transactions_batch_*.csv")
)

if len(header_files) != 152:
    raise ValueError("Transaction header batches are incomplete.")

header_ids = set()

for path in header_files:
    part = pd.read_csv(
        path,
        usecols=["tra_code"],
        dtype="string",
    )
    header_ids.update(
        part["tra_code"].dropna().str.strip().tolist()
    )

print("\nTransaction header references loaded:", len(header_ids))

total = 0
duplicate_ids = 0
missing_ids = 0
matched_transactions = 0
unmatched_transactions = 0
missing_transactions = 0
missing_prices = 0
negative_prices = 0
invalid_prices = 0
missing_quantities = 0
negative_quantities = 0
invalid_quantities = 0

seen_ids = set()
id_field = next(
    (c for c in ["trl_sqlidentity", "trl_code"] if c in columns),
    None,
)

for number, path in enumerate(files, start=1):

    expected = f"transaction_details_batch_{number:05d}.csv"
    if path.name != expected:
        raise ValueError(f"Unexpected batch: {path.name}")

    df = pd.read_csv(
        path,
        dtype="string",
        keep_default_na=False,
    )

    if list(df.columns) != columns:
        raise ValueError(f"Schema mismatch: {path.name}")

    if len(df) == 0 or len(df) > BATCH_SIZE:
        raise ValueError(f"Invalid batch size: {path.name}")

    if number < EXPECTED_FILES and len(df) != BATCH_SIZE:
        raise ValueError(f"Incomplete batch: {path.name}")

    total += len(df)

    if id_field:
        ids = df[id_field].str.strip()
        missing = ids.eq("")
        missing_ids += int(missing.sum())

        valid_ids = ids[~missing]

        duplicate_ids += int(valid_ids.duplicated().sum())

        unique_ids = set(valid_ids.tolist())
        duplicate_ids += len(unique_ids & seen_ids)
        seen_ids.update(unique_ids)

    if "trl_trans" in df.columns:
        refs = df["trl_trans"].str.strip()
        missing = refs.eq("")
        valid = refs[~missing]

        missing_transactions += int(missing.sum())
        matched_transactions += int(
            valid.isin(header_ids).sum()
        )
        unmatched_transactions += int(
            (~valid.isin(header_ids)).sum()
        )

    if "trl_price" in df.columns:
        raw = df["trl_price"].str.strip()
        numeric = pd.to_numeric(
            raw.replace("", pd.NA), errors="coerce"
        )

        missing_prices += int(raw.eq("").sum())
        invalid_prices += int(
            raw.ne("").sum() - numeric.notna().sum()
        )
        negative_prices += int((numeric < 0).sum())

    if "trl_qty" in df.columns:
        raw = df["trl_qty"].str.strip()
        numeric = pd.to_numeric(
            raw.replace("", pd.NA), errors="coerce"
        )

        missing_quantities += int(raw.eq("").sum())
        invalid_quantities += int(
            raw.ne("").sum() - numeric.notna().sum()
        )
        negative_quantities += int((numeric < 0).sum())

    if number % 50 == 0 or number == len(files):
        print(
            f"Audited {number}/{len(files)} batches | "
            f"{total:,} rows",
            flush=True,
        )

if total != EXPECTED_ROWS:
    raise ValueError(
        f"Expected {EXPECTED_ROWS:,} rows, found {total:,}"
    )

report = {
    "dataset": "NYC Parks AMPS Transaction Details",
    "total_rows": total,
    "csv_files": len(files),
    "columns": columns,
    "identifier_field": id_field,
    "missing_identifiers": missing_ids if id_field else None,
    "duplicate_identifiers": duplicate_ids if id_field else None,
    "matched_transaction_references": matched_transactions,
    "unmatched_transaction_references": unmatched_transactions,
    "missing_transaction_references": missing_transactions,
    "missing_prices": missing_prices,
    "invalid_prices": invalid_prices,
    "negative_prices": negative_prices,
    "missing_quantities": missing_quantities,
    "invalid_quantities": invalid_quantities,
    "negative_quantities": negative_quantities,
}

REPORT.parent.mkdir(parents=True, exist_ok=True)

if REPORT.exists():
    existing = json.loads(REPORT.read_text(encoding="utf-8"))
    if existing != report:
        raise ValueError(
            "Existing audit differs; review before replacing."
        )
else:
    REPORT.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

print("\n--- TRANSACTION DETAILS AUDIT ---")
for key, value in report.items():
    if key != "columns":
        print(f"{key}: {value}")

print("\nTRANSACTION DETAILS STRUCTURAL AUDIT COMPLETED")
