
# Creates Gold Transactions with validated Work Order links and transaction classifications.

from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data/silver/amps_transactions"
TARGET = ROOT / "data/gold/fact_transactions"

EXPECTED_BATCHES = 152
EXPECTED_ROWS = 755_122

files = sorted(SOURCE.glob("transactions_batch_*.parquet"))

if len(files) != EXPECTED_BATCHES:
    raise ValueError("Unexpected Silver Transactions batch count.")

TARGET.mkdir(parents=True, exist_ok=True)

total = 0
linked = 0
unmatched = 0
missing_links = 0
non_event = 0
created = 0
verified = 0

for number, source_file in enumerate(files, start=1):
    if source_file.name != f"transactions_batch_{number:05d}.parquet":
        raise ValueError(f"Unexpected batch: {source_file.name}")

    df = pd.read_parquet(source_file)

    required = {
        "TransactionID",
        "WorkOrderID",
        "TransactionDate",
        "DestinationType",
        "IsWorkOrderTransaction",
        "WorkOrderReferenceValid",
        "UnmatchedWorkOrderReference",
        "MissingWorkOrderReference",
        "tra_type",
        "tra_desc",
    }

    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns: {source_file.name}")

    gold = df[
        [
            "TransactionID",
            "WorkOrderID",
            "TransactionDate",
            "DestinationType",
            "tra_type",
            "tra_desc",
            "IsWorkOrderTransaction",
            "WorkOrderReferenceValid",
            "UnmatchedWorkOrderReference",
            "MissingWorkOrderReference",
        ]
    ].copy()

    gold = gold.rename(columns={
        "tra_type": "TransactionType",
        "tra_desc": "TransactionDescription",
    })

    gold["TransactionType"] = (
        gold["TransactionType"]
        .fillna("")
        .astype("string")
        .str.strip()
        .str.upper()
    )

    gold["IsNonWorkOrderTransaction"] = (
        ~gold["IsWorkOrderTransaction"]
    )

    total += len(gold)
    linked += int(gold["WorkOrderReferenceValid"].sum())
    unmatched += int(gold["UnmatchedWorkOrderReference"].sum())
    missing_links += int(gold["MissingWorkOrderReference"].sum())
    non_event += int(gold["IsNonWorkOrderTransaction"].sum())

    output = TARGET / f"fact_{source_file.name}"

    if output.exists():
        metadata = pq.read_metadata(output)

        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(f"Existing output mismatch: {output.name}")

        verified += 1

    else:
        temp = TARGET / f"fact_{source_file.stem}.parquet.tmp"

        if temp.exists():
            raise FileExistsError(
                f"Temporary output needs review: {temp.name}"
            )

        gold.to_parquet(temp, engine="pyarrow", index=False)

        metadata = pq.read_metadata(temp)

        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(f"Output validation failed: {output.name}")

        os.replace(temp, output)
        created += 1

    if number % 25 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total:,} records",
            flush=True
        )

print("\n--- GOLD FACT TRANSACTIONS ---")
print("Total records:", total)
print("New Gold files:", created)
print("Verified existing files:", verified)
print("Matched Work Order transactions:", linked)
print("Unmatched Work Order transactions:", unmatched)
print("Missing Work Order references:", missing_links)
print("Non-Work-Order transactions:", non_event)

if total != EXPECTED_ROWS:
    raise ValueError("Transaction record count mismatch.")

if linked + unmatched + missing_links + non_event != total:
    raise ValueError("Transaction classification does not reconcile.")

print("\nGOLD FACT TRANSACTIONS STRUCTURAL VALIDATION PASSED")
