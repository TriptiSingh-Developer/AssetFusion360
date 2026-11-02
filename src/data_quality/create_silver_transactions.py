
# Creates Silver Transactions and validates Work Order references.

from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/amps_transactions"
WORKORDERS = ROOT / "data/silver/amps_workorders"
TARGET = ROOT / "data/silver/amps_transactions"

EXPECTED_ROWS = 755_122
EXPECTED_BATCHES = 152

files = sorted(SOURCE.glob("transactions_batch_*.parquet"))
wo_files = sorted(WORKORDERS.glob("workorders_batch_*.parquet"))

if len(files) != EXPECTED_BATCHES or len(wo_files) != 447:
    raise ValueError("Bronze Transactions or Silver Work Orders incomplete.")

TARGET.mkdir(parents=True, exist_ok=True)

print("Loading Work Order identifiers...", flush=True)

wo_ids = set()

for path in wo_files:
    frame = pd.read_parquet(path, columns=["WorkOrderID"])
    wo_ids.update(frame["WorkOrderID"].astype(str))

print("Unique Work Orders:", len(wo_ids), flush=True)

total = 0
linked = 0
unmatched = 0
non_event = 0
missing_event_ids = 0
created = 0
verified = 0

for number, path in enumerate(files, start=1):
    if path.name != f"transactions_batch_{number:05d}.parquet":
        raise ValueError(f"Unexpected filename: {path.name}")

    df = pd.read_parquet(path)

    required = {
        "tra_sqlidentity", "tra_code", "tra_date",
        "tra_type", "tra_toentity", "tra_tocode"
    }

    if not required.issubset(df.columns):
        raise ValueError(f"Required columns missing: {path.name}")

    silver = df.copy()

    silver["TransactionID"] = (
        silver["tra_sqlidentity"].astype(str).str.strip()
    )

    silver["DestinationType"] = (
        silver["tra_toentity"].fillna("")
        .astype(str).str.strip().str.upper()
    )

    silver["WorkOrderID"] = (
        silver["tra_tocode"].fillna("")
        .astype(str).str.strip()
    ).astype("string")

    silver["TransactionDate"] = pd.to_datetime(
        silver["tra_date"].replace("", None),
        errors="coerce",
        format="mixed"
    )

    silver["IsWorkOrderTransaction"] = (
        silver["DestinationType"] == "EVNT"
    )

    silver["WorkOrderReferenceValid"] = (
        silver["IsWorkOrderTransaction"]
        & silver["WorkOrderID"].isin(wo_ids)
    )

    silver["UnmatchedWorkOrderReference"] = (
        silver["IsWorkOrderTransaction"]
        & silver["WorkOrderID"].ne("")
        & ~silver["WorkOrderReferenceValid"]
    )

    silver["MissingWorkOrderReference"] = (
        silver["IsWorkOrderTransaction"]
        & silver["WorkOrderID"].eq("")
    )

    total += len(silver)
    linked += int(silver["WorkOrderReferenceValid"].sum())
    unmatched += int(silver["UnmatchedWorkOrderReference"].sum())
    missing_event_ids += int(silver["MissingWorkOrderReference"].sum())
    non_event += int((~silver["IsWorkOrderTransaction"]).sum())

    output = TARGET / path.name

    if output.exists():
        metadata = pq.read_metadata(output)
        if (
            metadata.num_rows != len(silver)
            or metadata.schema.names != list(silver.columns)
        ):
            raise ValueError(f"Existing output mismatch: {output.name}")
        verified += 1

    else:
        temp = TARGET / f"{path.stem}.parquet.tmp"

        if temp.exists():
            raise FileExistsError(
                f"Review temporary file before continuing: {temp.name}"
            )

        silver.to_parquet(temp, engine="pyarrow", index=False)

        metadata = pq.read_metadata(temp)
        if (
            metadata.num_rows != len(silver)
            or metadata.schema.names != list(silver.columns)
        ):
            raise ValueError(f"Output verification failed: {path.name}")

        os.replace(temp, output)
        created += 1

    if number % 25 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total:,} records",
            flush=True
        )

print("\n--- SILVER TRANSACTIONS ---")
print("Total records:", total)
print("New Silver files:", created)
print("Verified existing files:", verified)
print("Matched Work Order transactions:", linked)
print("Unmatched Work Order transactions:", unmatched)
print("Missing Work Order references:", missing_event_ids)
print("Non-Work-Order transactions:", non_event)

if total != EXPECTED_ROWS:
    raise ValueError("Transaction record count mismatch.")

if linked + unmatched + missing_event_ids + non_event != total:
    raise ValueError("Transaction classification totals do not reconcile.")

print("\nSILVER TRANSACTIONS STRUCTURAL VALIDATION PASSED")
