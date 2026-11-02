
# Creates Gold Labor fact files with work hours, costs, and relationship flags.

from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/silver/amps_labor"
TARGET = ROOT / "data/gold/fact_labor"

EXPECTED_BATCHES = 647
EXPECTED_ROWS = 3_230_423

files = sorted(SOURCE.glob("labor_batch_*.parquet"))

if len(files) != EXPECTED_BATCHES:
    raise ValueError("Unexpected Silver Labor batch count.")

TARGET.mkdir(parents=True, exist_ok=True)

total = 0
matched = 0
unmatched = 0
missing_links = 0
missing_costs = 0
negative_costs = 0
created = 0
verified = 0

for number, source_file in enumerate(files, start=1):
    if source_file.name != f"labor_batch_{number:05d}.parquet":
        raise ValueError(f"Unexpected file: {source_file.name}")

    df = pd.read_parquet(source_file)

    required = {
        "LaborID",
        "WorkOrderID",
        "LaborDate",
        "LaborHours",
        "LaborRate",
        "LaborCost",
        "WorkOrderReferenceValid",
        "MissingWorkOrderReference",
    }

    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns: {source_file.name}")

    gold = df[
        [
            "LaborID",
            "WorkOrderID",
            "LaborDate",
            "LaborHours",
            "LaborRate",
            "LaborCost",
            "WorkOrderReferenceValid",
            "MissingWorkOrderReference",
        ]
    ].copy()

    gold["UnmatchedWorkOrderReference"] = (
        ~gold["WorkOrderReferenceValid"]
        & ~gold["MissingWorkOrderReference"]
    )

    gold["LaborCostDifference"] = (
        gold["LaborCost"]
        - gold["LaborHours"] * gold["LaborRate"]
    )

    total += len(gold)
    matched += int(gold["WorkOrderReferenceValid"].sum())
    missing_links += int(gold["MissingWorkOrderReference"].sum())
    unmatched += int(gold["UnmatchedWorkOrderReference"].sum())
    missing_costs += int(gold["LaborCost"].isna().sum())
    negative_costs += int((gold["LaborCost"] < 0).sum())

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
                f"Temporary file needs review: {temp.name}"
            )

        gold.to_parquet(temp, engine="pyarrow", index=False)

        metadata = pq.read_metadata(temp)
        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(f"Output verification failed: {output.name}")

        os.replace(temp, output)
        created += 1

    if number % 100 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total:,} records",
            flush=True,
        )

print("\n--- GOLD FACT LABOR ---")
print("Total Labor records:", total)
print("New Gold files:", created)
print("Verified existing files:", verified)
print("Matched Work Order references:", matched)
print("Unmatched Work Order references:", unmatched)
print("Missing Work Order references:", missing_links)
print("Missing Labor costs:", missing_costs)
print("Negative Labor costs:", negative_costs)

if total != EXPECTED_ROWS:
    raise ValueError("Gold Labor record count mismatch.")

if matched + unmatched + missing_links != total:
    raise ValueError("Labor relationship totals do not reconcile.")

if created + verified != EXPECTED_BATCHES:
    raise ValueError("Gold Labor file count mismatch.")

print("\nGOLD FACT LABOR STRUCTURAL VALIDATION PASSED")
