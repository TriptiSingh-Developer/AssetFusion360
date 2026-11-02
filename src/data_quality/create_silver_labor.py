
# Creates Silver Labor with validated work-order links, hours, rates, and costs.

from pathlib import Path
import os
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]

LABOR_SOURCE = ROOT / "data/bronze/amps_labor"
WORKORDERS_SOURCE = ROOT / "data/silver/amps_workorders"
TARGET = ROOT / "data/silver/amps_labor"

EXPECTED_BATCHES = 647
EXPECTED_ROWS = 3_230_423

labor_files = sorted(LABOR_SOURCE.glob("labor_batch_*.parquet"))
wo_files = sorted(WORKORDERS_SOURCE.glob("workorders_batch_*.parquet"))

if len(labor_files) != EXPECTED_BATCHES:
    raise ValueError("Unexpected Labor batch count.")

if len(wo_files) != 447:
    raise ValueError("Silver Work Orders are incomplete.")

print("Loading Work Order identifiers...", flush=True)

workorder_ids = set()

for file in wo_files:
    ids = pd.read_parquet(file, columns=["WorkOrderID"])
    workorder_ids.update(ids["WorkOrderID"].astype(str))

print("Unique Work Order IDs:", len(workorder_ids), flush=True)

TARGET.mkdir(parents=True, exist_ok=True)

total = 0
created = 0
verified = 0
matched = 0
unmatched = 0
missing_links = 0
missing_hours = 0
missing_costs = 0
negative_hours = 0
negative_costs = 0

for number, source_file in enumerate(labor_files, start=1):

    expected_name = f"labor_batch_{number:05d}.parquet"

    if source_file.name != expected_name:
        raise ValueError("Unexpected Labor batch filename.")

    df = pd.read_parquet(source_file)

    required = {
        "boo_code", "boo_event", "boo_date",
        "boo_hours", "boo_rate", "boo_cost"
    }

    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns: {source_file.name}")

    silver = df.copy()

    silver["LaborID"] = silver["boo_code"].astype(str).str.strip()
    silver["WorkOrderID"] = silver["boo_event"].astype(str).str.strip()

    silver["LaborDate"] = pd.to_datetime(
        silver["boo_date"].replace("", None),
        errors="coerce",
        format="mixed"
    )

    for source, target in [
        ("boo_hours", "LaborHours"),
        ("boo_rate", "LaborRate"),
        ("boo_cost", "LaborCost")
    ]:
        silver[target] = pd.to_numeric(
            silver[source].replace("", None),
            errors="coerce"
        ).astype("float64")

    silver["WorkOrderReferenceValid"] = (
        silver["WorkOrderID"].isin(workorder_ids)
    )

    silver["MissingWorkOrderReference"] = (
        silver["WorkOrderID"].eq("")
    )

    silver["LaborCostCalculationDifference"] = (
        silver["LaborCost"]
        - silver["LaborHours"] * silver["LaborRate"]
    )

    total += len(silver)
    matched += int(silver["WorkOrderReferenceValid"].sum())

    missing_links += int(
        silver["MissingWorkOrderReference"].sum()
    )

    unmatched += int((
        ~silver["WorkOrderReferenceValid"]
        & ~silver["MissingWorkOrderReference"]
    ).sum())

    missing_hours += int(silver["LaborHours"].isna().sum())
    missing_costs += int(silver["LaborCost"].isna().sum())
    negative_hours += int((silver["LaborHours"] < 0).sum())
    negative_costs += int((silver["LaborCost"] < 0).sum())

    output = TARGET / source_file.name

    if output.exists():
        saved = pd.read_parquet(output)
        assert_frame_equal(
            saved, silver,
            check_dtype=False,
            check_freq=False
        )
        verified += 1

    else:
        temp = TARGET / f"{source_file.stem}.parquet.tmp"

        if temp.exists():
            raise FileExistsError(
                f"Review existing temporary file: {temp.name}"
            )

        silver.to_parquet(temp, engine="pyarrow", index=False)

        saved = pd.read_parquet(temp)
        assert_frame_equal(
            saved, silver,
            check_dtype=False,
            check_freq=False
        )

        os.replace(temp, output)
        created += 1

    if number % 50 == 0 or number == len(labor_files):
        print(
            f"Processed {number}/{len(labor_files)} batches | "
            f"{total:,} records",
            flush=True
        )

print("\n--- SILVER LABOR ---")
print("Total records:", total)
print("New Silver files:", created)
print("Verified existing files:", verified)
print("Matched Work Order references:", matched)
print("Unmatched Work Order references:", unmatched)
print("Missing Work Order references:", missing_links)
print("Missing Labor Hours:", missing_hours)
print("Missing Labor Costs:", missing_costs)
print("Negative Labor Hours:", negative_hours)
print("Negative Labor Costs:", negative_costs)

if total != EXPECTED_ROWS:
    raise ValueError("Silver Labor record count mismatch.")

if matched + unmatched + missing_links != total:
    raise ValueError("Labor relationship totals do not reconcile.")

print("\nSILVER LABOR VALIDATION PASSED")
