
# Aggregates matched Labor hours and signed costs by asset.

from pathlib import Path
import os
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data/gold"

WORKORDERS = GOLD / "fact_workorders"
LABOR = GOLD / "fact_labor"
ASSET_SUMMARY = GOLD / "asset_maintenance_summary.parquet"
OUTPUT = GOLD / "asset_labor_summary.parquet"

wo_files = sorted(WORKORDERS.glob("fact_workorders_batch_*.parquet"))
labor_files = sorted(LABOR.glob("fact_labor_batch_*.parquet"))

if len(wo_files) != 447 or len(labor_files) != 647:
    raise ValueError("Gold Work Orders or Labor files incomplete.")

print("Loading Work Order-to-Asset relationships...", flush=True)

wo_parts = [
    pd.read_parquet(p, columns=["WorkOrderID", "AssetID"])
    for p in wo_files
]
wo = pd.concat(wo_parts, ignore_index=True)
del wo_parts

if len(wo) != 2_231_535 or wo["WorkOrderID"].duplicated().any():
    raise ValueError("Invalid Work Order lookup.")

lookup = wo.set_index("WorkOrderID")["AssetID"]
del wo

totals = []
labor_rows = 0
matched_rows = 0
unmatched_rows = 0
negative_cost_rows = 0

for number, path in enumerate(labor_files, 1):
    df = pd.read_parquet(
        path,
        columns=[
            "WorkOrderID",
            "LaborHours",
            "LaborCost",
            "WorkOrderReferenceValid",
        ],
    )

    labor_rows += len(df)
    matched_rows += int(df["WorkOrderReferenceValid"].sum())
    unmatched_rows += int((~df["WorkOrderReferenceValid"]).sum())
    negative_cost_rows += int((df["LaborCost"] < 0).sum())

    valid = df.loc[df["WorkOrderReferenceValid"]].copy()
    valid["AssetID"] = valid["WorkOrderID"].map(lookup)

    if valid["AssetID"].isna().any():
        raise ValueError(f"Asset lookup failed: {path.name}")

    valid["NegativeCostAdjustment"] = valid["LaborCost"].where(
        valid["LaborCost"] < 0, 0
    )

    grouped = valid.groupby("AssetID").agg(
        LaborEntries=("WorkOrderID", "size"),
        TotalLaborHours=("LaborHours", "sum"),
        NetLaborCost=("LaborCost", "sum"),
        NegativeCostEntries=("NegativeCostAdjustment", "size"),
        NegativeCostTotal=("NegativeCostAdjustment", "sum"),
    )

    # Replace the placeholder count with an actual negative-cost count.
    grouped["NegativeCostEntries"] = (
        valid["LaborCost"].lt(0).groupby(valid["AssetID"]).sum()
    )

    totals.append(grouped)

    if number % 100 == 0 or number == len(labor_files):
        print(f"Processed {number}/{len(labor_files)} batches", flush=True)

labor_by_asset = (
    pd.concat(totals)
    .groupby(level=0)
    .sum()
    .reset_index()
)

assets = pd.read_parquet(ASSET_SUMMARY)

result = assets.merge(
    labor_by_asset,
    on="AssetID",
    how="left",
    validate="one_to_one",
)

count_cols = ["LaborEntries", "NegativeCostEntries"]
amount_cols = [
    "TotalLaborHours",
    "NetLaborCost",
    "NegativeCostTotal",
]

result[count_cols] = result[count_cols].fillna(0).astype("int64")
result[amount_cols] = result[amount_cols].fillna(0.0)

if len(result) != 148_521:
    raise ValueError("Asset count changed.")

if labor_rows != 3_230_423:
    raise ValueError("Labor record count mismatch.")

if matched_rows + unmatched_rows != labor_rows:
    raise ValueError("Labor relationship count mismatch.")

if matched_rows != 3_217_345 or unmatched_rows != 13_078:
    raise ValueError("Unexpected Labor relationship totals.")

if negative_cost_rows != 71_374:
    raise ValueError("Negative Labor cost count mismatch.")

if result["LaborEntries"].sum() != matched_rows:
    raise ValueError("Asset labor entries do not reconcile.")

if result["NegativeCostEntries"].sum() > negative_cost_rows:
    raise ValueError("Invalid negative cost reconciliation.")

if OUTPUT.exists():
    saved = pd.read_parquet(OUTPUT)
    assert_frame_equal(saved, result, check_dtype=False)
    print("Existing asset labor summary verified.")
else:
    temp = OUTPUT.with_name("asset_labor_summary.parquet.tmp")
    if temp.exists():
        raise FileExistsError("Review existing temporary file.")
    result.to_parquet(temp, index=False)
    assert_frame_equal(
        pd.read_parquet(temp), result, check_dtype=False
    )
    os.replace(temp, OUTPUT)
    print("Asset labor summary created.")

print("\n--- GOLD ASSET LABOR SUMMARY ---")
print("Assets:", len(result))
print("Matched Labor entries:", matched_rows)
print("Unmatched Labor entries:", unmatched_rows)
print("Negative cost entries, all Labor:", negative_cost_rows)
print("Assets with Labor:", int((result["LaborEntries"] > 0).sum()))
print("Matched Labor hours:", round(result["TotalLaborHours"].sum(), 2))
print("Matched net Labor cost:", round(result["NetLaborCost"].sum(), 2))
print("Output:", OUTPUT)

print("\nGOLD ASSET LABOR SUMMARY PASSED")
