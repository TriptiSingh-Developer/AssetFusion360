
# Creates asset-level maintenance KPIs from Gold Work Orders.

from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data/gold"

ASSETS = GOLD / "dim_asset.parquet"
WORKORDERS = GOLD / "fact_workorders"
OUTPUT = GOLD / "asset_maintenance_summary.parquet"

assets = pd.read_parquet(ASSETS)
files = sorted(WORKORDERS.glob("fact_workorders_batch_*.parquet"))

if len(files) != 447:
    raise ValueError("Expected 447 Gold Work Order files.")

if assets["AssetID"].duplicated().any():
    raise ValueError("Duplicate AssetIDs in dimension.")

aggregates = []
total_workorders = 0

for number, file in enumerate(files, 1):
    df = pd.read_parquet(
        file,
        columns=[
            "AssetID",
            "IsCompleted",
            "CompletionHours",
            "ObjectTypeMismatch",
        ],
    )

    df["ValidCompletion"] = df["CompletionHours"].notna()
    df["DurationForSum"] = df["CompletionHours"].fillna(0)

    grouped = df.groupby("AssetID", dropna=False).agg(
        WorkOrderCount=("AssetID", "size"),
        CompletedWorkOrders=("IsCompleted", "sum"),
        ValidDurationCount=("ValidCompletion", "sum"),
        TotalCompletionHours=("DurationForSum", "sum"),
        ObjectTypeMismatchCount=("ObjectTypeMismatch", "sum"),
    )

    aggregates.append(grouped)
    total_workorders += len(df)

    if number % 100 == 0 or number == len(files):
        print(f"Processed {number}/{len(files)} files", flush=True)

combined = pd.concat(aggregates)

summary = combined.groupby(level=0).sum()

summary["AverageCompletionHours"] = (
    summary["TotalCompletionHours"]
    / summary["ValidDurationCount"].replace(0, float("nan"))
)

summary["CompletionDateCoveragePct"] = (
    summary["CompletedWorkOrders"]
    / summary["WorkOrderCount"] * 100
)

summary = summary.reset_index()

result = assets.merge(
    summary,
    on="AssetID",
    how="left",
    validate="one_to_one",
)

count_columns = [
    "WorkOrderCount",
    "CompletedWorkOrders",
    "ValidDurationCount",
    "ObjectTypeMismatchCount",
]

result[count_columns] = result[count_columns].fillna(0).astype("int64")

result = result.sort_values("AssetID").reset_index(drop=True)

if len(result) != 148_521:
    raise ValueError("Asset dimension row count changed.")

if total_workorders != 2_231_535:
    raise ValueError("Work Order count mismatch.")

if result["WorkOrderCount"].sum() != total_workorders:
    raise ValueError("Asset-level Work Orders do not reconcile.")

if result["ObjectTypeMismatchCount"].sum() != 937:
    raise ValueError("Object-type discrepancy count mismatch.")

if OUTPUT.exists():
    existing = pd.read_parquet(OUTPUT)
    assert_frame_equal(existing, result, check_dtype=False)
    print("Existing asset maintenance summary verified.")
else:
    temp = OUTPUT.with_name("asset_maintenance_summary.parquet.tmp")
    if temp.exists():
        raise FileExistsError("Review existing temporary output.")
    result.to_parquet(temp, index=False)
    assert_frame_equal(
        pd.read_parquet(temp), result, check_dtype=False
    )
    temp.rename(OUTPUT)
    print("Asset maintenance summary created.")

print("\n--- GOLD ASSET MAINTENANCE SUMMARY ---")
print("Total assets:", len(result))
print("Work Orders reconciled:", total_workorders)
print("Assets with Work Orders:", int((result["WorkOrderCount"] > 0).sum()))
print("Object-type mismatches:", int(result["ObjectTypeMismatchCount"].sum()))

print("\nTop 10 assets by Work Order count:")
print(
    result[
        ["AssetID", "AssetName", "WorkOrderCount"]
    ]
    .sort_values("WorkOrderCount", ascending=False)
    .head(10)
    .to_string(index=False)
)

print("\nGOLD ASSET MAINTENANCE SUMMARY PASSED")
