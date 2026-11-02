
# Adds inspection coverage and latest condition ratings to the asset analytics model.

from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data/gold"

SOURCE = GOLD / "fact_inspections.parquet"
ASSETS = GOLD / "asset_labor_summary.parquet"
OUTPUT = GOLD / "asset_inspection_summary.parquet"

assets = pd.read_parquet(ASSETS)
inspections = pd.read_parquet(SOURCE)

if len(assets) != 148_521 or assets["AssetID"].duplicated().any():
    raise ValueError("Invalid asset summary.")

if len(inspections) != 153_093:
    raise ValueError("Unexpected inspection count.")

matched = inspections.loc[
    inspections["AssetReferenceValid"]
].copy()

if len(matched) != 148_235:
    raise ValueError("Inspection relationship count mismatch.")

matched["InspectionDate"] = pd.to_datetime(
    matched["InspectionDate"], errors="coerce"
)

matched["InspectionIDNumber"] = pd.to_numeric(
    matched["InspectionID"], errors="coerce"
)

counts = (
    matched.groupby("AssetID")
    .agg(
        InspectionCount=("InspectionID", "size"),
        FirstInspectionDate=("InspectionDate", "min"),
        LastInspectionDate=("InspectionDate", "max"),
        OverallRatingAvailable=("HasOverallCondition", "sum"),
        SafetyRatingAvailable=("HasSafetyCondition", "sum"),
        StructuralRatingAvailable=("HasStructuralCondition", "sum"),
    )
    .reset_index()
)

latest = (
    matched.sort_values(
        ["InspectionDate", "InspectionIDNumber"],
        kind="stable",
    )
    .drop_duplicates("AssetID", keep="last")
    [[
        "AssetID",
        "OverallCondition",
        "CleanlinessCondition",
        "SafetyCondition",
        "StructuralCondition",
    ]]
    .rename(columns={
        "OverallCondition": "LatestOverallCondition",
        "CleanlinessCondition": "LatestCleanlinessCondition",
        "SafetyCondition": "LatestSafetyCondition",
        "StructuralCondition": "LatestStructuralCondition",
    })
)

result = assets.merge(
    counts, on="AssetID", how="left", validate="one_to_one"
).merge(
    latest, on="AssetID", how="left", validate="one_to_one"
)

result["InspectionCount"] = (
    result["InspectionCount"].fillna(0).astype("int64")
)

for col in [
    "OverallRatingAvailable",
    "SafetyRatingAvailable",
    "StructuralRatingAvailable",
]:
    result[col] = result[col].fillna(0).astype("int64")

result["HasInspectionHistory"] = result["InspectionCount"].gt(0)

if len(result) != 148_521:
    raise ValueError("Asset count changed.")

if result["InspectionCount"].sum() != 148_235:
    raise ValueError("Inspection totals do not reconcile.")

if OUTPUT.exists():
    assert_frame_equal(
        pd.read_parquet(OUTPUT),
        result,
        check_dtype=False,
        check_freq=False,
    )
    print("Existing asset inspection summary verified.")
else:
    temp = OUTPUT.with_name("asset_inspection_summary.parquet.tmp")
    if temp.exists():
        raise FileExistsError("Review existing temporary output.")
    result.to_parquet(temp, index=False)
    assert_frame_equal(
        pd.read_parquet(temp),
        result,
        check_dtype=False,
        check_freq=False,
    )
    temp.rename(OUTPUT)
    print("Asset inspection summary created.")

print("\n--- GOLD ASSET INSPECTION SUMMARY ---")
print("Total assets:", len(result))
print("Assets with inspection history:", int(result["HasInspectionHistory"].sum()))
print("Matched inspections:", int(result["InspectionCount"].sum()))
print("Unmatched inspections retained in fact table:", len(inspections) - len(matched))
print("Output:", OUTPUT)
print("\nGOLD ASSET INSPECTION SUMMARY PASSED")
