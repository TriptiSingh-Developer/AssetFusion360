
# Creates Silver Parks Inspections with cleaned fields and validated asset links.

from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/parks_inspections.parquet"
ASSETS = ROOT / "data/silver/amps_assets.parquet"
TARGET = ROOT / "data/silver/parks_inspections.parquet"

EXPECTED_ROWS = 153_093

inspections = pd.read_parquet(SOURCE)
assets = pd.read_parquet(ASSETS, columns=["AssetID", "OBJ_OBTYPE"])

required = {
    "Inspection ID",
    "Prop ID",
    "Date",
    "Overall Condition",
    "Cleanliness",
    "Safety Condition",
    "Structural Condition",
    "Inspection Year",
}

if not required.issubset(inspections.columns):
    raise ValueError("Required inspection columns missing.")

if assets["AssetID"].duplicated().any():
    raise ValueError("Silver Assets contains duplicate IDs.")

silver = inspections.copy()

silver["InspectionID"] = (
    silver["Inspection ID"].fillna("").astype(str).str.strip()
)

silver["AssetID"] = (
    silver["Prop ID"]
    .fillna("")
    .astype(str)
    .str.strip()
    .str.upper()
)

silver["InspectionDate"] = pd.to_datetime(
    silver["Date"].replace("", pd.NA),
    errors="coerce",
    format="mixed",
)

silver["InspectionYear"] = pd.to_numeric(
    silver["Inspection Year"].replace("", pd.NA),
    errors="coerce",
).astype("Int64")

condition_columns = {
    "Overall Condition": "OverallCondition",
    "Cleanliness": "CleanlinessCondition",
    "Safety Condition": "SafetyCondition",
    "Structural Condition": "StructuralCondition",
}

for original, cleaned in condition_columns.items():
    silver[cleaned] = (
        silver[original]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .astype("string")
    )

asset_ids = set(assets["AssetID"])

silver["AssetReferenceValid"] = (
    silver["AssetID"].isin(asset_ids)
)

silver["MissingAssetReference"] = (
    silver["AssetID"].eq("")
)

silver["UnmatchedAssetReference"] = (
    ~silver["AssetReferenceValid"]
    & ~silver["MissingAssetReference"]
)

total = len(silver)
matched = int(silver["AssetReferenceValid"].sum())
unmatched = int(silver["UnmatchedAssetReference"].sum())
missing_asset = int(silver["MissingAssetReference"].sum())
missing_dates = int(silver["InspectionDate"].isna().sum())
missing_ids = int(silver["InspectionID"].eq("").sum())
duplicate_ids = int(silver["InspectionID"].duplicated().sum())

if total != EXPECTED_ROWS:
    raise ValueError("Inspection record count mismatch.")

if missing_ids or duplicate_ids:
    raise ValueError("Missing or duplicate Inspection IDs.")

if matched + unmatched + missing_asset != total:
    raise ValueError("Asset relationship counts do not reconcile.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    metadata = pq.read_metadata(TARGET)

    if (
        metadata.num_rows != total
        or metadata.schema.names != list(silver.columns)
    ):
        raise ValueError("Existing Silver Inspections mismatch.")

    print("Existing Silver Inspections structurally verified.")

else:
    temporary = TARGET.with_name(
        "parks_inspections.parquet.tmp"
    )

    if temporary.exists():
        raise FileExistsError(
            "Temporary output exists. Review before rerunning."
        )

    silver.to_parquet(
        temporary,
        engine="pyarrow",
        index=False,
    )

    metadata = pq.read_metadata(temporary)

    if (
        metadata.num_rows != total
        or metadata.schema.names != list(silver.columns)
    ):
        raise ValueError("Silver Inspections verification failed.")

    os.replace(temporary, TARGET)
    print("Silver Inspections created.")

print("\n--- SILVER PARKS INSPECTIONS ---")
print("Total records:", total)
print("Unique Inspection IDs:", silver["InspectionID"].nunique())
print("Matched asset references:", matched)
print("Unmatched asset references:", unmatched)
print("Missing asset references:", missing_asset)
print("Missing inspection dates:", missing_dates)
print("Asset match rate (%):", round(matched / total * 100, 2))

print("\nSILVER INSPECTIONS STRUCTURAL VALIDATION PASSED")
