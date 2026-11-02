
# Creates the Gold Parks Inspections fact table from validated Silver data.

from pathlib import Path
import os

import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/silver/parks_inspections.parquet"
TARGET = ROOT / "data/gold/fact_inspections.parquet"

EXPECTED_ROWS = 153_093
EXPECTED_MATCHED = 148_235
EXPECTED_UNMATCHED = 4_858

silver = pd.read_parquet(SOURCE)

required = {
    "InspectionID",
    "AssetID",
    "InspectionDate",
    "InspectionYear",
    "OverallCondition",
    "CleanlinessCondition",
    "SafetyCondition",
    "StructuralCondition",
    "AssetReferenceValid",
    "MissingAssetReference",
    "UnmatchedAssetReference",
}

if not required.issubset(silver.columns):
    raise ValueError(
        f"Missing columns: {sorted(required - set(silver.columns))}"
    )

gold = silver[
    [
        "InspectionID",
        "AssetID",
        "InspectionDate",
        "InspectionYear",
        "OverallCondition",
        "CleanlinessCondition",
        "SafetyCondition",
        "StructuralCondition",
        "AssetReferenceValid",
        "MissingAssetReference",
        "UnmatchedAssetReference",
    ]
].copy()

gold["InspectionID"] = (
    gold["InspectionID"].astype("string").str.strip()
)

gold["AssetID"] = (
    gold["AssetID"].astype("string").str.strip().str.upper()
)

gold["InspectionDate"] = pd.to_datetime(
    gold["InspectionDate"],
    errors="coerce",
)

for column in [
    "OverallCondition",
    "CleanlinessCondition",
    "SafetyCondition",
    "StructuralCondition",
]:
    gold[column] = gold[column].astype("string")

gold["HasOverallCondition"] = (
    gold["OverallCondition"].notna()
    & gold["OverallCondition"].ne("").fillna(False)
)

gold["HasSafetyCondition"] = (
    gold["SafetyCondition"].notna()
    & gold["SafetyCondition"].ne("").fillna(False)
)

gold["HasStructuralCondition"] = (
    gold["StructuralCondition"].notna()
    & gold["StructuralCondition"].ne("").fillna(False)
)

total = len(gold)
matched = int(gold["AssetReferenceValid"].sum())
unmatched = int(gold["UnmatchedAssetReference"].sum())
missing_references = int(gold["MissingAssetReference"].sum())

if total != EXPECTED_ROWS:
    raise ValueError("Inspection record count mismatch.")

if matched != EXPECTED_MATCHED:
    raise ValueError("Matched asset count mismatch.")

if unmatched != EXPECTED_UNMATCHED:
    raise ValueError("Unmatched asset count mismatch.")

if matched + unmatched + missing_references != total:
    raise ValueError("Inspection relationship counts do not reconcile.")

if gold["InspectionID"].isna().any():
    raise ValueError("Missing inspection identifiers.")

if gold["InspectionID"].duplicated().any():
    raise ValueError("Duplicate inspection identifiers.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    saved = pd.read_parquet(TARGET)

    assert_frame_equal(
        saved,
        gold,
        check_dtype=False,
        check_freq=False,
    )

    print("Existing Gold Inspections verified.")

else:
    temporary = TARGET.with_name("fact_inspections.parquet.tmp")

    if temporary.exists():
        raise FileExistsError(
            "Temporary output exists. Review before rerunning."
        )

    gold.to_parquet(
        temporary,
        engine="pyarrow",
        index=False,
    )

    saved = pd.read_parquet(temporary)

    assert_frame_equal(
        saved,
        gold,
        check_dtype=False,
        check_freq=False,
    )

    os.replace(temporary, TARGET)

    print("Gold Inspections created and verified.")

print("\n--- GOLD FACT INSPECTIONS ---")
print("Total inspections:", total)
print("Unique inspection IDs:", gold["InspectionID"].nunique())
print("Matched asset references:", matched)
print("Unmatched asset references:", unmatched)
print("Missing asset references:", missing_references)
print("Missing inspection dates:", int(gold["InspectionDate"].isna().sum()))
print("Overall conditions available:", int(gold["HasOverallCondition"].sum()))
print("Safety conditions available:", int(gold["HasSafetyCondition"].sum()))
print("Structural conditions available:", int(gold["HasStructuralCondition"].sum()))

print("\nGOLD FACT INSPECTIONS VALIDATION PASSED")
