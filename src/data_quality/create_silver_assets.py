
# Creates canonical Silver Assets with standardized identifiers and deduplication.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/amps_assets.parquet"
TARGET = ROOT / "data/silver/amps_assets.parquet"

assets = pd.read_parquet(SOURCE)

if len(assets) != 148_522:
    raise ValueError("Unexpected Bronze Assets row count.")

required = {"OBJ_CODE", "OBJ_STATUS", "OBJ_SQLIDENTITY", "OBJ_OBTYPE"}

if not required.issubset(assets.columns):
    raise ValueError("Required columns missing.")

silver = assets.copy()

silver["AssetID"] = (
    silver["OBJ_CODE"]
    .fillna("")
    .astype(str)
    .str.strip()
    .str.upper()
)

if silver["AssetID"].eq("").any():
    raise ValueError("Missing asset identifiers found.")

silver["_active_priority"] = (
    silver["OBJ_STATUS"].fillna("").str.strip().eq("I").astype(int)
)

silver["_record_id"] = pd.to_numeric(
    silver["OBJ_SQLIDENTITY"], errors="coerce"
)

silver = (
    silver
    .sort_values(
        ["AssetID", "_active_priority", "_record_id"],
        ascending=[True, False, False],
        kind="stable",
    )
    .drop_duplicates("AssetID", keep="first")
    .drop(columns=["_active_priority", "_record_id"])
    .reset_index(drop=True)
)

if silver["AssetID"].duplicated().any():
    raise ValueError("Duplicate AssetIDs remain.")

if len(silver) != 148_521:
    raise ValueError("Unexpected Silver Assets row count.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    existing = pd.read_parquet(TARGET)
    if not existing.equals(silver):
        raise ValueError("Existing Silver Assets differs. Review before replacing.")
    print("Existing Silver Assets verified.")
else:
    temp = TARGET.with_name("amps_assets.parquet.tmp")
    if temp.exists():
        raise FileExistsError(temp)
    silver.to_parquet(temp, engine="pyarrow", index=False)
    temp.rename(TARGET)
    print("Silver Assets created.")

print("\n--- SILVER ASSETS ---")
print("Bronze rows:", len(assets))
print("Silver rows:", len(silver))
print("Duplicates resolved:", len(assets) - len(silver))

print("\nCanonical B223PA:")
print(
    silver.loc[
        silver["AssetID"].eq("B223PA"),
        ["AssetID", "OBJ_DESC", "OBJ_STATUS", "OBJ_SQLIDENTITY"]
    ].to_string(index=False)
)

print("\nSILVER ASSETS VALIDATION PASSED")
