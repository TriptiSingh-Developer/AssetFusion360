
# Creates the Gold asset dimension from validated Silver Assets.

from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/silver/amps_assets.parquet"
TARGET = ROOT / "data/gold/dim_asset.parquet"

EXPECTED_ROWS = 148_521

silver = pd.read_parquet(SOURCE)

required = {
    "AssetID",
    "OBJ_DESC",
    "OBJ_CLASS",
    "OBJ_OBTYPE",
    "OBJ_STATUS",
    "OBJ_SQLIDENTITY",
}

if not required.issubset(silver.columns):
    raise ValueError("Required Silver Asset columns missing.")

gold = pd.DataFrame({
    "AssetID": silver["AssetID"],
    "AssetName": silver["OBJ_DESC"],
    "AssetClass": silver["OBJ_CLASS"],
    "AssetObjectType": silver["OBJ_OBTYPE"],
    "AssetStatusCode": silver["OBJ_STATUS"],
    "SourceRecordID": silver["OBJ_SQLIDENTITY"],
})

for column in gold.columns:
    gold[column] = (
        gold[column]
        .fillna("")
        .astype("string")
        .str.strip()
    )

gold = gold.sort_values("AssetID").reset_index(drop=True)

if len(gold) != EXPECTED_ROWS:
    raise ValueError("Unexpected Gold Asset count.")

if gold["AssetID"].isna().any() or gold["AssetID"].eq("").any():
    raise ValueError("Missing AssetIDs.")

if gold["AssetID"].duplicated().any():
    raise ValueError("Duplicate AssetIDs.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    existing = pd.read_parquet(TARGET)

    assert_frame_equal(
        existing,
        gold,
        check_dtype=False,
    )

    print("Existing Gold Asset dimension verified.")

else:
    temporary = TARGET.with_name("dim_asset.parquet.tmp")

    if temporary.exists():
        raise FileExistsError(
            "Temporary file exists. Review before rerunning."
        )

    gold.to_parquet(temporary, engine="pyarrow", index=False)

    saved = pd.read_parquet(temporary)

    assert_frame_equal(saved, gold, check_dtype=False)

    temporary.rename(TARGET)
    print("Gold Asset dimension created.")

print("\n--- GOLD DIM ASSET ---")
print("Total assets:", len(gold))
print("Unique AssetIDs:", gold["AssetID"].nunique())
print("Missing names:", int(gold["AssetName"].eq("").sum()))

print("\nTop asset classes:")
print(gold["AssetClass"].value_counts().head(10))

print("\nGOLD DIM ASSET VALIDATION PASSED")
