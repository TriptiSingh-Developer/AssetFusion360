
# Creates a validated Gold AssetGuard dataset for machine-failure prediction.

from pathlib import Path
import os

import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/silver/ai4i2020.parquet"
TARGET = ROOT / "data/gold/assetguard_ml.parquet"

EXPECTED_ROWS = 10_000
EXPECTED_FAILURES = 339

FEATURES = [
    "MachineType",
    "AirTemperatureK",
    "ProcessTemperatureK",
    "RotationalSpeedRPM",
    "TorqueNm",
    "ToolWearMinutes",
]

TARGET_COLUMN = "MachineFailure"

silver = pd.read_parquet(SOURCE)

required = {"MachineID", *FEATURES, TARGET_COLUMN}

if not required.issubset(silver.columns):
    raise ValueError(
        f"Missing required columns: {sorted(required - set(silver.columns))}"
    )

gold = silver[
    ["MachineID", *FEATURES, TARGET_COLUMN]
].copy()

gold["MachineID"] = (
    gold["MachineID"].astype("string").str.strip()
)

gold["MachineType"] = (
    gold["MachineType"].astype("string").str.strip().str.upper()
)

for column in FEATURES[1:]:
    gold[column] = pd.to_numeric(
        gold[column], errors="coerce"
    ).astype("float64")

gold[TARGET_COLUMN] = pd.to_numeric(
    gold[TARGET_COLUMN], errors="coerce"
).astype("Int64")

if len(gold) != EXPECTED_ROWS:
    raise ValueError("Unexpected record count.")

if gold["MachineID"].isna().any() or gold["MachineID"].eq("").any():
    raise ValueError("Missing Machine IDs.")

if gold["MachineID"].duplicated().any():
    raise ValueError("Duplicate Machine IDs.")

if gold[FEATURES].isna().any().any():
    raise ValueError("Missing predictive feature values.")

if not gold[TARGET_COLUMN].isin([0, 1]).all():
    raise ValueError("Invalid target labels.")

failures = int(gold[TARGET_COLUMN].sum())

if failures != EXPECTED_FAILURES:
    raise ValueError("Unexpected failure count.")

gold = gold.sort_values("MachineID").reset_index(drop=True)

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    saved = pd.read_parquet(TARGET)

    assert_frame_equal(
        saved,
        gold,
        check_dtype=False,
        check_freq=False,
    )

    print("Existing Gold AssetGuard dataset verified.")

else:
    temp = TARGET.with_name("assetguard_ml.parquet.tmp")

    if temp.exists():
        raise FileExistsError(
            "Temporary file exists. Review before rerunning."
        )

    gold.to_parquet(temp, engine="pyarrow", index=False)

    saved = pd.read_parquet(temp)

    assert_frame_equal(
        saved,
        gold,
        check_dtype=False,
        check_freq=False,
    )

    os.replace(temp, TARGET)

    print("Gold AssetGuard dataset created and verified.")

print("\n--- GOLD ASSETGUARD ML ---")
print("Rows:", len(gold))
print("Predictive features:", len(FEATURES))
print("Features:", FEATURES)
print("Machine failures:", failures)
print("Non-failures:", len(gold) - failures)
print("Failure rate (%):", round(failures / len(gold) * 100, 2))
print("Missing feature values:", int(gold[FEATURES].isna().sum().sum()))
print("Output:", TARGET)

print("\nGOLD ASSETGUARD VALIDATION PASSED")
