
# Creates Silver AI4I with validated machine measurements and failure labels.

from pathlib import Path
import os
import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/ai4i2020.parquet"
TARGET = ROOT / "data/silver/ai4i2020.parquet"

EXPECTED_ROWS = 10_000
EXPECTED_FAILURES = 339

df = pd.read_parquet(SOURCE)

required = {
    "UDI", "Product ID", "Type",
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]",
    "Machine failure",
    "TWF", "HDF", "PWF", "OSF", "RNF",
}

if not required.issubset(df.columns):
    raise ValueError("Required AI4I columns missing.")

silver = df.copy()

silver["MachineID"] = (
    silver["UDI"].fillna("").astype(str).str.strip()
)

silver["ProductID"] = (
    silver["Product ID"].fillna("").astype(str).str.strip()
)

silver["MachineType"] = (
    silver["Type"].fillna("").astype(str).str.strip().str.upper()
)

measurements = {
    "Air temperature [K]": "AirTemperatureK",
    "Process temperature [K]": "ProcessTemperatureK",
    "Rotational speed [rpm]": "RotationalSpeedRPM",
    "Torque [Nm]": "TorqueNm",
    "Tool wear [min]": "ToolWearMinutes",
}

for original, cleaned in measurements.items():
    silver[cleaned] = pd.to_numeric(
        silver[original].replace("", pd.NA),
        errors="coerce",
    ).astype("float64")

labels = {
    "Machine failure": "MachineFailure",
    "TWF": "ToolWearFailure",
    "HDF": "HeatDissipationFailure",
    "PWF": "PowerFailure",
    "OSF": "OverstrainFailure",
    "RNF": "RandomFailure",
}

for original, cleaned in labels.items():
    silver[cleaned] = pd.to_numeric(
        silver[original].replace("", pd.NA),
        errors="coerce",
    ).astype("Int64")

if len(silver) != EXPECTED_ROWS:
    raise ValueError("Unexpected AI4I row count.")

if silver["MachineID"].eq("").any():
    raise ValueError("Missing Machine IDs.")

if silver["MachineID"].duplicated().any():
    raise ValueError("Duplicate Machine IDs.")

for column in labels.values():
    if silver[column].isna().any():
        raise ValueError(f"Missing failure labels: {column}")

    if not silver[column].isin([0, 1]).all():
        raise ValueError(f"Invalid binary values: {column}")

failures = int(silver["MachineFailure"].sum())

if failures != EXPECTED_FAILURES:
    raise ValueError("Unexpected machine-failure count.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    existing = pd.read_parquet(TARGET)

    assert_frame_equal(
        existing,
        silver,
        check_dtype=False,
        check_freq=False,
    )

    print("Existing Silver AI4I verified.")

else:
    temporary = TARGET.with_name("ai4i2020.parquet.tmp")

    if temporary.exists():
        raise FileExistsError(
            "Temporary AI4I file exists. Review before rerunning."
        )

    silver.to_parquet(
        temporary,
        engine="pyarrow",
        index=False,
    )

    saved = pd.read_parquet(temporary)

    assert_frame_equal(
        saved,
        silver,
        check_dtype=False,
        check_freq=False,
    )

    os.replace(temporary, TARGET)
    print("Silver AI4I created and verified.")

print("\n--- SILVER AI4I ---")
print("Rows:", len(silver))
print("Columns:", len(silver.columns))
print("Machine failures:", failures)
print("Failure rate (%):", round(failures / len(silver) * 100, 2))

print("\nMissing measurement values:")
print(silver[list(measurements.values())].isna().sum())

print("\nSILVER AI4I VALIDATION PASSED")
