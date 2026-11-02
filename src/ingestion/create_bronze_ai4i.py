
# Converts AI4I machine-failure data into validated local Bronze Parquet.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/ai4i/ai4i2020.csv"
TARGET = ROOT / "data/bronze/ai4i2020.parquet"

EXPECTED_ROWS = 10_000
EXPECTED_COLUMNS = 14

if not SOURCE.exists():
    raise FileNotFoundError(SOURCE)

df = pd.read_csv(
    SOURCE,
    dtype=str,
    keep_default_na=False,
    encoding="utf-8-sig",
    low_memory=False,
)

required = {
    "UDI",
    "Product ID",
    "Type",
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]",
    "Machine failure",
    "TWF",
    "HDF",
    "PWF",
    "OSF",
    "RNF",
}

if len(df) != EXPECTED_ROWS:
    raise ValueError(f"Unexpected row count: {len(df)}")

if len(df.columns) != EXPECTED_COLUMNS:
    raise ValueError(f"Unexpected column count: {len(df.columns)}")

if not required.issubset(df.columns):
    raise ValueError(
        f"Missing columns: {sorted(required - set(df.columns))}"
    )

if df["UDI"].eq("").any() or df["UDI"].duplicated().any():
    raise ValueError("Missing or duplicate UDI identifiers.")

if not df["Machine failure"].isin(["0", "1"]).all():
    raise ValueError("Invalid machine-failure labels.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    bronze = pd.read_parquet(TARGET)

    if not df.equals(bronze):
        raise ValueError("Existing Bronze AI4I content mismatch.")

    print("Existing Bronze AI4I verified.")

else:
    temporary = TARGET.with_name("ai4i2020.parquet.tmp")

    if temporary.exists():
        raise FileExistsError(temporary)

    df.to_parquet(temporary, engine="pyarrow", index=False)

    bronze = pd.read_parquet(temporary)

    if not df.equals(bronze):
        raise ValueError("Bronze AI4I content mismatch.")

    temporary.rename(TARGET)
    print("Bronze AI4I created and verified.")

failures = int((bronze["Machine failure"] == "1").sum())

print("\n--- BRONZE AI4I ---")
print("Rows:", len(bronze))
print("Columns:", len(bronze.columns))
print("Machine failures:", failures)
print("Failure rate (%):", round(failures / len(bronze) * 100, 2))
print("Output:", TARGET)
print("BRONZE AI4I VALIDATION PASSED")
