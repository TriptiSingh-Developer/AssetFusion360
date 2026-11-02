
# Validates AI4I dataset size, columns, identifiers, and failure labels.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
FILE = ROOT / "data/raw/ai4i/ai4i2020.csv"

df = pd.read_csv(FILE)

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

missing_columns = required - set(df.columns)
duplicate_ids = df["UDI"].duplicated().sum() if "UDI" in df else None

print("Rows:", len(df))
print("Columns:", len(df.columns))
print("Missing required columns:", sorted(missing_columns))
print("Duplicate UDI:", duplicate_ids)

if "Machine failure" in df.columns:
    print("\nMachine failure distribution:")
    print(df["Machine failure"].value_counts(dropna=False))

    print("\nFailure rate (%):")
    print(round(df["Machine failure"].mean() * 100, 2))

passed = (
    len(df) == 10000
    and len(df.columns) == 14
    and not missing_columns
    and duplicate_ids == 0
)

print("\nVALIDATION PASSED" if passed else "\nVALIDATION NEEDS REVIEW")
