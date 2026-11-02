
# Profiles NYC Parks inspections and checks identifiers and missing values.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
FILE = ROOT / "data/raw/supplementary/Parks_Inspections.csv"

df = pd.read_csv(FILE, low_memory=False)

print("Rows:", len(df))
print("Columns:", len(df.columns))

print("\n--- COLUMN NAMES ---")
print(df.columns.tolist())

print("\n--- FIRST THREE RECORDS ---")
print(df.head(3).to_string(index=False))

print("\n--- MISSING VALUES (TOP 10) ---")
print(df.isna().sum().sort_values(ascending=False).head(10))

print("\n--- POTENTIAL IDENTIFIERS ---")
for column in df.columns:
    if any(term in column.lower() for term in ["id", "code", "gis", "property"]):
        print(
            column,
            "| Unique:", df[column].nunique(dropna=True),
            "| Missing:", df[column].isna().sum()
        )

print("\nINSPECTION PROFILING COMPLETED")
