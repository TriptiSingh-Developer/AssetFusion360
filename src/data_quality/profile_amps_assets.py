
# Profiles the raw AMPS Assets dataset for structure, missing values, and duplicate asset IDs.

from pathlib import Path
import pandas as pd

project_root = Path(__file__).resolve().parents[2]

file_path = (
    project_root
    / "data"
    / "raw"
    / "nyc_amps"
    / "AMPS_Assets.csv"
)

df = pd.read_csv(
    file_path,
    dtype=str,
    low_memory=False
)

print("\n--- DATASET STRUCTURE ---")
print("Rows:", len(df))
print("Columns:", len(df.columns))

print("\n--- COLUMN NAMES ---")
print(df.columns.tolist())

print("\n--- FIRST FIVE RECORDS ---")
print(df.head().to_string(index=False))

print("\n--- MISSING VALUES ---")
print(df.isna().sum().sort_values(ascending=False))

print("\n--- ASSET ID QUALITY ---")
print("Missing OBJ_CODE:", df["OBJ_CODE"].isna().sum())
print("Duplicate OBJ_CODE:", df["OBJ_CODE"].duplicated().sum())

print("\n--- ASSET CATEGORIES ---")
print(df["OBJ_CATEGORY"].value_counts(dropna=False).head(10))

print("\n--- ASSET STATUS ---")
print(df["OBJ_STATUS"].value_counts(dropna=False))


# Identifies repeated asset codes and examines asset object types.

duplicates = df[df["OBJ_CODE"].duplicated(keep=False)]

print("\n--- DUPLICATE ASSET RECORDS ---")
print(
    duplicates[
        ["OBJ_CODE", "OBJ_OBTYPE", "OBJ_DESC",
         "OBJ_CLASS", "OBJ_STATUS", "OBJ_SQLIDENTITY"]
    ].to_string(index=False)
)

print("\n--- OBJECT TYPE DISTRIBUTION ---")
print(df["OBJ_OBTYPE"].value_counts(dropna=False))

print("\n--- UNIQUE ASSET CODES ---")
print(df["OBJ_CODE"].nunique(dropna=False))
