
# Converts NYC Parks Inspections CSV into a validated local Bronze Parquet file.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/supplementary/Parks_Inspections.csv"
TARGET = ROOT / "data/bronze/parks_inspections.parquet"

EXPECTED_ROWS = 153_093
EXPECTED_COLUMNS = 20

if not SOURCE.exists():
    raise FileNotFoundError(SOURCE)

df = pd.read_csv(
    SOURCE,
    dtype=str,
    keep_default_na=False,
    encoding="utf-8-sig",
    low_memory=False,
)

if len(df) != EXPECTED_ROWS:
    raise ValueError(
        f"Expected {EXPECTED_ROWS} rows, found {len(df)}."
    )

if len(df.columns) != EXPECTED_COLUMNS:
    raise ValueError("Unexpected inspection column count.")

required = {
    "Prop ID",
    "Inspection ID",
    "Date",
    "Overall Condition",
}

if not required.issubset(df.columns):
    raise ValueError("Required inspection columns missing.")

if df["Inspection ID"].eq("").any():
    raise ValueError("Missing Inspection IDs found.")

if df["Inspection ID"].duplicated().any():
    raise ValueError("Duplicate Inspection IDs found.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    bronze = pd.read_parquet(TARGET)

    if not df.equals(bronze):
        raise ValueError("Existing Bronze Inspections content mismatch.")

    print("Existing Bronze Inspections verified.")

else:
    temporary = TARGET.with_name("parks_inspections.parquet.tmp")

    if temporary.exists():
        raise FileExistsError(temporary)

    df.to_parquet(
        temporary,
        engine="pyarrow",
        index=False,
    )

    bronze = pd.read_parquet(temporary)

    if not df.equals(bronze):
        raise ValueError("Bronze Inspections verification failed.")

    temporary.rename(TARGET)

    print("Bronze Inspections created and verified.")

print("\n--- BRONZE PARKS INSPECTIONS ---")
print("Rows:", len(bronze))
print("Columns:", len(bronze.columns))
print("Missing property IDs:", int(bronze["Prop ID"].eq("").sum()))
print("Output:", TARGET)
print("BRONZE INSPECTIONS VALIDATION PASSED")
