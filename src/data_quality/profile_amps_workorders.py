
# Profiles AMPS Work Orders, including missing values, dates, and asset references.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"

files = sorted(BATCH_DIR.glob("workorders_batch_*.csv"))

if not files:
    raise FileNotFoundError("No Work Orders CSV batches found.")

total = 0
missing = {}
types = {}
priorities = {}
earliest = None
latest = None
missing_asset_refs = 0

for file in files:
    df = pd.read_csv(file, dtype=str, low_memory=False)
    total += len(df)

    for col in df.columns:
        missing[col] = missing.get(col, 0) + int(df[col].isna().sum())

    missing_asset_refs += int(
        df["evt_object"].isna().sum()
        + df["evt_object"].fillna("").str.strip().eq("").sum()
        - df["evt_object"].isna().sum()
    )

    for key, count in df["evt_type"].value_counts(dropna=False).items():
        key = str(key)
        types[key] = types.get(key, 0) + int(count)

    for key, count in df["evt_priority"].value_counts(dropna=False).items():
        key = str(key)
        priorities[key] = priorities.get(key, 0) + int(count)

    dates = pd.to_datetime(
        df["evt_date"], errors="coerce", format="mixed"
    ).dropna()

    if not dates.empty:
        batch_min = dates.min()
        batch_max = dates.max()
        earliest = batch_min if earliest is None else min(earliest, batch_min)
        latest = batch_max if latest is None else max(latest, batch_max)

print("\n--- AMPS WORK ORDERS PROFILE ---")
print("Batches:", len(files))
print("Total records:", total)
print("Columns:", len(missing))
print("Missing asset references:", missing_asset_refs)

print("\nWork Order types:")
print(pd.Series(types).sort_values(ascending=False).head(15))

print("\nPriorities:")
print(pd.Series(priorities).sort_values(ascending=False).head(15))

print("\nTop 15 columns with missing values:")
print(pd.Series(missing).sort_values(ascending=False).head(15))

print("\nEarliest Work Order date:", earliest)
print("Latest Work Order date:", latest)

print("\nPROFILING COMPLETED")
