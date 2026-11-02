
# Compares Bronze Work Orders Parquet content with the original CSV batches.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data/raw/nyc_amps/workorders_batches"
BRONZE = ROOT / "data/bronze/amps_workorders"

files = sorted(RAW.glob("workorders_batch_*.csv"))
parquet_files = list(BRONZE.glob("workorders_batch_*.parquet"))

if len(files) != 447 or len(parquet_files) != 447:
    raise ValueError("Expected exactly 447 CSV and 447 Parquet files.")

total = 0

for number, csv_file in enumerate(files, start=1):
    parquet_file = BRONZE / f"{csv_file.stem}.parquet"

    if not parquet_file.exists():
        raise FileNotFoundError(parquet_file)

    raw = pd.read_csv(
        csv_file,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )

    bronze = pd.read_parquet(parquet_file)

    if not raw.equals(bronze):
        raise ValueError(
            f"Content mismatch in batch {number}: {csv_file.name}"
        )

    total += len(raw)

    if number % 50 == 0 or number == len(files):
        print(
            f"Validated {number}/447 batches | "
            f"{total:,} records",
            flush=True,
        )

if total != 2_231_535:
    raise ValueError("Final record count mismatch.")

print("\nBRONZE CONTENT VALIDATION PASSED")
print("Validated records:", total)
print("Validated files:", len(files))
