
# Converts NYC 311 DPR CSV batches into validated local Bronze Parquet files.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/nyc_311/dpr_batches"
TARGET = ROOT / "data/bronze/nyc311_dpr"

EXPECTED_BATCHES = 176
EXPECTED_ROWS = 875_134

files = sorted(SOURCE.glob("dpr_batch_*.csv"))

if len(files) != EXPECTED_BATCHES:
    raise ValueError(
        f"Expected {EXPECTED_BATCHES} batches, found {len(files)}."
    )

TARGET.mkdir(parents=True, exist_ok=True)

total_rows = 0
created = 0
verified = 0
expected_columns = None

for number, source_file in enumerate(files, start=1):

    expected_name = f"dpr_batch_{number:05d}.csv"

    if source_file.name != expected_name:
        raise ValueError(f"Unexpected filename: {source_file.name}")

    df = pd.read_csv(
        source_file,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=False,
    )

    required = {"unique_key", "agency", "created_date"}

    if not required.issubset(df.columns):
        raise ValueError(f"Required columns missing: {source_file.name}")

    if expected_columns is None:
        expected_columns = list(df.columns)
    elif list(df.columns) != expected_columns:
        raise ValueError(f"Schema mismatch: {source_file.name}")

    if not df["agency"].eq("DPR").all():
        raise ValueError(f"Non-DPR records found: {source_file.name}")

    output_file = TARGET / f"{source_file.stem}.parquet"

    if output_file.exists():
        bronze = pd.read_parquet(output_file)

        if not df.equals(bronze):
            raise ValueError(f"Content mismatch: {output_file.name}")

        verified += 1

    else:
        temporary = TARGET / f"{source_file.stem}.parquet.tmp"

        if temporary.exists():
            raise FileExistsError(temporary)

        df.to_parquet(temporary, engine="pyarrow", index=False)

        bronze = pd.read_parquet(temporary)

        if not df.equals(bronze):
            raise ValueError(f"Verification failed: {source_file.name}")

        temporary.rename(output_file)
        created += 1

    total_rows += len(df)

    if number % 25 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total_rows:,} records",
            flush=True,
        )

if total_rows != EXPECTED_ROWS:
    raise ValueError(
        f"Expected {EXPECTED_ROWS} records, found {total_rows}."
    )

print("\n--- BRONZE NYC 311 DPR ---")
print("Source batches:", len(files))
print("New Parquet files:", created)
print("Verified existing files:", verified)
print("Total records:", total_rows)
print("Columns:", len(expected_columns))
print("BRONZE NYC 311 VALIDATION PASSED")
