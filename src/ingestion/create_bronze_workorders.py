
# Converts AMPS Work Orders CSV batches to validated local Bronze Parquet files.

from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/nyc_amps/workorders_batches"
TARGET = ROOT / "data/bronze/amps_workorders"

TARGET.mkdir(parents=True, exist_ok=True)

files = sorted(SOURCE.glob("workorders_batch_*.csv"))

EXPECTED_BATCHES = 447
EXPECTED_ROWS = 2_231_535

if len(files) != EXPECTED_BATCHES:
    raise ValueError(
        f"Expected {EXPECTED_BATCHES} batches, found {len(files)}"
    )

total_rows = 0
converted = 0
verified_existing = 0
expected_columns = None

for number, file in enumerate(files, start=1):

    expected_name = f"workorders_batch_{number:05d}.csv"

    if file.name != expected_name:
        raise ValueError(f"Unexpected filename: {file.name}")

    df = pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=False,
    )

    if "evt_code" not in df.columns:
        raise ValueError(f"Missing evt_code in {file.name}")

    if expected_columns is None:
        expected_columns = list(df.columns)
    elif list(df.columns) != expected_columns:
        raise ValueError(f"Schema mismatch in {file.name}")

    output = TARGET / f"{file.stem}.parquet"

    if output.exists():
        metadata = pq.read_metadata(output)

        if metadata.num_rows != len(df):
            raise ValueError(
                f"Existing Parquet row-count mismatch: {output.name}"
            )

        if metadata.schema.names != expected_columns:
            raise ValueError(
                f"Existing Parquet schema mismatch: {output.name}"
            )

        verified_existing += 1

    else:
        temporary = TARGET / f"{file.stem}.parquet.tmp"

        if temporary.exists():
            raise FileExistsError(
                f"Temporary file exists: {temporary.name}"
            )

        df.to_parquet(
            temporary,
            engine="pyarrow",
            index=False,
        )

        metadata = pq.read_metadata(temporary)

        if metadata.num_rows != len(df):
            raise ValueError(
                f"Parquet verification failed: {file.name}"
            )

        temporary.rename(output)
        converted += 1

    total_rows += len(df)

    if number % 50 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total_rows:,} records",
            flush=True,
        )

print("\n--- BRONZE WORK ORDERS ---")
print("Source batches:", len(files))
print("New Parquet files:", converted)
print("Verified existing files:", verified_existing)
print("Total records:", total_rows)
print("Columns:", len(expected_columns))

if total_rows != EXPECTED_ROWS:
    raise ValueError("Total record count mismatch.")

print("\nBRONZE WORK ORDERS COMPLETED")
