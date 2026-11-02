
# Converts raw AMPS Transaction Details into validated Bronze Parquet batches.

from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/nyc_amps/transaction_details_batches"
TARGET = ROOT / "data/bronze/amps_transaction_details"

EXPECTED_FILES = 461
EXPECTED_ROWS = 2_301_927
EXPECTED_COLUMNS = 31

files = sorted(SOURCE.glob("transaction_details_batch_*.csv"))

if len(files) != EXPECTED_FILES:
    raise ValueError(
        f"Expected {EXPECTED_FILES} CSV files, found {len(files)}."
    )

TARGET.mkdir(parents=True, exist_ok=True)

total_rows = 0
created = 0
verified = 0
schema_columns = None

for number, source_file in enumerate(files, start=1):

    expected_name = (
        f"transaction_details_batch_{number:05d}.csv"
    )

    if source_file.name != expected_name:
        raise ValueError(
            f"Unexpected source batch: {source_file.name}"
        )

    df = pd.read_csv(
        source_file,
        dtype="string",
        keep_default_na=False,
    )

    if len(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Unexpected column count: {source_file.name}"
        )

    if schema_columns is None:
        schema_columns = list(df.columns)
    elif list(df.columns) != schema_columns:
        raise ValueError(
            f"Inconsistent schema: {source_file.name}"
        )

    if len(df) == 0 or len(df) > 5000:
        raise ValueError(
            f"Invalid batch size: {source_file.name}"
        )

    if number < EXPECTED_FILES and len(df) != 5000:
        raise ValueError(
            f"Incomplete batch: {source_file.name}"
        )

    output = TARGET / (
        f"transaction_details_batch_{number:05d}.parquet"
    )

    if output.exists():
        saved = pd.read_parquet(output)
        if not saved.equals(df):
            raise ValueError(
                f"Existing Bronze data differs: {output.name}"
            )
        verified += 1

    else:
        temporary = TARGET / (
            f"transaction_details_batch_{number:05d}.parquet.tmp"
        )

        if temporary.exists():
            raise FileExistsError(
                f"Review temporary file: {temporary.name}"
            )

        df.to_parquet(
            temporary,
            engine="pyarrow",
            index=False,
        )

        metadata = pq.read_metadata(temporary)

        if (
            metadata.num_rows != len(df)
            or metadata.schema.names != schema_columns
        ):
            raise ValueError(
                f"Parquet metadata mismatch: {output.name}"
            )

        if not pd.read_parquet(temporary).equals(df):
            raise ValueError(
                f"Bronze content mismatch: {output.name}"
            )

        os.replace(temporary, output)
        created += 1

    total_rows += len(df)

    if number % 50 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total_rows:,} rows",
            flush=True,
        )

if total_rows != EXPECTED_ROWS:
    raise ValueError(
        f"Expected {EXPECTED_ROWS:,} rows, found {total_rows:,}."
    )

if created + verified != EXPECTED_FILES:
    raise ValueError("Bronze file count mismatch.")

print("\n--- BRONZE TRANSACTION DETAILS ---")
print("Total records:", total_rows)
print("New Bronze files:", created)
print("Verified existing files:", verified)
print("Columns:", len(schema_columns))
print("Output:", TARGET)

print("\nBRONZE TRANSACTION DETAILS VALIDATION PASSED")
