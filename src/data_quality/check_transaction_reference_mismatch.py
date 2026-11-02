
# Compares original and Silver transaction IDs to identify reference mismatches.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

RAW_HEADERS = ROOT / "data/raw/nyc_amps/transactions_batches"
SILVER_HEADERS = ROOT / "data/silver/amps_transactions"
SILVER_DETAILS = ROOT / "data/silver/amps_transaction_details"


def clean(series):
    return (
        series.astype("string")
        .str.strip()
        .replace("", pd.NA)
    )


def load_ids(folder, pattern, column):
    result = set()
    files = sorted(folder.glob(pattern))

    for path in files:
        frame = pd.read_parquet(path, columns=[column]) if (
            path.suffix == ".parquet"
        ) else pd.read_csv(path, usecols=[column], dtype="string")

        result.update(clean(frame[column]).dropna().tolist())

    return result, len(files)


raw_ids, raw_files = load_ids(
    RAW_HEADERS,
    "transactions_batch_*.csv",
    "tra_code",
)

silver_ids, silver_files = load_ids(
    SILVER_HEADERS,
    "transactions_batch_*.parquet",
    "TransactionID",
)

print("\n--- TRANSACTION HEADER COMPARISON ---")
print("Raw header files:", raw_files)
print("Silver header files:", silver_files)
print("Raw unique transaction IDs:", len(raw_ids))
print("Silver unique transaction IDs:", len(silver_ids))
print("IDs only in Raw:", len(raw_ids - silver_ids))
print("IDs only in Silver:", len(silver_ids - raw_ids))
print("Sample Raw-only IDs:", sorted(raw_ids - silver_ids)[:10])
print("Sample Silver-only IDs:", sorted(silver_ids - raw_ids)[:10])

matched_raw = 0
matched_silver = 0
raw_only_matches = 0
silver_only_matches = 0
missing = 0
total = 0

files = sorted(
    SILVER_DETAILS.glob("transaction_details_batch_*.parquet")
)

for path in files:
    df = pd.read_parquet(
        path,
        columns=["TransactionID"],
    )

    refs = clean(df["TransactionID"])

    raw_match = refs.isin(raw_ids)
    silver_match = refs.isin(silver_ids)

    total += len(refs)
    missing += int(refs.isna().sum())
    matched_raw += int(raw_match.sum())
    matched_silver += int(silver_match.sum())
    raw_only_matches += int(
        (raw_match & ~silver_match).sum()
    )
    silver_only_matches += int(
        (silver_match & ~raw_match).sum()
    )

print("\n--- TRANSACTION DETAIL RECONCILIATION ---")
print("Detail files:", len(files))
print("Total detail records:", total)
print("Matches using Raw headers:", matched_raw)
print("Matches using Silver headers:", matched_silver)
print("Matches Raw only:", raw_only_matches)
print("Matches Silver only:", silver_only_matches)
print("Missing Transaction IDs:", missing)

print("\nDIAGNOSTIC COMPLETED — NO FILES MODIFIED")
