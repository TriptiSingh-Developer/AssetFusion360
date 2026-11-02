
# Identifies transaction identifier dependencies before making corrections.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

paths = {
    "Silver Transaction Headers":
        ROOT / "data/silver/amps_transactions",
    "Gold Transaction Headers":
        ROOT / "data/gold/fact_transactions",
    "Silver Transaction Details":
        ROOT / "data/silver/amps_transaction_details",
}

for name, folder in paths.items():
    files = sorted(folder.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No Parquet files: {folder}")

    sample = pd.read_parquet(files[0])

    print(f"\n--- {name} ---")
    print("Files:", len(files))

    for col in [
        "tra_code",
        "tra_sqlidentity",
        "TransactionID",
        "trl_trans",
        "trl_sqlidentity",
        "TransactionDetailID",
        "TransactionReferenceValid",
    ]:
        if col in sample.columns:
            print(f"{col}: {sample[col].head(5).tolist()}")

print("\nDEPENDENCY INSPECTION COMPLETED — NO FILES MODIFIED")
