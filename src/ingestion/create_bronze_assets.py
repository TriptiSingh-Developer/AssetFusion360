
# Converts AMPS Assets CSV into a validated local Bronze Parquet file.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/raw/nyc_amps/AMPS_Assets.csv"
TARGET = ROOT / "data/bronze/amps_assets.parquet"

EXPECTED_ROWS = 148_522
EXPECTED_COLUMNS = 29

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
    raise ValueError("Unexpected Assets record count.")

if len(df.columns) != EXPECTED_COLUMNS:
    raise ValueError("Unexpected Assets column count.")

if "OBJ_CODE" not in df.columns:
    raise ValueError("OBJ_CODE column missing.")

TARGET.parent.mkdir(parents=True, exist_ok=True)

if TARGET.exists():
    bronze = pd.read_parquet(TARGET)

    if not df.equals(bronze):
        raise ValueError(
            "Existing Bronze Assets content does not match the source."
        )

    print("Existing Bronze Assets verified.")

else:
    temporary = TARGET.with_name("amps_assets.parquet.tmp")

    if temporary.exists():
        raise FileExistsError(temporary)

    df.to_parquet(temporary, engine="pyarrow", index=False)

    bronze = pd.read_parquet(temporary)

    if not df.equals(bronze):
        raise ValueError("Bronze Assets content validation failed.")

    temporary.rename(TARGET)
    print("Bronze Assets created and verified.")

print("\n--- BRONZE ASSETS ---")
print("Rows:", len(bronze))
print("Columns:", len(bronze.columns))
print("Output:", TARGET)
print("BRONZE ASSETS VALIDATION PASSED")
