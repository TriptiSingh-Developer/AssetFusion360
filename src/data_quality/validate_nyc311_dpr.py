
# Validates NYC 311 DPR records, unique identifiers, and CSV batches.

from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_311/dpr_batches"

files = sorted(BATCH_DIR.glob("dpr_batch_*.csv"))

total = 0
keys = set()
duplicates = 0
missing = 0
non_dpr = 0
errors = []
previous_key = ""

for number, path in enumerate(files, 1):
    if path.name != f"dpr_batch_{number:05d}.csv":
        errors.append((path.name, "Unexpected batch numbering"))

    try:
        with path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            reader = csv.DictReader(handle, strict=True)

            required = {"unique_key", "agency"}
            if not required.issubset(reader.fieldnames or []):
                raise ValueError("Required columns missing")

            for row in reader:
                total += 1
                key = row["unique_key"]

                if not key:
                    missing += 1
                    continue

                if key in keys:
                    duplicates += 1
                else:
                    keys.add(key)

                if previous_key and key <= previous_key:
                    errors.append((path.name, "Key ordering error"))

                previous_key = key

                if row["agency"] != "DPR":
                    non_dpr += 1

    except Exception as error:
        errors.append((path.name, str(error)))

print("Batch files:", len(files))
print("Total records:", total)
print("Duplicate unique_key:", duplicates)
print("Missing unique_key:", missing)
print("Non-DPR records:", non_dpr)
print("Errors:", errors[:5])

passed = (
    len(files) == 176
    and total == 875134
    and duplicates == 0
    and missing == 0
    and non_dpr == 0
    and not errors
)

print("VALIDATION PASSED" if passed else "VALIDATION NEEDS REVIEW")
