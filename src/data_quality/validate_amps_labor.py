
# Validates AMPS Labor batch files, record counts, and unique identifiers.

from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/labor_batches"

files = sorted(BATCH_DIR.glob("labor_batch_*.csv"))

total = 0
identifiers = set()
duplicates = 0
missing = 0
errors = []

for file in files:
    try:
        with file.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)

            for row in reader:
                total += 1
                identifier = row.get("boo_code")

                if not identifier:
                    missing += 1
                elif identifier in identifiers:
                    duplicates += 1
                else:
                    identifiers.add(identifier)

    except Exception as error:
        errors.append((file.name, str(error)))

print("Batch files:", len(files))
print("Total records:", total)
print("Duplicate BOO_CODE:", duplicates)
print("Missing BOO_CODE:", missing)
print("CSV errors:", errors[:5])

if len(files) == 647 and total == 3230423 and duplicates == 0 and missing == 0 and not errors:
    print("VALIDATION PASSED")
else:
    print("VALIDATION NEEDS REVIEW")
