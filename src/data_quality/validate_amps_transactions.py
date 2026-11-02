
# Validates AMPS Transactions batch files, record counts, and identifiers.

from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/transactions_batches"

files = sorted(BATCH_DIR.glob("transactions_batch_*.csv"))

total = 0
identifiers = set()
duplicates = 0
missing = 0
errors = []
last_id = -1

for number, file in enumerate(files, start=1):
    if file.name != f"transactions_batch_{number:05d}.csv":
        errors.append((file.name, "Unexpected batch numbering"))

    try:
        with file.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)

            if "tra_sqlidentity" not in (reader.fieldnames or []):
                raise ValueError("Missing tra_sqlidentity column")

            for row in reader:
                total += 1
                identifier = row.get("tra_sqlidentity")

                if not identifier:
                    missing += 1
                    continue

                numeric_id = int(identifier)

                if numeric_id in identifiers:
                    duplicates += 1
                else:
                    identifiers.add(numeric_id)

                if numeric_id <= last_id:
                    errors.append(
                        (file.name, "Identifiers out of order")
                    )

                last_id = numeric_id

    except Exception as error:
        errors.append((file.name, str(error)))

print("Batch files:", len(files))
print("Total records:", total)
print("Duplicate TRA_SQLIDENTITY:", duplicates)
print("Missing TRA_SQLIDENTITY:", missing)
print("Last TRA_SQLIDENTITY:", last_id)
print("Errors:", errors[:5])

passed = (
    len(files) == 152
    and total == 755122
    and duplicates == 0
    and missing == 0
    and last_id == 966897
    and not errors
)

print("VALIDATION PASSED" if passed else "VALIDATION NEEDS REVIEW")
