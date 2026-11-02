
# Validates the AMPS Work Orders CSV structure and checks for incomplete records.

from pathlib import Path
import csv

project_root = Path(__file__).resolve().parents[2]
file_path = project_root / "data/raw/nyc_amps/AMPS_WorkOrders.csv"

record_count = 0
invalid_rows = []

try:
    with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file, strict=True)
        header = next(reader)
        expected_columns = len(header)

        for row_number, row in enumerate(reader, start=2):
            record_count += 1

            if len(row) != expected_columns:
                invalid_rows.append((row_number, len(row)))

                if len(invalid_rows) >= 10:
                    break

    print("Expected columns:", expected_columns)
    print("Records checked:", record_count)
    print("Invalid records:", invalid_rows)

except (csv.Error, UnicodeError) as error:
    print("CSV parsing error:", error)
    print("Records successfully read:", record_count)
