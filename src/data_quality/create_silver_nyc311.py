
# Creates Silver NYC 311 DPR requests with standardized fields and dates.

from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/nyc311_dpr"
TARGET = ROOT / "data/silver/nyc311_dpr"

EXPECTED_BATCHES = 176
EXPECTED_ROWS = 875_134

files = sorted(SOURCE.glob("dpr_batch_*.parquet"))

if len(files) != EXPECTED_BATCHES:
    raise ValueError("Unexpected NYC 311 Bronze batch count.")

TARGET.mkdir(parents=True, exist_ok=True)

total = 0
created = 0
verified = 0
missing_ids = 0
missing_created_dates = 0
missing_closed_dates = 0
negative_resolution = 0
non_dpr = 0

for number, source_file in enumerate(files, start=1):

    if source_file.name != f"dpr_batch_{number:05d}.parquet":
        raise ValueError("Unexpected source filename.")

    df = pd.read_parquet(source_file)

    required = {
        "unique_key",
        "agency",
        "created_date",
        "closed_date",
        "status",
        "complaint_type",
        "borough",
    }

    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns: {source_file.name}")

    silver = df.copy()

    silver["ServiceRequestID"] = (
        silver["unique_key"].fillna("").astype(str).str.strip()
    )

    silver["Agency"] = (
        silver["agency"].fillna("").astype(str).str.strip().str.upper()
    )

    silver["RequestStatus"] = (
        silver["status"].fillna("").astype(str).str.strip()
    )

    silver["ComplaintType"] = (
        silver["complaint_type"].fillna("").astype(str).str.strip()
    )

    silver["Borough"] = (
        silver["borough"].fillna("").astype(str).str.strip().str.upper()
    )

    silver["CreatedDate"] = pd.to_datetime(
        silver["created_date"].replace("", None),
        errors="coerce",
        format="mixed",
    )

    silver["ClosedDate"] = pd.to_datetime(
        silver["closed_date"].replace("", None),
        errors="coerce",
        format="mixed",
    )

    duration = (
        silver["ClosedDate"] - silver["CreatedDate"]
    ).dt.total_seconds() / 3600

    silver["NegativeResolutionDuration"] = duration.lt(0)

    silver["ResolutionHours"] = duration.where(
        duration.ge(0)
    ).astype("float64")

    total += len(silver)
    missing_ids += int(silver["ServiceRequestID"].eq("").sum())
    missing_created_dates += int(silver["CreatedDate"].isna().sum())
    missing_closed_dates += int(silver["ClosedDate"].isna().sum())
    negative_resolution += int(
        silver["NegativeResolutionDuration"].sum()
    )
    non_dpr += int(silver["Agency"].ne("DPR").sum())

    output = TARGET / source_file.name

    if output.exists():
        metadata = pq.read_metadata(output)

        if (
            metadata.num_rows != len(silver)
            or metadata.schema.names != list(silver.columns)
        ):
            raise ValueError(f"Existing output mismatch: {output.name}")

        verified += 1

    else:
        temp = TARGET / f"{source_file.stem}.parquet.tmp"

        if temp.exists():
            raise FileExistsError(
                f"Existing temporary file needs review: {temp.name}"
            )

        silver.to_parquet(temp, engine="pyarrow", index=False)

        metadata = pq.read_metadata(temp)

        if (
            metadata.num_rows != len(silver)
            or metadata.schema.names != list(silver.columns)
        ):
            raise ValueError(
                f"Silver output verification failed: {source_file.name}"
            )

        os.replace(temp, output)
        created += 1

    if number % 25 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total:,} records",
            flush=True,
        )

print("\n--- SILVER NYC 311 DPR ---")
print("Total records:", total)
print("New Silver files:", created)
print("Verified existing files:", verified)
print("Missing Service Request IDs:", missing_ids)
print("Missing created dates:", missing_created_dates)
print("Missing closed dates:", missing_closed_dates)
print("Negative resolution durations:", negative_resolution)
print("Non-DPR records:", non_dpr)

if total != EXPECTED_ROWS or missing_ids != 0 or non_dpr != 0:
    raise ValueError("Silver NYC 311 validation needs review.")

print("\nSILVER NYC 311 STRUCTURAL VALIDATION PASSED")
