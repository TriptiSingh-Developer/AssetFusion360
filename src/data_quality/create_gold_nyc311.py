
# Creates Gold NYC 311 service-request facts with validated resolution metrics.

from pathlib import Path
import os

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/silver/nyc311_dpr"
TARGET = ROOT / "data/gold/fact_service_requests"

EXPECTED_BATCHES = 176
EXPECTED_ROWS = 875_134

files = sorted(SOURCE.glob("dpr_batch_*.parquet"))

if len(files) != EXPECTED_BATCHES:
    raise ValueError("Unexpected Silver NYC 311 batch count.")

TARGET.mkdir(parents=True, exist_ok=True)

total = 0
created = 0
verified = 0
missing_ids = 0
missing_created = 0
missing_closed = 0
negative_durations = 0
valid_durations = 0
non_dpr = 0

required = {
    "ServiceRequestID",
    "Agency",
    "RequestStatus",
    "ComplaintType",
    "Borough",
    "CreatedDate",
    "ClosedDate",
    "ResolutionHours",
    "NegativeResolutionDuration",
}

for number, source_file in enumerate(files, start=1):

    expected_name = f"dpr_batch_{number:05d}.parquet"

    if source_file.name != expected_name:
        raise ValueError(f"Unexpected filename: {source_file.name}")

    df = pd.read_parquet(source_file)

    if not required.issubset(df.columns):
        raise ValueError(f"Missing columns: {source_file.name}")

    gold = df[
        [
            "ServiceRequestID",
            "Agency",
            "RequestStatus",
            "ComplaintType",
            "Borough",
            "CreatedDate",
            "ClosedDate",
            "ResolutionHours",
            "NegativeResolutionDuration",
        ]
    ].copy()

    gold["ServiceRequestID"] = (
        gold["ServiceRequestID"].astype("string").str.strip()
    )

    for column in [
        "Agency",
        "RequestStatus",
        "ComplaintType",
        "Borough",
    ]:
        gold[column] = gold[column].astype("string").str.strip()

    gold["IsClosedDatePresent"] = gold["ClosedDate"].notna()

    gold["HasValidResolutionDuration"] = (
        gold["ResolutionHours"].notna()
        & ~gold["NegativeResolutionDuration"].fillna(False)
    )

    gold["ResolutionDays"] = (
        gold["ResolutionHours"] / 24.0
    )

    total += len(gold)
    missing_ids += int(
        gold["ServiceRequestID"].isna().sum()
        + gold["ServiceRequestID"].eq("").sum()
    )
    missing_created += int(gold["CreatedDate"].isna().sum())
    missing_closed += int(gold["ClosedDate"].isna().sum())
    negative_durations += int(
        gold["NegativeResolutionDuration"].fillna(False).sum()
    )
    valid_durations += int(gold["HasValidResolutionDuration"].sum())
    non_dpr += int(gold["Agency"].ne("DPR").fillna(True).sum())

    output = TARGET / f"fact_{source_file.name}"

    if output.exists():
        metadata = pq.read_metadata(output)

        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(f"Existing Gold file mismatch: {output.name}")

        verified += 1

    else:
        temp = TARGET / f"fact_{source_file.stem}.parquet.tmp"

        if temp.exists():
            raise FileExistsError(
                f"Temporary file needs review: {temp.name}"
            )

        gold.to_parquet(temp, engine="pyarrow", index=False)

        metadata = pq.read_metadata(temp)

        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(f"Gold verification failed: {output.name}")

        os.replace(temp, output)
        created += 1

    if number % 25 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total:,} records",
            flush=True,
        )

print("\n--- GOLD FACT SERVICE REQUESTS ---")
print("Total requests:", total)
print("New Gold files:", created)
print("Verified existing files:", verified)
print("Missing request IDs:", missing_ids)
print("Missing created dates:", missing_created)
print("Missing closed dates:", missing_closed)
print("Negative resolution durations:", negative_durations)
print("Valid resolution durations:", valid_durations)
print("Non-DPR records:", non_dpr)

if (
    total != EXPECTED_ROWS
    or missing_ids != 0
    or missing_created != 0
    or non_dpr != 0
    or created + verified != EXPECTED_BATCHES
):
    raise ValueError("Gold Service Requests validation needs review.")

if valid_durations + negative_durations + missing_closed != total:
    raise ValueError(
        "Resolution duration classification needs review."
    )

print("\nGOLD SERVICE REQUESTS STRUCTURAL VALIDATION PASSED")
