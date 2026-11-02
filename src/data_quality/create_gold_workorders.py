
# Builds Gold Work Orders fact tables from validated Silver Work Orders.

from pathlib import Path
import os

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/silver/amps_workorders"
ASSETS = ROOT / "data/gold/dim_asset.parquet"
TARGET = ROOT / "data/gold/fact_workorders"

EXPECTED_BATCHES = 447
EXPECTED_ROWS = 2_231_535

files = sorted(SOURCE.glob("workorders_batch_*.parquet"))

if len(files) != EXPECTED_BATCHES:
    raise ValueError("Silver Work Orders batch count mismatch.")

assets = pd.read_parquet(ASSETS, columns=["AssetID"])
asset_ids = set(assets["AssetID"].astype(str))

TARGET.mkdir(parents=True, exist_ok=True)

total = 0
created = 0
verified = 0
unmatched = 0
completed = 0
negative_duration = 0
missing_reported = 0

for number, source_file in enumerate(files, start=1):

    expected_name = f"workorders_batch_{number:05d}.parquet"

    if source_file.name != expected_name:
        raise ValueError(f"Unexpected source file: {source_file.name}")

    df = pd.read_parquet(source_file)

    required = {
        "WorkOrderID",
        "AssetID",
        "WorkOrderType",
        "Priority",
        "evt_date_dt",
        "evt_reported_dt",
        "evt_start_dt",
        "evt_due_dt",
        "evt_completed_dt",
        "AssetReferenceValid",
        "ObjectTypeMismatch",
    }

    if not required.issubset(df.columns):
        raise ValueError(f"Required columns missing: {source_file.name}")

    gold = df[
        [
            "WorkOrderID",
            "AssetID",
            "WorkOrderType",
            "Priority",
            "evt_date_dt",
            "evt_reported_dt",
            "evt_start_dt",
            "evt_due_dt",
            "evt_completed_dt",
            "AssetReferenceValid",
            "ObjectTypeMismatch",
        ]
    ].copy()

    gold = gold.rename(
        columns={
            "evt_date_dt": "WorkOrderDate",
            "evt_reported_dt": "ReportedDate",
            "evt_start_dt": "StartDate",
            "evt_due_dt": "DueDate",
            "evt_completed_dt": "CompletedDate",
        }
    )

    gold["IsCompleted"] = gold["CompletedDate"].notna()

    duration = (
        gold["CompletedDate"] - gold["ReportedDate"]
    ).dt.total_seconds() / 3600

    gold["NegativeCompletionDuration"] = duration.lt(0)

    gold["CompletionHours"] = duration.where(
        duration.ge(0)
    ).astype("float64")

    invalid_assets = ~gold["AssetID"].isin(asset_ids)

    unmatched += int(invalid_assets.sum())
    completed += int(gold["IsCompleted"].sum())
    negative_duration += int(
        gold["NegativeCompletionDuration"].sum()
    )
    missing_reported += int(gold["ReportedDate"].isna().sum())

    if invalid_assets.any():
        raise ValueError(
            f"Unknown AssetID in {source_file.name}"
        )

    if gold["WorkOrderID"].eq("").any():
        raise ValueError(
            f"Missing WorkOrderID in {source_file.name}"
        )

    output = TARGET / f"fact_{source_file.name}"

    if output.exists():
        metadata = pq.read_metadata(output)

        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(
                f"Existing Gold output mismatch: {output.name}"
            )

        verified += 1

    else:
        temp = TARGET / f"fact_{source_file.stem}.parquet.tmp"

        if temp.exists():
            raise FileExistsError(
                f"Review temporary output: {temp.name}"
            )

        gold.to_parquet(
            temp,
            engine="pyarrow",
            index=False,
        )

        metadata = pq.read_metadata(temp)

        if (
            metadata.num_rows != len(gold)
            or metadata.schema.names != list(gold.columns)
        ):
            raise ValueError(
                f"Gold output verification failed: {output.name}"
            )

        os.replace(temp, output)
        created += 1

    total += len(gold)

    if number % 50 == 0 or number == len(files):
        print(
            f"Processed {number}/{len(files)} batches | "
            f"{total:,} records",
            flush=True,
        )

print("\n--- GOLD FACT WORK ORDERS ---")
print("Total Work Orders:", total)
print("New Gold files:", created)
print("Verified existing files:", verified)
print("Unmatched Asset IDs:", unmatched)
print("Work Orders with completion dates:", completed)
print("Negative completion durations:", negative_duration)
print("Missing reported dates:", missing_reported)

if (
    total != EXPECTED_ROWS
    or unmatched != 0
    or created + verified != EXPECTED_BATCHES
):
    raise ValueError("Gold Work Orders validation needs review.")

print("\nGOLD FACT WORK ORDERS STRUCTURAL VALIDATION PASSED")
