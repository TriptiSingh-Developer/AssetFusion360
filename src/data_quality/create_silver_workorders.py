
# Creates and validates Silver Work Orders with standardized fields and asset relationships.

from pathlib import Path
import os

import pandas as pd
from pandas.testing import assert_frame_equal


ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/amps_workorders"
ASSETS = ROOT / "data/silver/amps_assets.parquet"
TARGET = ROOT / "data/silver/amps_workorders"

EXPECTED_ROWS = 2_231_535
EXPECTED_BATCHES = 447
EXPECTED_MISMATCHES = 937

DATE_COLUMNS = [
    "evt_date",
    "evt_reported",
    "evt_start",
    "evt_due",
    "evt_completed",
    "evt_created",
    "evt_updated",
]


def normalize(series):
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )


def validate_saved(saved, expected):
    # Normalize equivalent missing-value representations before comparison.
    saved = saved.copy()
    expected = expected.copy()

    for column in ["WorkOrderType", "Priority"]:
        saved[column] = saved[column].astype("string")
        expected[column] = expected[column].astype("string")

    assert_frame_equal(
        saved,
        expected,
        check_dtype=False,
        check_exact=True,
        check_freq=False,
    )


def main():
    files = sorted(SOURCE.glob("workorders_batch_*.parquet"))

    if len(files) != EXPECTED_BATCHES:
        raise ValueError(
            f"Expected {EXPECTED_BATCHES} Bronze batches, "
            f"found {len(files)}."
        )

    assets = pd.read_parquet(
        ASSETS,
        columns=["AssetID", "OBJ_OBTYPE"],
    )

    if assets["AssetID"].duplicated().any():
        raise ValueError("Silver Assets contains duplicate AssetIDs.")

    asset_types = assets.set_index("AssetID")["OBJ_OBTYPE"]
    asset_keys = set(asset_types.index)

    TARGET.mkdir(parents=True, exist_ok=True)

    total = 0
    mismatches = 0
    unmatched = 0
    missing_ids = 0
    created = 0
    verified = 0

    for number, source_file in enumerate(files, start=1):
        expected_name = f"workorders_batch_{number:05d}.parquet"

        if source_file.name != expected_name:
            raise ValueError(
                f"Unexpected batch filename: {source_file.name}"
            )

        df = pd.read_parquet(source_file)
        silver = df.copy()

        required = {
            "evt_code",
            "evt_object",
            "evt_obtype",
            "evt_type",
            "evt_priority",
            *DATE_COLUMNS,
        }

        if not required.issubset(silver.columns):
            raise ValueError(
                f"Required columns missing: {source_file.name}"
            )

        silver["WorkOrderID"] = silver["evt_code"].astype(str).str.strip()
        silver["AssetID"] = normalize(silver["evt_object"])

        # Nullable strings preserve missing values consistently in Parquet.
        silver["WorkOrderType"] = (
            silver["evt_type"]
            .replace("", pd.NA)
            .astype("string")
        )

        silver["Priority"] = (
            silver["evt_priority"]
            .replace("", pd.NA)
            .astype("string")
        )

        for column in DATE_COLUMNS:
            silver[column + "_dt"] = pd.to_datetime(
                silver[column].replace("", pd.NA),
                errors="coerce",
                format="mixed",
            )

        actual_types = normalize(silver["evt_obtype"])
        expected_types = normalize(
            silver["AssetID"].map(asset_types)
        )

        silver["AssetReferenceValid"] = silver["AssetID"].isin(
            asset_keys
        )

        silver["ObjectTypeMismatch"] = (
            silver["AssetReferenceValid"]
            & actual_types.ne(expected_types)
        )

        missing_ids += int(silver["WorkOrderID"].eq("").sum())
        unmatched += int((~silver["AssetReferenceValid"]).sum())
        mismatches += int(silver["ObjectTypeMismatch"].sum())
        total += len(silver)

        output_file = TARGET / source_file.name

        if output_file.exists():
            saved = pd.read_parquet(output_file)
            validate_saved(saved, silver)
            verified += 1

        else:
            temp = TARGET / f"{source_file.stem}.parquet.tmp"

            # Only a prior incomplete temporary output may be replaced.
            # Never overwrite an existing completed Silver Parquet file.
            if temp.exists():
                temp.unlink()

            silver.to_parquet(
                temp,
                engine="pyarrow",
                index=False,
            )

            saved = pd.read_parquet(temp)
            validate_saved(saved, silver)

            os.replace(temp, output_file)
            created += 1

        if number % 50 == 0 or number == len(files):
            print(
                f"Processed {number}/{len(files)} batches | "
                f"{total:,} records",
                flush=True,
            )

    print("\n--- SILVER WORK ORDERS ---")
    print("Total records:", total)
    print("New Silver files:", created)
    print("Verified existing files:", verified)
    print("Missing Work Order IDs:", missing_ids)
    print("Unmatched asset references:", unmatched)
    print("Object-type mismatches:", mismatches)

    if (
        total != EXPECTED_ROWS
        or missing_ids != 0
        or unmatched != 0
        or mismatches != EXPECTED_MISMATCHES
        or created + verified != EXPECTED_BATCHES
    ):
        raise ValueError("Silver Work Orders validation needs review.")

    print("\nSILVER WORK ORDERS VALIDATION PASSED")


if __name__ == "__main__":
    main()
