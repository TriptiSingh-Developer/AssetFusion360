
# Validates AMPS Work Orders and Assets relationships and saves an audit report.

from pathlib import Path
from collections import Counter
from datetime import datetime, timezone
import json

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]

ASSETS_FILE = ROOT / "data/raw/nyc_amps/AMPS_Assets.csv"
BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"
REPORT_DIR = ROOT / "docs/methodology"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = REPORT_DIR / "workorders_assets_relationship_audit.json"

EXPECTED_WORKORDERS = 2_231_535
EXPECTED_BATCHES = 447


def normalize(series):
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )


# Load and validate the Assets dataset.
print("Loading AMPS Assets...", flush=True)

assets = pd.read_csv(
    ASSETS_FILE,
    dtype=str,
    low_memory=False
)

required_assets = {
    "OBJ_CODE",
    "OBJ_STATUS",
    "OBJ_OBTYPE",
    "OBJ_SQLIDENTITY",
}

if not required_assets.issubset(assets.columns):
    raise ValueError("Required Assets columns are missing.")

assets["asset_key"] = normalize(assets["OBJ_CODE"])

missing_asset_ids = int(
    (assets["asset_key"] == "").sum()
)

valid_assets = assets[
    assets["asset_key"] != ""
].copy()

duplicate_assets = valid_assets[
    valid_assets["asset_key"].duplicated(keep=False)
].copy()

# Prefer status I for duplicate asset identifiers.
valid_assets["_preferred"] = (
    normalize(valid_assets["OBJ_STATUS"]) == "I"
).astype(int)

canonical_assets = (
    valid_assets
    .sort_values(
        "_preferred",
        ascending=False,
        kind="stable"
    )
    .drop_duplicates("asset_key", keep="first")
)

asset_lookup = canonical_assets.set_index(
    "asset_key"
)["OBJ_OBTYPE"]

asset_keys = set(canonical_assets["asset_key"])

print("Raw Assets:", len(assets))
print("Canonical Assets:", len(canonical_assets))
print(
    "Duplicate asset identifiers:",
    duplicate_assets["asset_key"].unique().tolist()
)


# Identify Work Orders batch files.
files = sorted(
    BATCH_DIR.glob("workorders_batch_*.csv")
)

if not files:
    raise FileNotFoundError(
        "No Work Orders batches found."
    )

print("Work Orders batches:", len(files), flush=True)


# Initialize audit metrics.
total = 0

missing_wo_id = 0
duplicate_wo_id = 0

missing_asset_reference = 0
matched_asset_reference = 0
unmatched_asset_reference = 0

type_match = 0
type_mismatch = 0
type_missing = 0

seen_workorders = set()

unmatched_examples = Counter()
type_mismatch_examples = Counter()


# Validate each Work Orders batch.
for number, file in enumerate(files, start=1):

    expected_name = (
        f"workorders_batch_{number:05d}.csv"
    )

    if file.name != expected_name:
        raise ValueError(
            f"Unexpected batch filename: {file.name}"
        )

    df = pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        low_memory=False
    )

    required_columns = {
        "evt_code",
        "evt_object",
        "evt_obtype",
    }

    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"Required columns missing in {file.name}"
        )

    total += len(df)

    # Check Work Order identifiers.
    wo_ids = normalize(df["evt_code"])

    missing_wo_id += int(
        (wo_ids == "").sum()
    )

    for identifier in wo_ids[wo_ids != ""]:

        if identifier in seen_workorders:
            duplicate_wo_id += 1
        else:
            seen_workorders.add(identifier)

    # Validate asset references.
    keys = normalize(df["evt_object"])

    workorder_types = normalize(df["evt_obtype"])

    missing_mask = keys == ""

    matched_mask = (
        keys.isin(asset_keys) & ~missing_mask
    )

    unmatched_mask = (
        ~matched_mask & ~missing_mask
    )

    missing_asset_reference += int(
        missing_mask.sum()
    )

    matched_asset_reference += int(
        matched_mask.sum()
    )

    unmatched_asset_reference += int(
        unmatched_mask.sum()
    )

    unmatched_examples.update(
        keys[unmatched_mask].value_counts().to_dict()
    )

    # Compare object types for linked records.
    linked_types = normalize(
        keys[matched_mask].map(asset_lookup)
    )

    wo_types = workorder_types[matched_mask]

    both_present = (
        (linked_types != "") &
        (wo_types != "")
    )

    type_match += int(
        (
            both_present &
            (linked_types == wo_types)
        ).sum()
    )

    type_mismatch += int(
        (
            both_present &
            (linked_types != wo_types)
        ).sum()
    )

    type_missing += int(
        (~both_present).sum()
    )

    mismatches = pd.DataFrame({
        "workorder_type": wo_types[both_present].to_numpy(),
        "asset_type": linked_types[both_present].to_numpy(),
    })

    mismatches = mismatches[
        mismatches["workorder_type"] !=
        mismatches["asset_type"]
    ]

    type_mismatch_examples.update(
        Counter(
            zip(
                mismatches["workorder_type"],
                mismatches["asset_type"]
            )
        )
    )

    if number % 50 == 0 or number == len(files):
        print(
            f"Checked {number}/{len(files)} batches "
            f"| {total:,} Work Orders",
            flush=True
        )


# Build the audit report.
match_rate = (
    round(
        matched_asset_reference / total * 100,
        2
    )
    if total else 0
)

report = {
    "generated_at_utc": datetime.now(
        timezone.utc
    ).isoformat(),

    "assets": {
        "raw_rows": len(assets),
        "canonical_rows": len(canonical_assets),
        "missing_asset_ids": missing_asset_ids,
        "duplicate_asset_ids": sorted(
            duplicate_assets["asset_key"].unique().tolist()
        ),
    },

    "workorders": {
        "batch_files": len(files),
        "total_rows": total,
        "expected_rows": EXPECTED_WORKORDERS,
        "missing_workorder_ids": missing_wo_id,
        "duplicate_workorder_ids": duplicate_wo_id,
        "missing_asset_references": missing_asset_reference,
        "matched_asset_references": matched_asset_reference,
        "unmatched_asset_references": unmatched_asset_reference,
        "asset_match_rate_percent": match_rate,
    },

    "object_type_comparison": {
        "matching_type_codes": type_match,
        "mismatching_type_codes": type_mismatch,
        "missing_type_codes": type_missing,
        "top_mismatching_type_pairs": [
            {
                "workorder_type": pair[0],
                "asset_type": pair[1],
                "count": count,
            }
            for pair, count
            in type_mismatch_examples.most_common(10)
        ],
    },

    "top_unmatched_asset_references": [
        {
            "asset_id": key,
            "count": count,
        }
        for key, count
        in unmatched_examples.most_common(20)
    ],
}


with REPORT_FILE.open(
    "w", encoding="utf-8"
) as handle:
    json.dump(report, handle, indent=2)


# Display results.
print("\n--- RELATIONSHIP AUDIT RESULTS ---")

print("Total Work Orders:", total)
print("Work Orders batches:", len(files))
print("Raw Assets:", len(assets))
print("Canonical Assets:", len(canonical_assets))

print(
    "Missing Work Order IDs:",
    missing_wo_id
)

print(
    "Duplicate Work Order IDs:",
    duplicate_wo_id
)

print(
    "Matched asset references:",
    matched_asset_reference
)

print(
    "Unmatched asset references:",
    unmatched_asset_reference
)

print(
    "Missing asset references:",
    missing_asset_reference
)

print(
    "Asset match rate:",
    match_rate,
    "%"
)

print(
    "Matching object types:",
    type_match
)

print(
    "Mismatching object types:",
    type_mismatch
)

print(
    "Missing object types:",
    type_missing
)

print(
    "Top unmatched asset references:",
    unmatched_examples.most_common(10)
)

print("Report saved:", REPORT_FILE)


passed = (
    len(files) == EXPECTED_BATCHES
    and total == EXPECTED_WORKORDERS
    and missing_wo_id == 0
    and duplicate_wo_id == 0
    and (
        matched_asset_reference
        + unmatched_asset_reference
        + missing_asset_reference
    ) == total
    and len(canonical_assets) == len(asset_keys)
)

print(
    "\nSTRUCTURAL AUDIT PASSED"
    if passed
    else "\nSTRUCTURAL AUDIT NEEDS REVIEW"
)
