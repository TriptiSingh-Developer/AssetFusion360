
# Checks whether NYC Parks inspection property IDs match AMPS asset identifiers.

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

assets = pd.read_csv(
    ROOT / "data/raw/nyc_amps/AMPS_Assets.csv",
    dtype=str,
    low_memory=False,
)

inspections = pd.read_csv(
    ROOT / "data/raw/supplementary/Parks_Inspections.csv",
    dtype=str,
    low_memory=False,
)

asset_ids = (
    assets["OBJ_CODE"]
    .dropna()
    .str.strip()
    .str.upper()
)

property_ids = (
    inspections["Prop ID"]
    .dropna()
    .str.strip()
    .str.upper()
)

unique_assets = set(asset_ids)
unique_properties = set(property_ids)

matching_properties = unique_properties & unique_assets
unmatched_properties = unique_properties - unique_assets

print("Unique AMPS asset IDs:", len(unique_assets))
print("Unique inspection property IDs:", len(unique_properties))
print("Matching property IDs:", len(matching_properties))
print("Unmatched property IDs:", len(unmatched_properties))

print(
    "Property match percentage:",
    round(
        len(matching_properties) / len(unique_properties) * 100,
        2,
    ),
)

print("\nSample matching IDs:")
print(sorted(matching_properties)[:10])

print("\nSample unmatched IDs:")
print(sorted(unmatched_properties)[:10])

inspection_matches = property_ids.isin(unique_assets)

print(
    "\nInspection records with matching IDs:",
    int(inspection_matches.sum()),
)
print(
    "Inspection record match percentage:",
    round(inspection_matches.mean() * 100, 2),
)
