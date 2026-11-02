
# Validates integrated EAM metrics and creates a business KPI report.

from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/gold/asset_inspection_summary.parquet"
OUTPUT = ROOT / "docs/reports/eam_kpi_summary.json"

df = pd.read_parquet(SOURCE)

required = {
    "AssetID",
    "WorkOrderCount",
    "CompletedWorkOrders",
    "ValidDurationCount",
    "TotalCompletionHours",
    "ObjectTypeMismatchCount",
    "LaborEntries",
    "TotalLaborHours",
    "NetLaborCost",
    "NegativeCostEntries",
    "InspectionCount",
    "HasInspectionHistory",
}

if not required.issubset(df.columns):
    raise ValueError(
        f"Missing fields: {sorted(required - set(df.columns))}"
    )

if len(df) != 148_521:
    raise ValueError("Unexpected asset count.")

if df["AssetID"].duplicated().any():
    raise ValueError("Duplicate AssetIDs found.")

def total(column):
    return float(df[column].sum())

assets = len(df)
workorders = int(total("WorkOrderCount"))
completed = int(total("CompletedWorkOrders"))
valid_durations = int(total("ValidDurationCount"))
labor_entries = int(total("LaborEntries"))
inspections = int(total("InspectionCount"))
mismatches = int(total("ObjectTypeMismatchCount"))

if workorders != 2_231_535:
    raise ValueError("Work Order totals do not reconcile.")

if labor_entries != 3_217_345:
    raise ValueError("Matched Labor totals do not reconcile.")

if inspections != 148_235:
    raise ValueError("Inspection totals do not reconcile.")

if mismatches != 937:
    raise ValueError("Object-type mismatch totals do not reconcile.")

average_completion_hours = (
    total("TotalCompletionHours") / valid_durations
    if valid_durations else None
)

report = {
    "total_assets": assets,
    "assets_with_workorders": int(
        df["WorkOrderCount"].gt(0).sum()
    ),
    "total_workorders": workorders,
    "workorders_with_completion_date": completed,
    "workorders_with_valid_duration": valid_durations,
    "average_valid_completion_hours": average_completion_hours,
    "workorders_missing_valid_duration": workorders - valid_durations,
    "matched_labor_entries": labor_entries,
    "unmatched_labor_entries": 13_078,
    "matched_labor_hours": total("TotalLaborHours"),
    "net_matched_labor_cost": total("NetLaborCost"),
    "matched_negative_cost_entries": int(
        total("NegativeCostEntries")
    ),
    "assets_with_labor": int(
        df["LaborEntries"].gt(0).sum()
    ),
    "assets_with_inspections": int(
        df["HasInspectionHistory"].sum()
    ),
    "matched_inspections": inspections,
    "unmatched_inspections": 4_858,
    "object_type_mismatches": mismatches,
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

content = json.dumps(report, indent=2)

if OUTPUT.exists():
    existing = json.loads(OUTPUT.read_text(encoding="utf-8"))
    if existing != report:
        raise ValueError(
            "Existing KPI report differs. Review before replacing it."
        )
    print("Existing KPI report verified.")
else:
    OUTPUT.write_text(content, encoding="utf-8")
    print("KPI report created.")

print("\n--- INTEGRATED EAM KPI SUMMARY ---")

for key, value in report.items():
    if isinstance(value, float):
        print(f"{key}: {value:,.2f}")
    else:
        print(f"{key}: {value:,}")

print("\nOutput:", OUTPUT)
print("INTEGRATED EAM KPI VALIDATION PASSED")
