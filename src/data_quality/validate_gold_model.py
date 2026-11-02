
# Validates Gold table schemas, record counts, and file completeness.

from pathlib import Path
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data/gold"

TABLES = {
    "dim_asset": {
        "path": "dim_asset.parquet",
        "rows": 148521,
        "files": 1,
        "required": ["AssetID", "AssetName", "AssetClass"],
    },
    "fact_workorders": {
        "path": "fact_workorders/fact_workorders_batch_*.parquet",
        "rows": 2231535,
        "files": 447,
        "required": ["WorkOrderID", "AssetID", "CompletionHours"],
    },
    "fact_labor": {
        "path": "fact_labor/fact_labor_batch_*.parquet",
        "rows": 3230423,
        "files": 647,
        "required": ["LaborID", "WorkOrderID", "LaborCost"],
    },
    "fact_transactions": {
        "path": "fact_transactions/fact_transactions_batch_*.parquet",
        "rows": 755122,
        "files": 152,
        "required": ["TransactionID", "WorkOrderReferenceValid"],
    },
    "fact_inspections": {
        "path": "fact_inspections.parquet",
        "rows": 153093,
        "files": 1,
        "required": ["InspectionID", "AssetID", "OverallCondition"],
    },
    "fact_service_requests": {
        "path": "fact_service_requests/fact_dpr_batch_*.parquet",
        "rows": 875134,
        "files": 176,
        "required": ["ServiceRequestID", "ResolutionHours"],
    },
    "assetguard_ml": {
        "path": "assetguard_ml.parquet",
        "rows": 10000,
        "files": 1,
        "required": ["MachineID", "MachineFailure"],
    },
}

errors = []
grand_total = 0

print("\n--- GOLD DATA MODEL AUDIT ---")

for name, config in TABLES.items():
    paths = sorted(GOLD.glob(config["path"]))
    rows = 0

    if len(paths) != config["files"]:
        errors.append(f"{name}: incorrect file count")

    expected_schema = None

    for path in paths:
        try:
            metadata = pq.read_metadata(path)
            rows += metadata.num_rows

            columns = metadata.schema.names

            if not set(config["required"]).issubset(columns):
                errors.append(f"{name}: missing required columns")

            if expected_schema is None:
                expected_schema = columns
            elif columns != expected_schema:
                errors.append(f"{name}: inconsistent batch schema")

        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    if rows != config["rows"]:
        errors.append(f"{name}: expected {config['rows']}, found {rows}")

    grand_total += rows

    print(
        f"{name}: {rows:,} rows | "
        f"{len(paths)} files | "
        f"{'OK' if rows == config['rows'] and len(paths) == config['files'] else 'REVIEW'}"
    )

print("\nTotal Gold rows:", f"{grand_total:,}")
print("Errors:", errors)

if errors:
    raise ValueError("GOLD DATA MODEL AUDIT NEEDS REVIEW")

print("\nGOLD STRUCTURAL AUDIT PASSED")
