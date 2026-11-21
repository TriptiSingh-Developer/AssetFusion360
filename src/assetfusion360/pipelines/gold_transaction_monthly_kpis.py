"""
AssetFusion360 - Gold Monthly Transaction KPIs

Source: fact_transactions
Grain: reporting month x transaction type x transaction status

Preserves missing reporting dates and category values.
Runs through Kedro and publishes with staging safeguards.
"""

import gc
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "data" / "04_gold_analytics"

SOURCE_DIR = GOLD / "fact_transactions"
FINAL_DIR = GOLD / "gold_transaction_monthly_kpis"
STAGING_DIR = GOLD / "_staging_gold_transaction_monthly_kpis"

STAGING_FILE = STAGING_DIR / "part-00000.parquet"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_transaction_monthly_kpis_manifest.json"
)

EXPECTED_ROWS = 755_122
EXPECTED_FILES = 152

SOURCE_FIELDS = [
    "transaction_date",
    "transaction_type",
    "transaction_status",
    "dq_transaction_date_invalid",
    "silver_dq_issue",
]

SCHEMA = pa.schema([
    pa.field("reporting_month", pa.string(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("transaction_status", pa.string(), nullable=False),
    pa.field("transaction_count", pa.int64(), nullable=False),
    pa.field("missing_date_count", pa.int64(), nullable=False),
    pa.field("invalid_date_flag_count", pa.int64(), nullable=False),
    pa.field("silver_dq_issue_count", pa.int64(), nullable=False),
])


def check_outputs():
    assert SOURCE_DIR.is_dir(), "Missing transaction Gold source"

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output must be preserved: {path}"
            )

    print("Output safety: PASS")


def inspect_source():
    files = sorted(SOURCE_DIR.glob("*.parquet"))
    assert len(files) == EXPECTED_FILES

    reference = pq.read_schema(files[0])
    total = 0

    for path in files:
        assert pq.read_schema(path).equals(
            reference, check_metadata=False
        ), f"Schema mismatch: {path.name}"

        total += pq.read_metadata(path).num_rows

    assert total == EXPECTED_ROWS

    for field in SOURCE_FIELDS:
        assert field in reference.names, (
            f"Missing required field: {field}"
        )

    assert pa.types.is_timestamp(
        reference.field("transaction_date").type
    )

    assert pa.types.is_boolean(
        reference.field("dq_transaction_date_invalid").type
    )

    assert pa.types.is_boolean(
        reference.field("silver_dq_issue").type
    )

    print("Source files:", len(files))
    print("Source transactions:", f"{total:,}")
    print("Source schema: PASS")

    return files


def aggregate_transactions(files):
    groups = defaultdict(Counter)
    totals = Counter()
    valid_months = set()

    for index, path in enumerate(files, 1):
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=10000,
                columns=SOURCE_FIELDS,
            ):
                data = batch.to_pydict()

                for i in range(batch.num_rows):
                    date = data["transaction_date"][i]
                    kind = data["transaction_type"][i]
                    status = data["transaction_status"][i]

                    if date is None:
                        month = "<MISSING_DATE>"
                    else:
                        month = date.strftime("%Y-%m")
                        valid_months.add(month)

                    if kind is None:
                        kind = "<MISSING_TYPE>"

                    if status is None:
                        status = "<MISSING_STATUS>"

                    key = (month, kind, status)
                    group = groups[key]

                    missing_date = int(date is None)
                    invalid_date = int(
                        data["dq_transaction_date_invalid"][i]
                        is True
                    )
                    dq_issue = int(
                        data["silver_dq_issue"][i] is True
                    )

                    group["transaction_count"] += 1
                    group["missing_date_count"] += missing_date
                    group["invalid_date_flag_count"] += invalid_date
                    group["silver_dq_issue_count"] += dq_issue

                    totals["rows"] += 1
                    totals["missing_date"] += missing_date
                    totals["invalid_date"] += invalid_date
                    totals["silver_dq"] += dq_issue

        if index % 25 == 0 or index == len(files):
            print(
                f"Processed: {index}/{len(files)} files"
                f" | {totals['rows']:,} transactions",
                flush=True,
            )

    assert totals["rows"] == EXPECTED_ROWS

    print("\nSOURCE RECONCILIATION")
    print("Transactions:", f"{totals['rows']:,}")
    print("Monthly/type/status groups:", len(groups))
    print("Valid reporting months:", len(valid_months))
    print("Missing dates:", totals["missing_date"])
    print("Invalid date flags:", totals["invalid_date"])
    print("Silver DQ issues:", totals["silver_dq"])
    print("Source reconciliation: PASS")

    return groups, totals, valid_months


def build_gold_transaction_monthly_kpis():
    print("=" * 68)
    print("STEP 7.14 - BUILD MONTHLY TRANSACTION KPIS")
    print("=" * 68)

    check_outputs()
    files = inspect_source()

    groups, totals, valid_months = (
        aggregate_transactions(files)
    )

    records = []

    for key in sorted(groups):
        month, kind, status = key
        metrics = groups[key]

        record = {
            "reporting_month": month,
            "transaction_type": kind,
            "transaction_status": status,
        }

        for field in SCHEMA.names[3:]:
            record[field] = metrics[field]

        assert (
            record["missing_date_count"]
            <= record["transaction_count"]
        )
        assert (
            record["invalid_date_flag_count"]
            <= record["transaction_count"]
        )
        assert (
            record["silver_dq_issue_count"]
            <= record["transaction_count"]
        )

        records.append(record)

    assert sum(
        row["transaction_count"] for row in records
    ) == EXPECTED_ROWS

    # All source checks complete before creating staging.
    STAGING_DIR.mkdir(parents=True, exist_ok=False)

    table = pa.Table.from_pylist(records, schema=SCHEMA)

    pq.write_table(
        table,
        STAGING_FILE,
        compression="snappy",
    )

    del table
    gc.collect()

    # Independently verify staged output.
    with pq.ParquetFile(str(STAGING_FILE)) as reader:
        assert reader.metadata.num_rows == len(records)
        assert reader.schema_arrow.equals(
            SCHEMA, check_metadata=False
        )
        staged = reader.read().to_pylist()

    assert staged == records

    assert sum(
        r["transaction_count"] for r in staged
    ) == EXPECTED_ROWS

    assert sum(
        r["missing_date_count"] for r in staged
    ) == totals["missing_date"]

    assert sum(
        r["invalid_date_flag_count"] for r in staged
    ) == totals["invalid_date"]

    assert sum(
        r["silver_dq_issue_count"] for r in staged
    ) == totals["silver_dq"]

    print("\nSTAGED TRANSACTION KPI VALIDATION")
    print("KPI rows:", len(staged))
    print("Source transactions:", f"{EXPECTED_ROWS:,}")
    print("Staged validation: PASS")

    # Publish only after all staged checks pass.
    if FINAL_DIR.exists():
        raise FileExistsError(FINAL_DIR)

    STAGING_DIR.rename(FINAL_DIR)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold Analytics",
        "table": "gold_transaction_monthly_kpis",
        "status": "COMPLETED",
        "source": SOURCE_DIR.relative_to(ROOT).as_posix(),
        "output": (
            FINAL_DIR / "part-00000.parquet"
        ).relative_to(ROOT).as_posix(),
        "rows": len(staged),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "source_transaction_rows": EXPECTED_ROWS,
        "reporting_months": len(valid_months),
        "first_month": (
            min(valid_months) if valid_months else None
        ),
        "last_month": (
            max(valid_months) if valid_months else None
        ),
        "missing_date_rows": totals["missing_date"],
        "invalid_date_flag_rows": totals["invalid_date"],
        "silver_dq_issue_rows": totals["silver_dq"],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST.open("x", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("\nGOLD TRANSACTION MONTHLY KPIS: SUCCESS")
    return manifest


def create_gold_transaction_monthly_kpis_pipeline():
    return pipeline([
        node(
            func=build_gold_transaction_monthly_kpis,
            inputs=None,
            outputs="gold_transaction_monthly_manifest",
            name="build_gold_transaction_monthly_kpis",
            tags=["gold", "analytics", "transaction", "kpi"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_transaction_monthly_manifest": MemoryDataset()
    })

    SequentialRunner().run(
        create_gold_transaction_monthly_kpis_pipeline(),
        catalog,
    )

    result = catalog.load(
        "gold_transaction_monthly_manifest"
    )

    assert result["status"] == "COMPLETED"

    print("KEDRO TRANSACTION KPI PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
