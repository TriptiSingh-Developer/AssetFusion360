"""
AssetFusion360 Capstone
Step 7.13 - Gold Monthly Labor KPIs

Source: Gold fact_labor
Grain: Labor month x work-order match status

Preserves signed labor hours and costs.
Unmatched labor records remain included.
No existing data is overwritten.
"""

import gc
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


ROOT = Path(__file__).resolve().parents[3]

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

SOURCE_DIR = GOLD_ROOT / "fact_labor"
FINAL_DIR = GOLD_ROOT / "gold_labor_monthly_kpis"
STAGING_DIR = GOLD_ROOT / "_staging_gold_labor_monthly_kpis"

FINAL_FILE = FINAL_DIR / "part-00000.parquet"
STAGING_FILE = STAGING_DIR / "part-00000.parquet"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_labor_monthly_kpis_manifest.json"
)

EXPECTED_ROWS = 3_230_423
EXPECTED_FILES = 647
BATCH_SIZE = 10000

ZERO = Decimal("0")

SOURCE_FIELDS = [
    "labor_date",
    "labor_hours",
    "labor_cost",
    "labor_rate",
    "workorder_match_status",
    "negative_hours_flag",
    "negative_cost_flag",
    "zero_rate_flag",
]

GOLD_SCHEMA = pa.schema([
    pa.field("reporting_month", pa.string(), nullable=False),
    pa.field("workorder_match_status", pa.string(), nullable=False),
    pa.field("labor_record_count", pa.int64()),
    pa.field("total_labor_hours", pa.decimal128(30, 6)),
    pa.field("total_labor_cost", pa.decimal128(30, 6)),
    pa.field("negative_hours_count", pa.int64()),
    pa.field("negative_cost_count", pa.int64()),
    pa.field("zero_rate_count", pa.int64()),
])


def check_outputs():
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(SOURCE_DIR)

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output must be preserved: {path}"
            )

    print("Output safety: PASS")


def inspect_source():
    files = sorted(SOURCE_DIR.glob("*.parquet"))

    assert len(files) == EXPECTED_FILES

    schema = pq.read_schema(files[0])
    rows = 0

    for path in files:
        assert pq.read_schema(path).equals(
            schema,
            check_metadata=False,
        ), f"Schema mismatch: {path.name}"

        rows += pq.read_metadata(path).num_rows

    assert rows == EXPECTED_ROWS

    expected_types = {
        "labor_date": pa.timestamp("us"),
        "labor_hours": pa.decimal128(20, 6),
        "labor_cost": pa.decimal128(20, 6),
        "labor_rate": pa.decimal128(20, 6),
        "workorder_match_status": pa.string(),
        "negative_hours_flag": pa.bool_(),
        "negative_cost_flag": pa.bool_(),
        "zero_rate_flag": pa.bool_(),
    }

    for field, expected_type in expected_types.items():
        assert field in schema.names
        assert schema.field(field).type.equals(
            expected_type
        ), f"Unexpected type: {field}"

    print("Source files:", len(files))
    print("Source Labor rows:", f"{rows:,}")
    print("Source schema: PASS")

    return files


def aggregate_labor(files):
    groups = defaultdict(Counter)
    signed_totals = defaultdict(
        lambda: {"hours": ZERO, "cost": ZERO}
    )

    counts = Counter()
    months = set()

    for index, path in enumerate(files, 1):
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=BATCH_SIZE,
                columns=SOURCE_FIELDS,
            ):
                data = batch.to_pydict()

                for i in range(batch.num_rows):
                    date = data["labor_date"][i]
                    status = data[
                        "workorder_match_status"
                    ][i]
                    hours = data["labor_hours"][i]
                    cost = data["labor_cost"][i]
                    rate = data["labor_rate"][i]

                    if date is None:
                        raise ValueError(
                            "Missing labor reporting date"
                        )

                    assert status in {
                        "MATCHED",
                        "UNMATCHED",
                        "MISSING_REFERENCE",
                    }

                    assert hours is not None
                    assert cost is not None
                    assert rate is not None

                    month = date.strftime("%Y-%m")
                    key = (month, status)
                    months.add(month)

                    group = groups[key]
                    group["labor_record_count"] += 1

                    signed_totals[key]["hours"] += hours
                    signed_totals[key]["cost"] += cost

                    negative_hours = hours < 0
                    negative_cost = cost < 0
                    zero_rate = rate == 0

                    # Independently verify Silver-derived flags.
                    assert (
                        data["negative_hours_flag"][i]
                        == negative_hours
                    )
                    assert (
                        data["negative_cost_flag"][i]
                        == negative_cost
                    )
                    assert (
                        data["zero_rate_flag"][i]
                        == zero_rate
                    )

                    group["negative_hours_count"] += int(
                        negative_hours
                    )
                    group["negative_cost_count"] += int(
                        negative_cost
                    )
                    group["zero_rate_count"] += int(
                        zero_rate
                    )

                    counts["rows"] += 1
                    counts[status] += 1
                    counts["negative_hours"] += int(
                        negative_hours
                    )
                    counts["negative_cost"] += int(
                        negative_cost
                    )
                    counts["zero_rate"] += int(zero_rate)

        if index % 50 == 0 or index == len(files):
            print(
                f"Files: {index}/{len(files)}"
                f" | Labor records: {counts['rows']:,}",
                flush=True,
            )

    assert counts["rows"] == EXPECTED_ROWS
    assert counts["MATCHED"] == 3_217_345
    assert counts["UNMATCHED"] == 13_078
    assert counts["MISSING_REFERENCE"] == 0

    assert counts["negative_hours"] == 74_609
    assert counts["negative_cost"] == 71_374
    assert counts["zero_rate"] == 188_165

    print("\nSOURCE RECONCILIATION")
    print("Labor records:", f"{counts['rows']:,}")
    print("Reporting months:", len(months))
    print("Monthly/status groups:", len(groups))
    print("Source reconciliation: PASS")

    return groups, signed_totals, counts, months


def build_gold_labor_monthly_kpis():
    print("=" * 68)
    print("STEP 7.13 - BUILD MONTHLY LABOR KPIS")
    print("=" * 68)

    check_outputs()
    files = inspect_source()

    groups, signed_totals, counts, months = (
        aggregate_labor(files)
    )

    records = []

    for month, status in sorted(groups):
        key = (month, status)
        metrics = groups[key]

        records.append({
            "reporting_month": month,
            "workorder_match_status": status,
            "labor_record_count": metrics[
                "labor_record_count"
            ],
            "total_labor_hours": signed_totals[key]["hours"],
            "total_labor_cost": signed_totals[key]["cost"],
            "negative_hours_count": metrics[
                "negative_hours_count"
            ],
            "negative_cost_count": metrics[
                "negative_cost_count"
            ],
            "zero_rate_count": metrics["zero_rate_count"],
        })

    assert sum(
        r["labor_record_count"] for r in records
    ) == EXPECTED_ROWS

    assert len({
        (r["reporting_month"], r["workorder_match_status"])
        for r in records
    }) == len(records)

    # Write only after full source reconciliation.
    STAGING_DIR.mkdir(parents=True, exist_ok=False)

    table = pa.Table.from_pylist(
        records,
        schema=GOLD_SCHEMA,
    )

    pq.write_table(
        table,
        STAGING_FILE,
        compression="snappy",
    )

    del table
    gc.collect()

    with pq.ParquetFile(str(STAGING_FILE)) as reader:
        assert reader.metadata.num_rows == len(records)
        assert reader.schema_arrow.equals(
            GOLD_SCHEMA,
            check_metadata=False,
        )
        staged = reader.read().to_pylist()

    assert staged == records

    assert sum(
        r["negative_hours_count"] for r in staged
    ) == 74_609

    assert sum(
        r["negative_cost_count"] for r in staged
    ) == 71_374

    assert sum(
        r["zero_rate_count"] for r in staged
    ) == 188_165

    print("\nSTAGED LABOR KPI VALIDATION")
    print("KPI rows:", len(staged))
    print("Source labor rows:", f"{EXPECTED_ROWS:,}")
    print("Reporting months:", len(months))
    print("Matched labor:", f"{counts['MATCHED']:,}")
    print("Unmatched labor:", f"{counts['UNMATCHED']:,}")
    print("Negative hours records:", counts["negative_hours"])
    print("Negative cost records:", counts["negative_cost"])
    print("Staged validation: PASS")

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(FINAL_DIR)

    STAGING_DIR.rename(FINAL_DIR)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold Analytics",
        "table": "gold_labor_monthly_kpis",
        "status": "COMPLETED",
        "source": SOURCE_DIR.relative_to(ROOT).as_posix(),
        "output": FINAL_FILE.relative_to(ROOT).as_posix(),
        "rows": len(staged),
        "columns": len(GOLD_SCHEMA),
        "parquet_files": 1,
        "source_labor_rows": EXPECTED_ROWS,
        "reporting_months": len(months),
        "first_month": min(months),
        "last_month": max(months),
        "matched_labor_rows": counts["MATCHED"],
        "unmatched_labor_rows": counts["UNMATCHED"],
        "negative_hours_rows": counts["negative_hours"],
        "negative_cost_rows": counts["negative_cost"],
        "zero_rate_rows": counts["zero_rate"],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST.open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print("\nGOLD LABOR MONTHLY KPIS: SUCCESS")
    return manifest


def create_gold_labor_monthly_kpis_pipeline():
    return pipeline([
        node(
            func=build_gold_labor_monthly_kpis,
            inputs=None,
            outputs="gold_labor_monthly_manifest",
            name="build_gold_labor_monthly_kpis",
            tags=["gold", "analytics", "labor", "kpi"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_labor_monthly_manifest": MemoryDataset()
    })

    SequentialRunner().run(
        create_gold_labor_monthly_kpis_pipeline(),
        catalog,
    )

    result = catalog.load("gold_labor_monthly_manifest")
    assert result["status"] == "COMPLETED"

    print("KEDRO LABOR MONTHLY KPI PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
