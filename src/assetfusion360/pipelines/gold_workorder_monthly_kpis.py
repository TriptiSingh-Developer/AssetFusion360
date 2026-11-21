"""
AssetFusion360 Capstone
Step 7.12 - Monthly Work Order KPI Analytics

Source: Gold fact_workorders
Target: Gold gold_workorder_monthly_kpis

Grain: One row per reporting month and work-order type.

Monthly reporting is based on workorder_date.
Completion metrics measure timestamp availability.
No source files are modified or overwritten.
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

SOURCE_DIR = (
    ROOT / "data" / "04_gold_analytics"
    / "fact_workorders"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

FINAL_DIR = GOLD_ROOT / "gold_workorder_monthly_kpis"
STAGING_DIR = GOLD_ROOT / "_staging_gold_workorder_monthly_kpis"

FINAL_FILE = FINAL_DIR / "part-00000.parquet"
STAGING_FILE = STAGING_DIR / "part-00000.parquet"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_workorder_monthly_kpis_manifest.json"
)

EXPECTED_ROWS = 2_231_535
EXPECTED_FILES = 447
EXPECTED_MONTHS = 224
BATCH_SIZE = 10000


GOLD_SCHEMA = pa.schema([
    pa.field("reporting_month", pa.string(), nullable=False),
    pa.field("workorder_type", pa.string(), nullable=False),
    pa.field("workorder_count", pa.int64()),
    pa.field("completion_timestamp_present_count", pa.int64()),
    pa.field("completion_timestamp_missing_count", pa.int64()),
    pa.field("priority_present_count", pa.int64()),
    pa.field("priority_missing_count", pa.int64()),
    pa.field("ambiguous_asset_mapping_count", pa.int64()),
    pa.field("unique_asset_mapping_count", pa.int64()),
])


SOURCE_FIELDS = [
    "workorder_date",
    "workorder_type",
    "workorder_priority",
    "completed_at",
    "asset_resolution_status",
]


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

    reference_schema = pq.read_schema(files[0])
    total_rows = 0

    for path in files:
        assert pq.read_schema(path).equals(
            reference_schema,
            check_metadata=False,
        ), f"Schema mismatch: {path.name}"

        total_rows += pq.read_metadata(path).num_rows

    assert total_rows == EXPECTED_ROWS

    for name in SOURCE_FIELDS:
        assert name in reference_schema.names, (
            f"Missing field: {name}"
        )

    assert pa.types.is_timestamp(
        reference_schema.field("workorder_date").type
    )

    print("Source Parquet files:", len(files))
    print("Source work orders:", f"{total_rows:,}")
    print("Source schema: PASS")

    return files


def aggregate_workorders(files):
    groups = defaultdict(Counter)
    totals = Counter()
    work_types = Counter()
    months = set()

    for index, path in enumerate(files, 1):
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=BATCH_SIZE,
                columns=SOURCE_FIELDS,
            ):
                data = batch.to_pydict()

                for i in range(batch.num_rows):
                    date = data["workorder_date"][i]
                    kind = data["workorder_type"][i]

                    if date is None:
                        raise ValueError(
                            "Missing work-order reporting date"
                        )

                    if kind is None or not str(kind).strip():
                        raise ValueError(
                            "Missing work-order type"
                        )

                    month = date.strftime("%Y-%m")
                    key = (month, kind)

                    group = groups[key]
                    group["workorder_count"] += 1

                    totals["rows"] += 1
                    work_types[kind] += 1
                    months.add(month)

                    completed = (
                        data["completed_at"][i] is not None
                    )

                    priority_present = (
                        data["workorder_priority"][i] is not None
                    )

                    status = data[
                        "asset_resolution_status"
                    ][i]

                    if status not in {"UNIQUE", "AMBIGUOUS"}:
                        raise ValueError(
                            f"Invalid asset resolution: {status}"
                        )

                    if completed:
                        group[
                            "completion_timestamp_present_count"
                        ] += 1
                    else:
                        group[
                            "completion_timestamp_missing_count"
                        ] += 1
                        totals["missing_completion"] += 1

                    if priority_present:
                        group["priority_present_count"] += 1
                    else:
                        group["priority_missing_count"] += 1
                        totals["missing_priority"] += 1

                    if status == "AMBIGUOUS":
                        group[
                            "ambiguous_asset_mapping_count"
                        ] += 1
                        totals["ambiguous"] += 1
                    else:
                        group[
                            "unique_asset_mapping_count"
                        ] += 1

        if index % 50 == 0 or index == len(files):
            print(
                f"Processed files: {index}/{len(files)}"
                f" | Work orders: {totals['rows']:,}",
                flush=True,
            )

    assert totals["rows"] == EXPECTED_ROWS
    assert totals["missing_completion"] == 25_585
    assert totals["missing_priority"] == 994_238
    assert totals["ambiguous"] == 400

    assert len(months) == EXPECTED_MONTHS
    assert min(months) == "2001-03"
    assert max(months) == "2026-10"

    expected_types = {
        "JOB": 1_848_051,
        "IN": 108_258,
        "PPM": 101_154,
        "AA": 100_622,
        "STAT": 43_219,
        "MRC": 15_136,
        "XA": 15_095,
    }

    assert dict(work_types) == expected_types

    print("\nSOURCE RECONCILIATION")
    print("Work orders:", f"{totals['rows']:,}")
    print("Reporting months:", len(months))
    print("Monthly/type groups:", len(groups))
    print("Work-order types:", len(work_types))
    print("Source reconciliation: PASS")

    return groups, totals, months


def build_gold_workorder_monthly_kpis():
    print("=" * 68)
    print("STEP 7.12 - BUILD MONTHLY WORK ORDER KPIS")
    print("=" * 68)

    check_outputs()
    files = inspect_source()

    groups, totals, months = aggregate_workorders(files)

    records = []

    for month, kind in sorted(groups):
        metrics = groups[(month, kind)]

        record = {
            "reporting_month": month,
            "workorder_type": kind,
        }

        for field in GOLD_SCHEMA.names[2:]:
            record[field] = metrics[field]

        assert (
            record["completion_timestamp_present_count"]
            + record["completion_timestamp_missing_count"]
            == record["workorder_count"]
        )

        assert (
            record["priority_present_count"]
            + record["priority_missing_count"]
            == record["workorder_count"]
        )

        assert (
            record["ambiguous_asset_mapping_count"]
            + record["unique_asset_mapping_count"]
            == record["workorder_count"]
        )

        records.append(record)

    assert len(records) == len(groups)

    assert sum(
        r["workorder_count"] for r in records
    ) == EXPECTED_ROWS

    # All checks above run before staging is created.
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

    # Independently verify the published schema and
    # numerical reconciliation against staged data.
    with pq.ParquetFile(str(STAGING_FILE)) as reader:
        assert reader.metadata.num_rows == len(records)
        assert reader.schema_arrow.equals(
            GOLD_SCHEMA,
            check_metadata=False,
        )

        staged = reader.read().to_pylist()

    assert len(staged) == len(records)

    assert len({
        (r["reporting_month"], r["workorder_type"])
        for r in staged
    }) == len(records)

    assert sum(
        r["workorder_count"] for r in staged
    ) == EXPECTED_ROWS

    assert sum(
        r["completion_timestamp_missing_count"]
        for r in staged
    ) == 25_585

    assert sum(
        r["priority_missing_count"]
        for r in staged
    ) == 994_238

    assert sum(
        r["ambiguous_asset_mapping_count"]
        for r in staged
    ) == 400

    print("\nSTAGED KPI VALIDATION")
    print("KPI rows:", len(staged))
    print("Source work orders:", f"{EXPECTED_ROWS:,}")
    print("Missing completion timestamps: 25,585")
    print("Missing priorities: 994,238")
    print("Ambiguous asset mappings: 400")
    print("Staged validation: PASS")

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(FINAL_DIR)

    STAGING_DIR.rename(FINAL_DIR)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold Analytics",
        "table": "gold_workorder_monthly_kpis",
        "status": "COMPLETED",
        "source": SOURCE_DIR.relative_to(ROOT).as_posix(),
        "output": FINAL_FILE.relative_to(ROOT).as_posix(),
        "rows": len(staged),
        "columns": len(GOLD_SCHEMA),
        "parquet_files": 1,
        "source_workorder_rows": EXPECTED_ROWS,
        "reporting_months": len(months),
        "first_month": min(months),
        "last_month": max(months),
        "missing_priority_rows": totals["missing_priority"],
        "missing_completion_rows": totals["missing_completion"],
        "ambiguous_asset_rows": totals["ambiguous"],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST.open(
        "x", encoding="utf-8"
    ) as handle:
        json.dump(manifest, handle, indent=2)

    print("\nGOLD WORK ORDER MONTHLY KPIS: SUCCESS")

    return manifest


def create_gold_workorder_monthly_kpis_pipeline():
    return pipeline([
        node(
            func=build_gold_workorder_monthly_kpis,
            inputs=None,
            outputs="gold_monthly_kpi_manifest",
            name="build_gold_workorder_monthly_kpis",
            tags=["gold", "analytics", "workorders", "kpi"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_monthly_kpi_manifest": MemoryDataset()
    })

    SequentialRunner().run(
        create_gold_workorder_monthly_kpis_pipeline(),
        catalog,
    )

    result = catalog.load("gold_monthly_kpi_manifest")
    assert result["status"] == "COMPLETED"

    print("KEDRO MONTHLY KPI PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
