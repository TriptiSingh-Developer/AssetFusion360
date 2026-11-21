"""
AssetFusion360 - Gold Monthly Material KPIs

Source: Gold fact_transaction_details
Grain: month x detail type x UOM x transaction match
       status x work-order match status.

Preserves signed quantities and missing values.
No existing Gold table is overwritten.
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
GOLD = ROOT / "data" / "04_gold_analytics"

SOURCE_DIR = GOLD / "fact_transaction_details"
FINAL_DIR = GOLD / "gold_material_monthly_kpis"
STAGING_DIR = GOLD / "_staging_gold_material_monthly_kpis"

STAGING_FILE = STAGING_DIR / "part-00000.parquet"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_material_monthly_kpis_manifest.json"
)

EXPECTED_ROWS = 2_301_927
EXPECTED_FILES = 461

SOURCE_FIELDS = [
    "detail_date",
    "detail_type",
    "part_uom",
    "quantity",
    "unit_price",
    "average_price",
    "transaction_match_status",
    "workorder_match_status",
    "negative_quantity_flag",
    "negative_price_flag",
    "zero_quantity_flag",
    "zero_price_flag",
]

COUNT_FIELDS = [
    "material_detail_count",
    "quantity_present_count",
    "quantity_missing_count",
    "negative_quantity_count",
    "zero_quantity_count",
    "unit_price_present_count",
    "unit_price_missing_count",
    "negative_unit_price_count",
    "zero_unit_price_count",
    "average_price_missing_count",
]

SCHEMA = pa.schema([
    pa.field("reporting_month", pa.string(), nullable=False),
    pa.field("detail_type", pa.string(), nullable=False),
    pa.field("part_uom", pa.string(), nullable=False),
    pa.field(
        "transaction_match_status",
        pa.string(), nullable=False
    ),
    pa.field(
        "workorder_match_status",
        pa.string(), nullable=False
    ),
    pa.field("material_detail_count", pa.int64()),
    pa.field("total_signed_quantity", pa.decimal128(30, 6)),
    pa.field("quantity_present_count", pa.int64()),
    pa.field("quantity_missing_count", pa.int64()),
    pa.field("negative_quantity_count", pa.int64()),
    pa.field("zero_quantity_count", pa.int64()),
    pa.field("unit_price_present_count", pa.int64()),
    pa.field("unit_price_missing_count", pa.int64()),
    pa.field("negative_unit_price_count", pa.int64()),
    pa.field("zero_unit_price_count", pa.int64()),
    pa.field("average_price_missing_count", pa.int64()),
])


def check_outputs():
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(SOURCE_DIR)

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    print("Output safety: PASS")


def inspect_source():
    files = sorted(SOURCE_DIR.glob("*.parquet"))
    assert len(files) == EXPECTED_FILES

    schema = pq.read_schema(files[0])
    total_rows = 0

    for path in files:
        assert pq.read_schema(path).equals(
            schema, check_metadata=False
        ), f"Schema mismatch: {path.name}"

        total_rows += pq.read_metadata(path).num_rows

    assert total_rows == EXPECTED_ROWS

    for name in SOURCE_FIELDS:
        assert name in schema.names, (
            f"Missing source column: {name}"
        )

    assert pa.types.is_timestamp(
        schema.field("detail_date").type
    )

    for name in [
        "quantity", "unit_price", "average_price"
    ]:
        assert pa.types.is_decimal(
            schema.field(name).type
        )

    for name in [
        "negative_quantity_flag",
        "negative_price_flag",
        "zero_quantity_flag",
        "zero_price_flag",
    ]:
        assert pa.types.is_boolean(
            schema.field(name).type
        )

    print("Source Parquet files:", len(files))
    print("Source detail records:", f"{total_rows:,}")
    print("Source schema: PASS")

    return files


def category(value, missing_label):
    return str(value) if value is not None else missing_label


def aggregate_materials(files):
    groups = defaultdict(Counter)
    quantities = defaultdict(lambda: Decimal("0"))
    totals = Counter()
    months = set()

    for index, path in enumerate(files, 1):
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=10000,
                columns=SOURCE_FIELDS,
            ):
                for row in batch.to_pylist():
                    date = row["detail_date"]
                    quantity = row["quantity"]
                    price = row["unit_price"]
                    average_price = row["average_price"]

                    month = (
                        date.strftime("%Y-%m")
                        if date is not None
                        else "<MISSING_DATE>"
                    )

                    if date is not None:
                        months.add(month)

                    key = (
                        month,
                        category(
                            row["detail_type"],
                            "<MISSING_TYPE>",
                        ),
                        category(
                            row["part_uom"],
                            "<MISSING_UOM>",
                        ),
                        category(
                            row["transaction_match_status"],
                            "<MISSING_TRANSACTION_STATUS>",
                        ),
                        category(
                            row["workorder_match_status"],
                            "<MISSING_WORKORDER_STATUS>",
                        ),
                    )

                    group = groups[key]
                    group["material_detail_count"] += 1
                    totals["material_detail_count"] += 1

                    if quantity is None:
                        group["quantity_missing_count"] += 1
                        totals["quantity_missing_count"] += 1
                    else:
                        group["quantity_present_count"] += 1
                        totals["quantity_present_count"] += 1
                        quantities[key] += quantity

                    if price is None:
                        group["unit_price_missing_count"] += 1
                        totals["unit_price_missing_count"] += 1
                    else:
                        group["unit_price_present_count"] += 1
                        totals["unit_price_present_count"] += 1

                    if average_price is None:
                        group["average_price_missing_count"] += 1
                        totals["average_price_missing_count"] += 1

                    checks = {
                        "negative_quantity_count": (
                            quantity is not None
                            and quantity < 0
                        ),
                        "zero_quantity_count": (
                            quantity is not None
                            and quantity == 0
                        ),
                        "negative_unit_price_count": (
                            price is not None
                            and price < 0
                        ),
                        "zero_unit_price_count": (
                            price is not None
                            and price == 0
                        ),
                    }

                    flag_map = {
                        "negative_quantity_count":
                            "negative_quantity_flag",
                        "zero_quantity_count":
                            "zero_quantity_flag",
                        "negative_unit_price_count":
                            "negative_price_flag",
                        "zero_unit_price_count":
                            "zero_price_flag",
                    }

                    for measure, condition in checks.items():
                        flag = row[flag_map[measure]]

                        if flag is not None:
                            assert flag == condition, (
                                f"Source flag mismatch: {measure}"
                            )

                        group[measure] += int(condition)
                        totals[measure] += int(condition)

        if index % 50 == 0 or index == len(files):
            print(
                f"Processed files: {index}/{len(files)}"
                f" | Detail records:"
                f" {totals['material_detail_count']:,}",
                flush=True,
            )

    assert totals["material_detail_count"] == EXPECTED_ROWS
    assert totals["negative_quantity_count"] == 44_053
    assert totals["negative_unit_price_count"] == 5_501
    assert totals["zero_quantity_count"] == 164_449
    assert totals["zero_unit_price_count"] == 61_515

    print("\nSOURCE RECONCILIATION")
    print("Detail records:", f"{EXPECTED_ROWS:,}")
    print("Monthly KPI groups:", len(groups))
    print("Reporting months:", len(months))
    print("Source reconciliation: PASS")

    return groups, quantities, totals, months


def build_gold_material_monthly_kpis():
    print("=" * 68)
    print("STEP 7.15 - BUILD MONTHLY MATERIAL KPIS")
    print("=" * 68)

    check_outputs()
    files = inspect_source()

    groups, quantities, totals, months = (
        aggregate_materials(files)
    )

    records = []

    for key in sorted(groups):
        metrics = groups[key]
        n = metrics["material_detail_count"]

        assert (
            metrics["quantity_present_count"]
            + metrics["quantity_missing_count"]
            == n
        )

        assert (
            metrics["unit_price_present_count"]
            + metrics["unit_price_missing_count"]
            == n
        )

        record = dict(zip(
            [
                "reporting_month",
                "detail_type",
                "part_uom",
                "transaction_match_status",
                "workorder_match_status",
            ],
            key,
        ))

        record["total_signed_quantity"] = quantities[key]

        for name in COUNT_FIELDS:
            record[name] = metrics[name]

        records.append(record)

    assert sum(
        row["material_detail_count"] for row in records
    ) == EXPECTED_ROWS

    # Create staging only after source validation.
    STAGING_DIR.mkdir(parents=True, exist_ok=False)

    table = pa.Table.from_pylist(records, schema=SCHEMA)

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
            SCHEMA, check_metadata=False
        )
        staged = reader.read().to_pylist()

    assert staged == records

    for name in COUNT_FIELDS:
        assert sum(
            row[name] for row in staged
        ) == totals[name]

    print("\nSTAGED MATERIAL KPI VALIDATION")
    print("KPI rows:", len(staged))
    print("Source detail records:", f"{EXPECTED_ROWS:,}")
    print("Reporting months:", len(months))
    print("Negative quantities:", 44_053)
    print("Negative unit prices:", 5_501)
    print("Zero quantities:", 164_449)
    print("Zero unit prices:", 61_515)
    print("Staged validation: PASS")

    if FINAL_DIR.exists():
        raise FileExistsError(FINAL_DIR)

    STAGING_DIR.rename(FINAL_DIR)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold Analytics",
        "table": "gold_material_monthly_kpis",
        "status": "COMPLETED",
        "source": SOURCE_DIR.relative_to(ROOT).as_posix(),
        "output": (
            FINAL_DIR / "part-00000.parquet"
        ).relative_to(ROOT).as_posix(),
        "rows": len(staged),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "source_detail_rows": EXPECTED_ROWS,
        "reporting_months": len(months),
        "first_month": min(months) if months else None,
        "last_month": max(months) if months else None,
        "negative_quantity_rows": totals[
            "negative_quantity_count"
        ],
        "negative_price_rows": totals[
            "negative_unit_price_count"
        ],
        "zero_quantity_rows": totals[
            "zero_quantity_count"
        ],
        "zero_price_rows": totals[
            "zero_unit_price_count"
        ],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True, exist_ok=True
    )

    with MANIFEST.open(
        "x", encoding="utf-8"
    ) as handle:
        json.dump(manifest, handle, indent=2)

    print("\nGOLD MATERIAL MONTHLY KPIS: SUCCESS")
    return manifest


def create_gold_material_monthly_kpis_pipeline():
    return pipeline([
        node(
            func=build_gold_material_monthly_kpis,
            inputs=None,
            outputs="gold_material_monthly_manifest",
            name="build_gold_material_monthly_kpis",
            tags=["gold", "analytics", "material", "kpi"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_material_monthly_manifest": MemoryDataset()
    })

    SequentialRunner().run(
        create_gold_material_monthly_kpis_pipeline(),
        catalog,
    )

    result = catalog.load(
        "gold_material_monthly_manifest"
    )

    assert result["status"] == "COMPLETED"
    print("KEDRO MATERIAL KPI PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
