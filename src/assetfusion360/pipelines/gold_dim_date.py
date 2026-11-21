"""
AssetFusion360 - Step 8.2
Conformed calendar dimension from Gold fact dates.

Creates a continuous daily calendar.
Does not change any existing Gold dataset.
"""

import json
from datetime import timedelta, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "data" / "04_gold_analytics"

FINAL = GOLD / "dim_date"
STAGING = GOLD / "_staging_dim_date"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "dim_date_manifest.json"
)

SOURCES = {
    "fact_workorders": ("workorder_date", 447, 2_231_535),
    "fact_labor": ("labor_date", 647, 3_230_423),
    "fact_transactions": ("transaction_date", 152, 755_122),
    "fact_transaction_details": ("detail_date", 461, 2_301_927),
}

SCHEMA = pa.schema([
    pa.field("date_key", pa.int32(), nullable=False),
    pa.field("calendar_date", pa.date32(), nullable=False),
    pa.field("year", pa.int16(), nullable=False),
    pa.field("quarter", pa.int8(), nullable=False),
    pa.field("month", pa.int8(), nullable=False),
    pa.field("month_name", pa.string(), nullable=False),
    pa.field("year_month", pa.string(), nullable=False),
    pa.field("day_of_month", pa.int8(), nullable=False),
    pa.field("day_of_week", pa.int8(), nullable=False),
    pa.field("day_name", pa.string(), nullable=False),
    pa.field("iso_week", pa.int8(), nullable=False),
    pa.field("is_weekend", pa.bool_(), nullable=False),
])


def check_outputs():
    for path in (FINAL, STAGING, MANIFEST):
        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    print("Output safety: PASS")


def collect_date_coverage():
    observed = set()
    source_stats = {}

    for table, (column, expected_files, expected_rows) in (
        SOURCES.items()
    ):
        files = sorted((GOLD / table).glob("*.parquet"))

        assert len(files) == expected_files, (
            f"Unexpected file count: {table}"
        )

        reference = pq.read_schema(files[0])
        assert column in reference.names

        assert pa.types.is_timestamp(
            reference.field(column).type
        ), f"Expected timestamp: {table}.{column}"

        rows = 0
        missing = 0

        for path in files:
            assert pq.read_schema(path).equals(
                reference, check_metadata=False
            )

            with pq.ParquetFile(str(path)) as reader:
                for batch in reader.iter_batches(
                    batch_size=50000,
                    columns=[column],
                ):
                    rows += batch.num_rows

                    dates = pc.cast(
                        batch.column(0),
                        pa.date32(),
                    )

                    missing += dates.null_count

                    observed.update(
                        value
                        for value in pc.unique(dates).to_pylist()
                        if value is not None
                    )

        assert rows == expected_rows

        source_stats[table] = {
            "rows": rows,
            "missing_dates": missing,
        }

        print(
            f"{table}: {rows:,} rows, "
            f"{missing:,} missing dates",
            flush=True,
        )

    assert observed, "No usable source dates"

    return observed, source_stats


def make_records(first, last):
    records = []
    current = first

    while current <= last:
        iso_weekday = current.isoweekday()

        records.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "calendar_date": current,
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "year_month": current.strftime("%Y-%m"),
            "day_of_month": current.day,
            "day_of_week": iso_weekday,
            "day_name": current.strftime("%A"),
            "iso_week": current.isocalendar().week,
            "is_weekend": iso_weekday >= 6,
        })

        current += timedelta(days=1)

    return records


def build_dim_date():
    print("=" * 68)
    print("STEP 8.2 - BUILD DIM_DATE")
    print("=" * 68)

    check_outputs()

    observed, source_stats = collect_date_coverage()

    first = min(observed)
    last = max(observed)

    records = make_records(first, last)

    assert len(records) == (last - first).days + 1
    assert len(records) >= len(observed)

    date_keys = {r["date_key"] for r in records}
    calendar_dates = {r["calendar_date"] for r in records}

    assert len(date_keys) == len(records)
    assert len(calendar_dates) == len(records)
    assert observed.issubset(calendar_dates)

    print("\nCALENDAR VALIDATION")
    print("First date:", first)
    print("Last date:", last)
    print("Calendar rows:", len(records))
    print("Observed dates covered:", len(observed))
    print("Unique date keys: PASS")
    print("Continuous calendar: PASS")

    STAGING.mkdir(parents=True, exist_ok=False)
    staged_file = STAGING / "part-00000.parquet"

    table = pa.Table.from_pylist(
        records,
        schema=SCHEMA,
    )

    pq.write_table(
        table,
        staged_file,
        compression="snappy",
    )

    with pq.ParquetFile(str(staged_file)) as reader:
        assert reader.metadata.num_rows == len(records)
        assert reader.schema_arrow.equals(
            SCHEMA,
            check_metadata=False,
        )

        staged = reader.read().to_pylist()

    assert staged == records

    print("\nSTAGED DIM_DATE VALIDATION: PASS")

    if FINAL.exists():
        raise FileExistsError(FINAL)

    STAGING.rename(FINAL)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "dim_date",
        "status": "COMPLETED",
        "rows": len(records),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "first_date": first.isoformat(),
        "last_date": last.isoformat(),
        "observed_dates": len(observed),
        "source_stats": source_stats,
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

    print("\nGOLD DIM_DATE: SUCCESS")
    return manifest


def create_dim_date_pipeline():
    return pipeline([
        node(
            func=build_dim_date,
            inputs=None,
            outputs="dim_date_manifest",
            name="build_dim_date",
            tags=["gold", "dimension", "date"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "dim_date_manifest": MemoryDataset()
    })

    SequentialRunner().run(
        create_dim_date_pipeline(),
        catalog,
    )

    result = catalog.load("dim_date_manifest")
    assert result["status"] == "COMPLETED"

    print("KEDRO DIM_DATE PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
