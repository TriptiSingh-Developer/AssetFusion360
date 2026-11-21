"""
AssetFusion360 - Work Order Type Dimension

Source: Gold fact_workorders
Output: Gold dim_workorder_type

One row per distinct work-order type.
"""

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "data" / "04_gold_analytics"

SOURCE = GOLD / "fact_workorders"
FINAL = GOLD / "dim_workorder_type"
STAGING = GOLD / "_staging_dim_workorder_type"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "dim_workorder_type_manifest.json"
)

EXPECTED = {
    "JOB": 1_848_051,
    "IN": 108_258,
    "PPM": 101_154,
    "AA": 100_622,
    "STAT": 43_219,
    "MRC": 15_136,
    "XA": 15_095,
}

SCHEMA = pa.schema([
    pa.field("workorder_type_key", pa.int32(), nullable=False),
    pa.field("workorder_type", pa.string(), nullable=False),
    pa.field("source_workorder_count", pa.int64(), nullable=False),
])


def build_dimension():
    print("=" * 65)
    print("STEP 8.2D - BUILD DIM_WORKORDER_TYPE")
    print("=" * 65)

    for path in [FINAL, STAGING, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    files = sorted(SOURCE.glob("*.parquet"))
    assert len(files) == 447

    counts = Counter()

    for path in files:
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=10000,
                columns=["workorder_type"],
            ):
                counts.update(
                    batch.column(0).to_pylist()
                )

    assert dict(counts) == EXPECTED
    assert sum(counts.values()) == 2_231_535

    records = []

    for key, code in enumerate(sorted(counts), 1):
        records.append({
            "workorder_type_key": key,
            "workorder_type": code,
            "source_workorder_count": counts[code],
        })

    assert len(records) == 7

    STAGING.mkdir(parents=True, exist_ok=False)
    staged = STAGING / "part-00000.parquet"

    table = pa.Table.from_pylist(records, schema=SCHEMA)
    pq.write_table(table, staged, compression="snappy")

    verified = pq.read_table(staged).to_pylist()
    assert verified == records

    STAGING.rename(FINAL)

    manifest = {
        "table": "dim_workorder_type",
        "status": "COMPLETED",
        "rows": 7,
        "columns": 3,
        "source_workorders": 2_231_535,
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

    print("\nDIM_WORKORDER_TYPE")
    print("Dimension rows:", len(records))
    print("Source work orders:", f"{sum(counts.values()):,}")
    print("Unique work-order types: PASS")
    print("Source-to-dimension reconciliation: PASS")
    print("Published dimension: PASS")
    print("\nSTEP 8.2D DIM_WORKORDER_TYPE: PASS")


if __name__ == "__main__":
    build_dimension()
