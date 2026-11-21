"""
AssetFusion360 - Work Order Priority Dimension

Source: Gold fact_workorders
Output: Gold dim_workorder_priority

Preserves distinct priority values and includes an
explicit reporting member for missing priorities.
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
FINAL = GOLD / "dim_workorder_priority"
STAGING = GOLD / "_staging_dim_workorder_priority"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "dim_workorder_priority_manifest.json"
)

EXPECTED_ROWS = 2_231_535
EXPECTED_MISSING = 994_238
MISSING_LABEL = "<MISSING_PRIORITY>"

SCHEMA = pa.schema([
    pa.field("workorder_priority_key", pa.int32(), nullable=False),
    pa.field("workorder_priority", pa.string(), nullable=False),
    pa.field("is_missing_priority", pa.bool_(), nullable=False),
    pa.field("source_workorder_count", pa.int64(), nullable=False),
])


def build_dimension():
    print("=" * 68)
    print("STEP 8.2E - BUILD DIM_WORKORDER_PRIORITY")
    print("=" * 68)

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
                columns=["workorder_priority"],
            ):
                counts.update(
                    batch.column(0).to_pylist()
                )

    assert sum(counts.values()) == EXPECTED_ROWS
    assert counts[None] == EXPECTED_MISSING

    # Original non-null codes must remain distinguishable
    # from the artificial missing-value reporting member.
    non_null = [value for value in counts if value is not None]

    assert all(isinstance(value, str) for value in non_null), (
        "Non-string priority found; review required"
    )

    assert MISSING_LABEL not in non_null, (
        "Missing-value label conflicts with real priority"
    )

    records = [{
        "workorder_priority_key": 0,
        "workorder_priority": MISSING_LABEL,
        "is_missing_priority": True,
        "source_workorder_count": counts[None],
    }]

    for key, priority in enumerate(sorted(non_null), 1):
        records.append({
            "workorder_priority_key": key,
            "workorder_priority": priority,
            "is_missing_priority": False,
            "source_workorder_count": counts[priority],
        })

    assert len(records) == len(non_null) + 1
    assert sum(
        row["source_workorder_count"] for row in records
    ) == EXPECTED_ROWS

    assert len({
        row["workorder_priority_key"] for row in records
    }) == len(records)

    assert len({
        row["workorder_priority"] for row in records
    }) == len(records)

    print("\nSOURCE RECONCILIATION")
    print("Source work orders:", f"{EXPECTED_ROWS:,}")
    print("Missing priorities:", f"{EXPECTED_MISSING:,}")
    print("Distinct non-null priorities:", len(non_null))

    print("\nPRIORITY DISTRIBUTION")
    for row in records:
        print(
            f"{row['workorder_priority']}: "
            f"{row['source_workorder_count']:,}"
        )

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

        verified = reader.read().to_pylist()

    assert verified == records

    print("\nSTAGED DIMENSION VALIDATION: PASS")

    if FINAL.exists():
        raise FileExistsError(FINAL)

    STAGING.rename(FINAL)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "dim_workorder_priority",
        "status": "COMPLETED",
        "rows": len(records),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "source_workorders": EXPECTED_ROWS,
        "missing_priority_rows": EXPECTED_MISSING,
        "distinct_non_null_priorities": len(non_null),
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

    print("\nDIM_WORKORDER_PRIORITY")
    print("Dimension rows:", len(records))
    print("Source work orders:", f"{EXPECTED_ROWS:,}")
    print("Missing-value member: PASS")
    print("Unique dimension keys: PASS")
    print("Source-to-dimension reconciliation: PASS")
    print("Published dimension: PASS")

    print("\nSTEP 8.2E DIM_WORKORDER_PRIORITY: PASS")


if __name__ == "__main__":
    build_dimension()
