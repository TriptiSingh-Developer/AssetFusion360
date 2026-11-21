"""
AssetFusion360 - Transaction Type Dimension

Source: Gold fact_transactions
Output: Gold dim_transaction_type

One row per distinct transaction type.
Preserves original transaction type values.
"""

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
GOLD = ROOT / "data" / "04_gold_analytics"

SOURCE = GOLD / "fact_transactions"
FINAL = GOLD / "dim_transaction_type"
STAGING = GOLD / "_staging_dim_transaction_type"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "dim_transaction_type_manifest.json"
)

EXPECTED_ROWS = 755_122
EXPECTED_FILES = 152
MISSING_LABEL = "<MISSING_TRANSACTION_TYPE>"

SCHEMA = pa.schema([
    pa.field("transaction_type_key", pa.int32(), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("is_missing_type", pa.bool_(), nullable=False),
    pa.field("source_transaction_count", pa.int64(), nullable=False),
])


def build_dimension():
    print("=" * 68)
    print("STEP 8.2F - BUILD DIM_TRANSACTION_TYPE")
    print("=" * 68)

    for path in [FINAL, STAGING, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    files = sorted(SOURCE.glob("*.parquet"))
    assert len(files) == EXPECTED_FILES

    counts = Counter()

    for path in files:
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=10000,
                columns=["transaction_type"],
            ):
                counts.update(batch.column(0).to_pylist())

    assert sum(counts.values()) == EXPECTED_ROWS

    non_null = [
        value for value in counts
        if value is not None
    ]

    assert all(isinstance(v, str) for v in non_null)
    assert MISSING_LABEL not in non_null

    records = []

    if counts[None] > 0:
        records.append({
            "transaction_type_key": 0,
            "transaction_type": MISSING_LABEL,
            "is_missing_type": True,
            "source_transaction_count": counts[None],
        })

    for key, kind in enumerate(sorted(non_null), 1):
        records.append({
            "transaction_type_key": key,
            "transaction_type": kind,
            "is_missing_type": False,
            "source_transaction_count": counts[kind],
        })

    assert sum(
        r["source_transaction_count"] for r in records
    ) == EXPECTED_ROWS

    assert len({
        r["transaction_type_key"] for r in records
    }) == len(records)

    assert len({
        r["transaction_type"] for r in records
    }) == len(records)

    print("\nTRANSACTION TYPE DISTRIBUTION")
    for record in records:
        print(
            f"{record['transaction_type']}: "
            f"{record['source_transaction_count']:,}"
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

    print("\nSTAGED VALIDATION: PASS")

    if FINAL.exists():
        raise FileExistsError(FINAL)

    STAGING.rename(FINAL)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "dim_transaction_type",
        "status": "COMPLETED",
        "rows": len(records),
        "columns": len(SCHEMA),
        "parquet_files": 1,
        "source_transactions": EXPECTED_ROWS,
        "missing_transaction_types": counts[None],
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

    print("\nDIM_TRANSACTION_TYPE")
    print("Dimension rows:", len(records))
    print("Missing transaction types:", counts[None])
    print("Unique dimension keys: PASS")
    print("Source-to-dimension reconciliation: PASS")
    print("Published dimension: PASS")

    print("\nSTEP 8.2F DIM_TRANSACTION_TYPE: PASS")


if __name__ == "__main__":
    build_dimension()
