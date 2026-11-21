"""
AssetFusion360 Capstone
Step 7.10 - Gold Transaction Details Fact Table

Source: Silver AMPS Transaction Details
References: Gold Transactions and Gold Work Orders

Grain: One row per detail_identity_int
Preserves all 2,301,927 original detail records.
Never overwrites completed datasets.
"""

import gc
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


ROOT = Path(__file__).resolve().parents[3]

SILVER_DIR = (
    ROOT / "data" / "03_silver_cleaned_data"
    / "amps_transaction_details"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

TRANSACTION_DIR = GOLD_ROOT / "fact_transactions"
WORKORDER_DIR = GOLD_ROOT / "fact_workorders"

FINAL_DIR = GOLD_ROOT / "fact_transaction_details"
STAGING_DIR = GOLD_ROOT / "_staging_fact_transaction_details"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_fact_transaction_details_manifest.json"
)

EXPECTED_ROWS = 2_301_927
EXPECTED_FILES = 461
BATCH_SIZE = 10000


# ============================================================
# 1. GOLD SCHEMA
# ============================================================

GOLD_SCHEMA = pa.schema([
    pa.field("detail_identity", pa.int64(), nullable=False),
    pa.field("transaction_code", pa.string()),
    pa.field("transaction_identity", pa.int64()),
    pa.field("transaction_match_status", pa.string()),
    pa.field("workorder_code", pa.string()),
    pa.field("workorder_identity", pa.int64()),
    pa.field("workorder_match_status", pa.string()),
    pa.field("transaction_line", pa.string()),
    pa.field("detail_type", pa.string()),
    pa.field("activity_code", pa.string()),
    pa.field("detail_description", pa.string()),
    pa.field("detail_date", pa.timestamp("us")),
    pa.field("detail_created_at", pa.timestamp("us")),
    pa.field("detail_updated_at", pa.timestamp("us")),
    pa.field("part_code", pa.string()),
    pa.field("part_identity", pa.int64()),
    pa.field("part_description", pa.string()),
    pa.field("part_class", pa.string()),
    pa.field("part_uom", pa.string()),
    pa.field("store_code", pa.string()),
    pa.field("bin_code", pa.string()),
    pa.field("quantity", pa.decimal128(20, 6)),
    pa.field("unit_price", pa.decimal128(20, 6)),
    pa.field("average_price", pa.decimal128(20, 6)),
    pa.field("negative_quantity_flag", pa.bool_()),
    pa.field("negative_price_flag", pa.bool_()),
    pa.field("zero_quantity_flag", pa.bool_()),
    pa.field("zero_price_flag", pa.bool_()),
    pa.field("silver_dq_issue", pa.bool_()),
    pa.field("source_file", pa.string()),
    pa.field("source_batch", pa.string()),
])


# Gold column -> Silver column
SOURCE_MAPPING = {
    "detail_identity": "detail_identity_int",
    "transaction_code": "trl_trans",
    "workorder_code": "trl_event",
    "transaction_line": "trl_line",
    "detail_type": "trl_type",
    "activity_code": "trl_act",
    "detail_description": "trl_desc",
    "detail_date": "detail_date_ts",
    "detail_created_at": "detail_created_ts",
    "detail_updated_at": "detail_updated_ts",
    "part_code": "par_code",
    "part_identity": "part_identity_int",
    "part_description": "par_desc",
    "part_class": "par_class",
    "part_uom": "par_uom",
    "store_code": "trl_store",
    "bin_code": "trl_bin",
    "quantity": "detail_quantity_decimal",
    "unit_price": "detail_price_decimal",
    "average_price": "detail_avgprice_decimal",
    "negative_quantity_flag": "dq_negative_quantity",
    "negative_price_flag": "dq_negative_price",
    "zero_quantity_flag": "dq_zero_quantity",
    "zero_price_flag": "dq_zero_price",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
    "source_batch": "_bronze_batch_id",
}


# ============================================================
# 2. OUTPUT SAFETY
# ============================================================

def check_outputs():
    for directory in [
        SILVER_DIR,
        TRANSACTION_DIR,
        WORKORDER_DIR,
    ]:
        if not directory.is_dir():
            raise FileNotFoundError(directory)

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing artifact must be preserved: {path}"
            )

    print("Output safety: PASS")


# ============================================================
# 3. SOURCE SCHEMA CHECK
# ============================================================

def inspect_sources():
    files = sorted(SILVER_DIR.glob("*.parquet"))

    assert len(files) == EXPECTED_FILES

    schema = pq.read_schema(files[0])
    assert len(schema) == 68

    total_rows = 0

    for path in files:
        current_schema = pq.read_schema(path)

        assert current_schema.equals(
            schema, check_metadata=False
        ), f"Schema mismatch: {path.name}"

        total_rows += pq.read_metadata(path).num_rows

    assert total_rows == EXPECTED_ROWS

    for gold_name, silver_name in SOURCE_MAPPING.items():
        assert silver_name in schema.names, (
            f"Missing Silver field: {silver_name}"
        )

        assert schema.field(silver_name).type.equals(
            GOLD_SCHEMA.field(gold_name).type
        ), f"Field type mismatch: {silver_name}"

    print("Silver files:", len(files))
    print("Silver rows:", f"{total_rows:,}")
    print("Silver columns:", len(schema))
    print("Gold columns:", len(GOLD_SCHEMA))
    print("Source schema: PASS")

    return files


# ============================================================
# 4. LOAD GOLD RELATIONSHIP LOOKUPS
# ============================================================

def load_lookup(directory, code_field, identity_field, count):
    files = sorted(directory.glob("*.parquet"))
    lookup = {}

    for path in files:
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=BATCH_SIZE,
                columns=[code_field, identity_field],
            ):
                data = batch.to_pydict()

                for code, identity in zip(
                    data[code_field],
                    data[identity_field],
                ):
                    assert code is not None
                    assert identity is not None
                    assert code not in lookup

                    lookup[code] = identity

    assert len(lookup) == count
    return lookup


def load_references():
    transactions = load_lookup(
        TRANSACTION_DIR,
        "transaction_code",
        "transaction_identity",
        755_122,
    )

    workorders = load_lookup(
        WORKORDER_DIR,
        "workorder_code",
        "workorder_identity",
        2_231_535,
    )

    print("Transaction lookup:", f"{len(transactions):,}")
    print("Work Order lookup:", f"{len(workorders):,}")
    print("Relationship lookup validation: PASS")

    return transactions, workorders


# ============================================================
# 5. TRANSFORM A SILVER BATCH
# ============================================================

def resolve_codes(codes, lookup):
    identities = []
    statuses = []

    for code in codes:
        if code is None or not str(code).strip():
            identities.append(None)
            statuses.append("MISSING_REFERENCE")
        elif code in lookup:
            identities.append(lookup[code])
            statuses.append("MATCHED")
        else:
            identities.append(None)
            statuses.append("UNMATCHED")

    return identities, statuses


def transform_batch(batch, transactions, workorders):
    source = pa.Table.from_batches([batch])

    transaction_ids, transaction_statuses = resolve_codes(
        source.column("trl_trans").to_pylist(),
        transactions,
    )

    workorder_ids, workorder_statuses = resolve_codes(
        source.column("trl_event").to_pylist(),
        workorders,
    )

    derived = {
        "transaction_identity": pa.array(
            transaction_ids, type=pa.int64()
        ),
        "transaction_match_status": pa.array(
            transaction_statuses, type=pa.string()
        ),
        "workorder_identity": pa.array(
            workorder_ids, type=pa.int64()
        ),
        "workorder_match_status": pa.array(
            workorder_statuses, type=pa.string()
        ),
    }

    columns = []

    for field in GOLD_SCHEMA:
        if field.name in derived:
            columns.append(derived[field.name])
        else:
            columns.append(
                source.column(SOURCE_MAPPING[field.name])
            )

    result = pa.Table.from_arrays(
        columns,
        schema=GOLD_SCHEMA,
    )

    assert result.num_rows == batch.num_rows
    return result


# ============================================================
# 6. BUILD GOLD FACT TRANSACTION DETAILS
# ============================================================

def build_gold_fact_transaction_details():
    print("=" * 68)
    print("STEP 7.10 - BUILD GOLD FACT_TRANSACTION_DETAILS")
    print("=" * 68)

    check_outputs()
    files = inspect_sources()
    transactions, workorders = load_references()

    STAGING_DIR.mkdir(parents=True, exist_ok=False)

    counts = Counter()
    identities = set()

    for index, source_path in enumerate(files):
        target_path = (
            STAGING_DIR / f"part-{index:05d}.parquet"
        )

        with pq.ParquetFile(str(source_path)) as reader:
            with pq.ParquetWriter(
                str(target_path),
                GOLD_SCHEMA,
                compression="snappy",
            ) as writer:
                for batch in reader.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=list(SOURCE_MAPPING.values()),
                ):
                    gold = transform_batch(
                        batch, transactions, workorders
                    )

                    for identity in gold.column(
                        "detail_identity"
                    ).to_pylist():
                        if identity is None or identity in identities:
                            raise ValueError(
                                f"Invalid detail identity: {identity}"
                            )
                        identities.add(identity)

                    for column, prefix in [
                        ("transaction_match_status", "transaction"),
                        ("workorder_match_status", "workorder"),
                    ]:
                        counts.update(
                            f"{prefix}_{status}"
                            for status in gold.column(column).to_pylist()
                        )

                    for column, key in [
                        ("negative_quantity_flag", "negative_qty"),
                        ("negative_price_flag", "negative_price"),
                        ("zero_quantity_flag", "zero_qty"),
                        ("zero_price_flag", "zero_price"),
                        ("silver_dq_issue", "dq_issue"),
                    ]:
                        counts[key] += sum(
                            x is True
                            for x in gold.column(column).to_pylist()
                        )

                    writer.write_table(gold)
                    counts["rows"] += gold.num_rows

        if (
            (index + 1) % 25 == 0
            or index + 1 == EXPECTED_FILES
        ):
            print(
                f"Files: {index + 1}/{EXPECTED_FILES}"
                f" | Rows: {counts['rows']:,}",
                flush=True,
            )

    # Validate row counts and relationship outcomes.
    assert counts["rows"] == EXPECTED_ROWS
    assert len(identities) == EXPECTED_ROWS

    expected = {
        "transaction_MATCHED": 2_300_536,
        "transaction_UNMATCHED": 1_391,
        "transaction_MISSING_REFERENCE": 0,
        "workorder_MATCHED": 809_009,
        "workorder_UNMATCHED": 135,
        "workorder_MISSING_REFERENCE": 1_492_783,
        "negative_qty": 44_053,
        "negative_price": 5_501,
        "zero_qty": 164_449,
        "zero_price": 61_515,
        "dq_issue": 0,
    }

    for key, value in expected.items():
        assert counts[key] == value, (
            f"Validation failed: {key}: "
            f"{counts[key]} != {value}"
        )

    staged_files = sorted(STAGING_DIR.glob("*.parquet"))
    assert len(staged_files) == EXPECTED_FILES

    verified_rows = 0

    for path in staged_files:
        assert pq.read_schema(path).equals(
            GOLD_SCHEMA,
            check_metadata=False,
        )
        verified_rows += pq.read_metadata(path).num_rows

    assert verified_rows == EXPECTED_ROWS

    print("\nSTAGED GOLD TRANSACTION DETAILS VALIDATION")
    print("Records:", f"{verified_rows:,}")
    print("Parquet files:", len(staged_files))
    print("Unique identities:", f"{len(identities):,}")
    print("Matched transactions:", f"{counts['transaction_MATCHED']:,}")
    print("Unmatched transactions:", f"{counts['transaction_UNMATCHED']:,}")
    print("Matched work orders:", f"{counts['workorder_MATCHED']:,}")
    print("Unmatched work orders:", counts["workorder_UNMATCHED"])
    print("Missing work-order refs:", f"{counts['workorder_MISSING_REFERENCE']:,}")
    print("Staged validation: PASS")

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(FINAL_DIR)

    STAGING_DIR.rename(FINAL_DIR)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "fact_transaction_details",
        "status": "COMPLETED",
        "source": SILVER_DIR.relative_to(ROOT).as_posix(),
        "output": FINAL_DIR.relative_to(ROOT).as_posix(),
        "rows": verified_rows,
        "columns": len(GOLD_SCHEMA),
        "parquet_files": len(staged_files),
        "unique_detail_identities": len(identities),
        "matched_transaction_rows": counts["transaction_MATCHED"],
        "unmatched_transaction_rows": counts["transaction_UNMATCHED"],
        "missing_transaction_reference_rows": (
            counts["transaction_MISSING_REFERENCE"]
        ),
        "matched_workorder_rows": counts["workorder_MATCHED"],
        "unmatched_workorder_rows": counts["workorder_UNMATCHED"],
        "missing_workorder_reference_rows": (
            counts["workorder_MISSING_REFERENCE"]
        ),
        "negative_quantity_rows": counts["negative_qty"],
        "negative_price_rows": counts["negative_price"],
        "zero_quantity_rows": counts["zero_qty"],
        "zero_price_rows": counts["zero_price"],
        "silver_dq_issue_rows": counts["dq_issue"],
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)

    with MANIFEST.open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print("\nGOLD FACT_TRANSACTION_DETAILS BUILD: SUCCESS")
    return manifest


# ============================================================
# 7. KEDRO PIPELINE
# ============================================================

def create_gold_fact_transaction_details_pipeline():
    return pipeline([
        node(
            func=build_gold_fact_transaction_details,
            inputs=None,
            outputs="gold_transaction_details_manifest",
            name="build_gold_fact_transaction_details",
            tags=["gold", "transactions", "details"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_transaction_details_manifest": MemoryDataset()
    })

    runner = SequentialRunner()
    runner.run(
        create_gold_fact_transaction_details_pipeline(),
        catalog,
    )

    result = catalog.load("gold_transaction_details_manifest")
    assert result["status"] == "COMPLETED"

    print("KEDRO GOLD TRANSACTION DETAILS PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
