"""
AssetFusion360 Capstone
Step 7.9 - Gold Transactions Fact Table

Source: Silver AMPS Transactions
Target: Gold fact_transactions

Rules:
- Preserve all 755,122 transaction records.
- Preserve unique transaction identities and business codes.
- Retain raw transaction codes for detail joins.
- Preserve missing optional values.
- Preserve Silver quality indicators.
- Never overwrite existing Gold output.
- Validate staged data before publication.
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


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[3]

SILVER_DIR = (
    ROOT / "data" / "03_silver_cleaned_data"
    / "amps_transactions"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

FINAL_DIR = GOLD_ROOT / "fact_transactions"
STAGING_DIR = GOLD_ROOT / "_staging_fact_transactions"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_fact_transactions_manifest.json"
)

EXPECTED_ROWS = 755_122
EXPECTED_FILES = 152
EXPECTED_SILVER_COLUMNS = 33
BATCH_SIZE = 10000


# ============================================================
# 2. GOLD FACT SCHEMA
# ============================================================

GOLD_SCHEMA = pa.schema([
    pa.field("transaction_identity", pa.int64(), nullable=False),
    pa.field("transaction_code", pa.string(), nullable=False),
    pa.field("transaction_code_numeric", pa.int64()),
    pa.field("transaction_description", pa.string()),
    pa.field("transaction_type", pa.string()),
    pa.field("transaction_status", pa.string()),
    pa.field("request_code", pa.string()),
    pa.field("request_identity", pa.int64()),
    pa.field("from_entity", pa.string()),
    pa.field("from_code", pa.string()),
    pa.field("to_entity", pa.string()),
    pa.field("to_code", pa.string()),
    pa.field("transaction_date", pa.timestamp("us")),
    pa.field("created_at", pa.timestamp("us")),
    pa.field("updated_at", pa.timestamp("us")),
    pa.field("update_count", pa.int64()),
    pa.field("dq_transaction_code_invalid", pa.bool_()),
    pa.field("dq_transaction_identity_invalid", pa.bool_()),
    pa.field("dq_transaction_request_invalid", pa.bool_()),
    pa.field("dq_transaction_updatecount_invalid", pa.bool_()),
    pa.field("dq_transaction_date_invalid", pa.bool_()),
    pa.field("dq_transaction_created_invalid", pa.bool_()),
    pa.field("dq_transaction_updated_invalid", pa.bool_()),
    pa.field("silver_dq_issue", pa.bool_()),
    pa.field("source_file", pa.string()),
    pa.field("source_batch", pa.string()),
    pa.field("source_ingested_at", pa.string()),
])


# Gold column -> Silver column
SOURCE_MAPPING = {
    "transaction_identity": "transaction_identity_int",
    "transaction_code": "tra_code",
    "transaction_code_numeric": "transaction_code_int",
    "transaction_description": "tra_desc",
    "transaction_type": "tra_type",
    "transaction_status": "tra_status",
    "request_code": "tra_req",
    "request_identity": "transaction_request_int",
    "from_entity": "tra_fromentity",
    "from_code": "tra_fromcode",
    "to_entity": "tra_toentity",
    "to_code": "tra_tocode",
    "transaction_date": "transaction_date_ts",
    "created_at": "transaction_created_ts",
    "updated_at": "transaction_updated_ts",
    "update_count": "transaction_updatecount_int",
    "dq_transaction_code_invalid": "dq_transaction_code_invalid",
    "dq_transaction_identity_invalid": "dq_transaction_identity_invalid",
    "dq_transaction_request_invalid": "dq_transaction_request_invalid",
    "dq_transaction_updatecount_invalid": "dq_transaction_updatecount_invalid",
    "dq_transaction_date_invalid": "dq_transaction_date_invalid",
    "dq_transaction_created_invalid": "dq_transaction_created_invalid",
    "dq_transaction_updated_invalid": "dq_transaction_updated_invalid",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
    "source_batch": "_bronze_batch_id",
    "source_ingested_at": "_bronze_ingested_at_utc",
}


# ============================================================
# 3. OUTPUT PROTECTION
# ============================================================

def check_outputs():
    if not SILVER_DIR.is_dir():
        raise FileNotFoundError(
            f"Missing Silver Transactions: {SILVER_DIR}"
        )

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing output must be preserved: {path}"
            )

    print("Gold output safety: PASS")


# ============================================================
# 4. SILVER SOURCE VALIDATION
# ============================================================

def inspect_sources():
    files = sorted(SILVER_DIR.glob("*.parquet"))

    assert len(files) == EXPECTED_FILES, (
        f"Expected {EXPECTED_FILES} Silver files, "
        f"found {len(files)}"
    )

    reference_schema = pq.read_schema(files[0])

    assert len(reference_schema) == EXPECTED_SILVER_COLUMNS

    total_rows = 0

    for path in files:
        schema = pq.read_schema(path)

        assert schema.equals(
            reference_schema,
            check_metadata=False
        ), f"Schema mismatch: {path.name}"

        total_rows += pq.read_metadata(path).num_rows

    assert total_rows == EXPECTED_ROWS

    assert len(GOLD_SCHEMA) == len(SOURCE_MAPPING)

    for gold_name, silver_name in SOURCE_MAPPING.items():
        assert silver_name in reference_schema.names, (
            f"Missing Silver column: {silver_name}"
        )

        silver_type = reference_schema.field(
            silver_name
        ).type

        gold_type = GOLD_SCHEMA.field(
            gold_name
        ).type

        assert silver_type.equals(gold_type), (
            f"Type mismatch for {gold_name}: "
            f"{silver_type} != {gold_type}"
        )

    print("Silver Transaction files:", len(files))
    print("Silver Transaction rows:", f"{total_rows:,}")
    print("Silver Transaction columns:", len(reference_schema))
    print("Gold Transaction columns:", len(GOLD_SCHEMA))
    print("Source schema validation: PASS")

    return files


# ============================================================
# 5. TRANSFORM SILVER BATCH
# ============================================================

def transform_batch(batch):
    source = pa.Table.from_batches([batch])

    columns = [
        source.column(SOURCE_MAPPING[field.name])
        for field in GOLD_SCHEMA
    ]

    result = pa.Table.from_arrays(
        columns,
        schema=GOLD_SCHEMA
    )

    assert result.num_rows == batch.num_rows

    return result


# ============================================================
# 6. BUILD GOLD FACT_TRANSACTIONS
# ============================================================

def build_gold_fact_transactions():
    print("=" * 68)
    print("STEP 7.9 - BUILD GOLD FACT_TRANSACTIONS")
    print("=" * 68)

    check_outputs()
    silver_files = inspect_sources()

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False
    )

    counts = Counter()

    identities = set()
    transaction_codes = set()

    quality_fields = [
        field.name
        for field in GOLD_SCHEMA
        if field.name.startswith("dq_")
        or field.name == "silver_dq_issue"
    ]

    for index, source_file in enumerate(silver_files):
        target_file = (
            STAGING_DIR / f"part-{index:05d}.parquet"
        )

        with pq.ParquetFile(str(source_file)) as reader:
            with pq.ParquetWriter(
                str(target_file),
                GOLD_SCHEMA,
                compression="snappy",
            ) as writer:
                for batch in reader.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=list(SOURCE_MAPPING.values()),
                ):
                    gold = transform_batch(batch)

                    identity_values = gold.column(
                        "transaction_identity"
                    ).to_pylist()

                    code_values = gold.column(
                        "transaction_code"
                    ).to_pylist()

                    for identity, code in zip(
                        identity_values,
                        code_values
                    ):
                        if identity is None:
                            raise ValueError(
                                "Missing transaction identity"
                            )

                        if identity in identities:
                            raise ValueError(
                                f"Duplicate identity: {identity}"
                            )

                        if code is None or not str(code).strip():
                            raise ValueError(
                                "Missing transaction code"
                            )

                        if code in transaction_codes:
                            raise ValueError(
                                f"Duplicate transaction code: {code}"
                            )

                        identities.add(identity)
                        transaction_codes.add(code)

                    for name in quality_fields:
                        counts[name] += sum(
                            value is True
                            for value in gold.column(name).to_pylist()
                        )

                    counts["rows"] += gold.num_rows
                    counts["missing_status"] += gold.column(
                        "transaction_status"
                    ).null_count

                    counts["missing_type"] += gold.column(
                        "transaction_type"
                    ).null_count

                    counts["missing_date"] += gold.column(
                        "transaction_date"
                    ).null_count

                    writer.write_table(gold)

        if (
            (index + 1) % 25 == 0
            or index + 1 == EXPECTED_FILES
        ):
            print(
                f"Files: {index + 1}/{EXPECTED_FILES}"
                f" | Rows: {counts['rows']:,}",
                flush=True
            )

    # ========================================================
    # 7. VALIDATE KEYS AND QUALITY FLAGS
    # ========================================================

    assert counts["rows"] == EXPECTED_ROWS
    assert len(identities) == EXPECTED_ROWS
    assert len(transaction_codes) == EXPECTED_ROWS

    for name in quality_fields:
        assert counts[name] == 0, (
            f"Unexpected quality flags: "
            f"{name}={counts[name]}"
        )

    # ========================================================
    # 8. VALIDATE STAGED PARQUET
    # ========================================================

    staged_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    assert len(staged_files) == EXPECTED_FILES

    verified_rows = 0

    for path in staged_files:
        schema = pq.read_schema(path)

        assert schema.equals(
            GOLD_SCHEMA,
            check_metadata=False
        ), f"Gold schema mismatch: {path.name}"

        verified_rows += pq.read_metadata(path).num_rows

    assert verified_rows == EXPECTED_ROWS

    print("\nSTAGED GOLD TRANSACTION VALIDATION")
    print("Transaction rows:", f"{verified_rows:,}")
    print("Parquet files:", len(staged_files))
    print("Unique transaction identities:", len(identities))
    print("Unique transaction codes:", len(transaction_codes))
    print("Gold columns:", len(GOLD_SCHEMA))
    print("Silver DQ flagged rows:", counts["silver_dq_issue"])
    print("Staged validation: PASS")

    # ========================================================
    # 9. PUBLISH WITHOUT OVERWRITING
    # ========================================================

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(
            f"Gold target appeared during build: {FINAL_DIR}"
        )

    STAGING_DIR.rename(FINAL_DIR)

    # ========================================================
    # 10. COMPLETION MANIFEST
    # ========================================================

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "fact_transactions",
        "status": "COMPLETED",
        "source": SILVER_DIR.relative_to(ROOT).as_posix(),
        "output": FINAL_DIR.relative_to(ROOT).as_posix(),
        "rows": verified_rows,
        "columns": len(GOLD_SCHEMA),
        "parquet_files": len(staged_files),
        "unique_transaction_identities": len(identities),
        "unique_transaction_codes": len(transaction_codes),
        "missing_status_rows": counts["missing_status"],
        "missing_type_rows": counts["missing_type"],
        "missing_date_rows": counts["missing_date"],
        "silver_dq_issue_rows": counts["silver_dq_issue"],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with MANIFEST.open(
        "x",
        encoding="utf-8"
    ) as handle:
        json.dump(
            manifest,
            handle,
            indent=2
        )

    print("\nGOLD FACT_TRANSACTIONS BUILD: SUCCESS")

    return manifest


# ============================================================
# 11. KEDRO PIPELINE
# ============================================================

def create_gold_fact_transactions_pipeline():
    return pipeline([
        node(
            func=build_gold_fact_transactions,
            inputs=None,
            outputs="gold_fact_transactions_manifest",
            name="build_gold_fact_transactions",
            tags=["gold", "transactions", "fact"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_fact_transactions_manifest": MemoryDataset()
    })

    runner = SequentialRunner()

    runner.run(
        create_gold_fact_transactions_pipeline(),
        catalog
    )

    result = catalog.load(
        "gold_fact_transactions_manifest"
    )

    assert result["status"] == "COMPLETED"

    print(
        "KEDRO GOLD FACT_TRANSACTIONS PIPELINE: SUCCESS"
    )


if __name__ == "__main__":
    run_kedro_pipeline()
