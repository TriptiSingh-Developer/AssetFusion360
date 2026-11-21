"""
AssetFusion360 - Gold Work Orders Fact Table
Step 7.7: Silver Work Orders to Gold

One row per work order.
Preserve ambiguous asset references.
Protect existing datasets from overwriting.
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
    / "amps_workorders"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

BRIDGE_FILE = (
    GOLD_ROOT / "bridge_asset_code"
    / "part-00000.parquet"
)

FINAL_DIR = GOLD_ROOT / "fact_workorders"
STAGING_DIR = GOLD_ROOT / "_staging_fact_workorders"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_fact_workorders_manifest.json"
)

EXPECTED_ROWS = 2_231_535
EXPECTED_FILES = 447
BATCH_SIZE = 10000


GOLD_SCHEMA = pa.schema([
    pa.field("workorder_identity", pa.int64()),
    pa.field("workorder_code", pa.string()),
    pa.field("asset_code", pa.string()),
    pa.field("resolved_asset_identity", pa.int64()),
    pa.field("asset_resolution_status", pa.string()),
    pa.field("asset_reference_ambiguous", pa.bool_()),
    pa.field("asset_code_placeholder", pa.bool_()),
    pa.field("workorder_type", pa.string()),
    pa.field("workorder_description", pa.string()),
    pa.field("workorder_priority", pa.string()),
    pa.field("workorder_class", pa.string()),
    pa.field("job_type", pa.string()),
    pa.field("workorder_date", pa.timestamp("us")),
    pa.field("reported_at", pa.timestamp("us")),
    pa.field("started_at", pa.timestamp("us")),
    pa.field("due_at", pa.timestamp("us")),
    pa.field("completed_at", pa.timestamp("us")),
    pa.field("created_at", pa.timestamp("us")),
    pa.field("updated_at", pa.timestamp("us")),
    pa.field("duration", pa.decimal128(20, 6)),
    pa.field("update_count", pa.int64()),
    pa.field("silver_dq_issue", pa.bool_()),
    pa.field("source_file", pa.string()),
])


SOURCE_MAPPING = {
    "workorder_identity": "wo_identity_int",
    "workorder_code": "evt_code",
    "asset_code": "evt_object",
    "workorder_type": "evt_type",
    "workorder_description": "evt_desc",
    "workorder_priority": "evt_priority",
    "workorder_class": "evt_class",
    "job_type": "evt_jobtype",
    "workorder_date": "evt_date_ts",
    "reported_at": "evt_reported_ts",
    "started_at": "evt_start_ts",
    "due_at": "evt_due_ts",
    "completed_at": "evt_completed_ts",
    "created_at": "evt_created_ts",
    "updated_at": "evt_updated_ts",
    "duration": "wo_duration_decimal",
    "update_count": "wo_updatecount_int",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
}


def check_outputs():
    if not SILVER_DIR.is_dir():
        raise FileNotFoundError(SILVER_DIR)

    if not BRIDGE_FILE.is_file():
        raise FileNotFoundError(BRIDGE_FILE)

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing artifact must be preserved: {path}"
            )

    print("Output safety: PASS")


def inspect_sources():
    files = sorted(SILVER_DIR.glob("*.parquet"))

    if len(files) != EXPECTED_FILES:
        raise ValueError("Unexpected Silver file count")

    schema = pq.read_schema(files[0])
    total = 0

    for path in files:
        current_schema = pq.read_schema(path)

        if not current_schema.equals(
            schema, check_metadata=False
        ):
            raise ValueError(
                f"Schema mismatch: {path.name}"
            )

        total += pq.read_metadata(path).num_rows

    if total != EXPECTED_ROWS:
        raise ValueError(
            f"Unexpected Silver row count: {total}"
        )

    for gold_name, silver_name in SOURCE_MAPPING.items():
        if silver_name not in schema.names:
            raise ValueError(
                f"Missing field: {silver_name}"
            )

        actual_type = schema.field(silver_name).type
        expected_type = GOLD_SCHEMA.field(gold_name).type

        if not actual_type.equals(expected_type):
            raise TypeError(
                f"{silver_name}: {actual_type} "
                f"!= {expected_type}"
            )

    print("Silver files:", len(files))
    print("Silver rows:", f"{total:,}")
    print("Silver columns:", len(schema))
    print("Gold columns:", len(GOLD_SCHEMA))
    print("Source schema: PASS")

    return files


def load_asset_bridge():
    bridge = {}

    with pq.ParquetFile(str(BRIDGE_FILE)) as reader:
        if reader.metadata.num_rows != 148_521:
            raise ValueError("Unexpected bridge row count")

        for batch in reader.iter_batches(
            batch_size=BATCH_SIZE,
            columns=[
                "asset_code",
                "resolved_asset_identity",
                "resolution_status",
                "is_ambiguous",
                "is_placeholder",
            ],
        ):
            for row in batch.to_pylist():
                code = row["asset_code"]
                status = row["resolution_status"]
                identity = row["resolved_asset_identity"]

                if code is None or code in bridge:
                    raise ValueError(
                        f"Invalid bridge code: {code}"
                    )

                if status not in {"UNIQUE", "AMBIGUOUS"}:
                    raise ValueError(
                        f"Invalid bridge status: {status}"
                    )

                if status == "UNIQUE":
                    assert identity is not None
                    assert row["is_ambiguous"] is False
                else:
                    assert identity is None
                    assert row["is_ambiguous"] is True

                bridge[code] = {
                    "identity": identity,
                    "status": status,
                    "ambiguous": row["is_ambiguous"],
                    "placeholder": row["is_placeholder"],
                }

    assert len(bridge) == 148_521
    assert bridge["B223PA"]["status"] == "AMBIGUOUS"
    assert bridge["B223PA"]["identity"] is None

    print("Bridge codes:", f"{len(bridge):,}")
    print("Ambiguous asset handling: PASS")

    return bridge


def transform_batch(batch, bridge):
    source = pa.Table.from_batches([batch])

    asset_codes = source.column(
        "evt_object"
    ).to_pylist()

    resolved = []
    statuses = []
    ambiguous = []
    placeholders = []

    for code in asset_codes:
        if code not in bridge:
            raise ValueError(
                f"Unmatched work-order asset code: {code!r}"
            )

        match = bridge[code]

        resolved.append(match["identity"])
        statuses.append(match["status"])
        ambiguous.append(match["ambiguous"])
        placeholders.append(match["placeholder"])

    derived = {
        "resolved_asset_identity": pa.array(
            resolved, type=pa.int64()
        ),
        "asset_resolution_status": pa.array(
            statuses, type=pa.string()
        ),
        "asset_reference_ambiguous": pa.array(
            ambiguous, type=pa.bool_()
        ),
        "asset_code_placeholder": pa.array(
            placeholders, type=pa.bool_()
        ),
    }

    columns = []

    for field in GOLD_SCHEMA:
        name = field.name

        if name in derived:
            columns.append(derived[name])
        else:
            columns.append(
                source.column(SOURCE_MAPPING[name])
            )

    result = pa.Table.from_arrays(
        columns, schema=GOLD_SCHEMA
    )

    assert result.num_rows == batch.num_rows

    return result


def build_gold_fact_workorders():
    print("=" * 68)
    print("STEP 7.7 - BUILD GOLD FACT_WORKORDERS")
    print("=" * 68)

    check_outputs()
    silver_files = inspect_sources()
    bridge = load_asset_bridge()

    STAGING_DIR.mkdir(
        parents=True, exist_ok=False
    )

    counters = Counter()

    for index, source_file in enumerate(silver_files):
        output_file = (
            STAGING_DIR / f"part-{index:05d}.parquet"
        )

        with pq.ParquetFile(str(source_file)) as reader:
            with pq.ParquetWriter(
                str(output_file),
                GOLD_SCHEMA,
                compression="snappy",
            ) as writer:
                for batch in reader.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=list(SOURCE_MAPPING.values()),
                ):
                    gold = transform_batch(batch, bridge)

                    writer.write_table(gold)
                    counters["rows"] += gold.num_rows

                    for status in gold.column(
                        "asset_resolution_status"
                    ).to_pylist():
                        counters[status] += 1

                    counters["null_priority"] += (
                        gold.column(
                            "workorder_priority"
                        ).null_count
                    )

                    counters["null_completed"] += (
                        gold.column(
                            "completed_at"
                        ).null_count
                    )

                    counters["dq_issue"] += sum(
                        value is True
                        for value in gold.column(
                            "silver_dq_issue"
                        ).to_pylist()
                    )

        if (
            (index + 1) % 25 == 0
            or index + 1 == EXPECTED_FILES
        ):
            print(
                f"Files: {index + 1}/{EXPECTED_FILES}"
                f" | Rows: {counters['rows']:,}",
                flush=True,
            )

    assert counters["rows"] == EXPECTED_ROWS
    assert counters["UNIQUE"] == 2_231_135
    assert counters["AMBIGUOUS"] == 400
    assert counters["null_priority"] == 994_238
    assert counters["null_completed"] == 25_585
    assert counters["dq_issue"] == 0

    staged_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    assert len(staged_files) == EXPECTED_FILES

    verified_rows = 0

    for path in staged_files:
        assert pq.read_schema(path).equals(
            GOLD_SCHEMA, check_metadata=False
        )
        verified_rows += pq.read_metadata(path).num_rows

    assert verified_rows == EXPECTED_ROWS

    print("\nSTAGED GOLD VALIDATION")
    print("Verified rows:", f"{verified_rows:,}")
    print("Unique asset mappings:", f"{counters['UNIQUE']:,}")
    print("Ambiguous asset mappings:", counters["AMBIGUOUS"])
    print("Staged validation: PASS")

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(
            f"Gold output appeared: {FINAL_DIR}"
        )

    STAGING_DIR.rename(FINAL_DIR)

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "fact_workorders",
        "status": "COMPLETED",
        "rows": verified_rows,
        "columns": len(GOLD_SCHEMA),
        "parquet_files": len(staged_files),
        "unique_asset_mapping_rows": counters["UNIQUE"],
        "ambiguous_asset_mapping_rows": counters["AMBIGUOUS"],
        "missing_priority_rows": counters["null_priority"],
        "missing_completion_rows": counters["null_completed"],
        "silver_dq_issue_rows": counters["dq_issue"],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True, exist_ok=True
    )

    with MANIFEST.open(
        "x", encoding="utf-8"
    ) as f:
        json.dump(manifest, f, indent=2)

    print("GOLD FACT_WORKORDERS BUILD: SUCCESS")

    return manifest


def create_gold_fact_workorders_pipeline():
    return pipeline([
        node(
            func=build_gold_fact_workorders,
            inputs=None,
            outputs="gold_fact_workorders_manifest",
            name="build_gold_fact_workorders",
            tags=["gold", "workorders", "fact"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_fact_workorders_manifest": MemoryDataset()
    })

    runner = SequentialRunner()

    runner.run(
        create_gold_fact_workorders_pipeline(),
        catalog,
    )

    result = catalog.load(
        "gold_fact_workorders_manifest"
    )

    assert result["status"] == "COMPLETED"

    print(
        "KEDRO GOLD FACT_WORKORDERS PIPELINE: SUCCESS"
    )


if __name__ == "__main__":
    run_kedro_pipeline()
