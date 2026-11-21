"""
AssetFusion360 Capstone
Step 7.8 - Gold Labor Fact Table

Source: Silver AMPS Labor
Lookup: Gold fact_workorders
Target: Gold fact_labor

Rules:
- One Gold row per Silver Labor row.
- Preserve all 3,230,423 records.
- Preserve unmatched work-order references.
- Preserve negative and zero financial values.
- Never modify Bronze, Silver, or existing Gold tables.
- Validate staged output before publication.
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
    / "amps_labor"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

WORKORDER_DIR = GOLD_ROOT / "fact_workorders"

FINAL_DIR = GOLD_ROOT / "fact_labor"
STAGING_DIR = GOLD_ROOT / "_staging_fact_labor"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_fact_labor_manifest.json"
)

EXPECTED_ROWS = 3_230_423
EXPECTED_FILES = 647
EXPECTED_WORKORDERS = 2_231_535

BATCH_SIZE = 10000


# ============================================================
# GOLD FACT SCHEMA
# ============================================================

GOLD_SCHEMA = pa.schema([
    pa.field("labor_identity", pa.int64(), nullable=False),
    pa.field("workorder_code", pa.string()),
    pa.field("workorder_identity", pa.int64()),
    pa.field("workorder_match_status", pa.string()),
    pa.field("labor_date", pa.timestamp("us")),
    pa.field("labor_activity", pa.string()),
    pa.field("trade", pa.string()),
    pa.field("occupation_type", pa.string()),
    pa.field("labor_hours", pa.decimal128(20, 6)),
    pa.field("labor_rate", pa.decimal128(20, 6)),
    pa.field("labor_cost", pa.decimal128(20, 6)),
    pa.field("correction_code", pa.string()),
    pa.field("labor_description", pa.string()),
    pa.field("labor_code", pa.string()),
    pa.field("negative_hours_flag", pa.bool_()),
    pa.field("negative_cost_flag", pa.bool_()),
    pa.field("zero_rate_flag", pa.bool_()),
    pa.field("silver_dq_issue", pa.bool_()),
    pa.field("source_file", pa.string()),
    pa.field("source_batch", pa.string()),
])


SOURCE_MAPPING = {
    "labor_identity": "labor_identity_int",
    "workorder_code": "boo_event",
    "labor_date": "labor_date_ts",
    "labor_activity": "boo_act",
    "trade": "boo_trade",
    "occupation_type": "boo_octype",
    "labor_hours": "labor_hours_decimal",
    "labor_rate": "labor_rate_decimal",
    "labor_cost": "labor_cost_decimal",
    "correction_code": "boo_correction",
    "labor_description": "boo_desc",
    "labor_code": "boo_code",
    "negative_hours_flag": "dq_negative_labor_hours",
    "negative_cost_flag": "dq_negative_labor_cost",
    "zero_rate_flag": "dq_zero_labor_rate",
    "silver_dq_issue": "dq_any_issue",
    "source_file": "_bronze_source_file",
    "source_batch": "_bronze_batch_id",
}


# ============================================================
# OUTPUT PROTECTION
# ============================================================

def check_outputs():
    if not SILVER_DIR.is_dir():
        raise FileNotFoundError(SILVER_DIR)

    if not WORKORDER_DIR.is_dir():
        raise FileNotFoundError(WORKORDER_DIR)

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing artifact must be preserved: {path}"
            )

    print("Output safety: PASS")


# ============================================================
# SOURCE SCHEMA VALIDATION
# ============================================================

def inspect_sources():
    files = sorted(SILVER_DIR.glob("*.parquet"))

    assert len(files) == EXPECTED_FILES, (
        f"Unexpected Labor file count: {len(files)}"
    )

    reference_schema = pq.read_schema(files[0])
    total_rows = 0

    for path in files:
        current_schema = pq.read_schema(path)

        assert current_schema.equals(
            reference_schema,
            check_metadata=False,
        ), f"Schema mismatch: {path.name}"

        total_rows += pq.read_metadata(path).num_rows

    assert total_rows == EXPECTED_ROWS
    assert len(reference_schema) == 38

    for gold_name, silver_name in SOURCE_MAPPING.items():
        assert silver_name in reference_schema.names, (
            f"Missing Silver field: {silver_name}"
        )

        source_type = reference_schema.field(
            silver_name
        ).type

        gold_type = GOLD_SCHEMA.field(
            gold_name
        ).type

        assert source_type.equals(gold_type), (
            f"Type mismatch: {silver_name}: "
            f"{source_type} != {gold_type}"
        )

    print("Silver Labor files:", len(files))
    print("Silver Labor rows:", f"{total_rows:,}")
    print("Silver Labor columns:", len(reference_schema))
    print("Gold Labor columns:", len(GOLD_SCHEMA))
    print("Labor source schema: PASS")

    return files


# ============================================================
# LOAD WORK-ORDER BUSINESS KEY LOOKUP
# ============================================================

def load_workorder_lookup():
    files = sorted(WORKORDER_DIR.glob("*.parquet"))

    assert len(files) == 447

    lookup = {}
    total_rows = 0

    for path in files:
        with pq.ParquetFile(str(path)) as reader:
            for batch in reader.iter_batches(
                batch_size=BATCH_SIZE,
                columns=[
                    "workorder_code",
                    "workorder_identity",
                ],
            ):
                for row in batch.to_pylist():
                    total_rows += 1

                    code = row["workorder_code"]
                    identity = row["workorder_identity"]

                    assert code is not None
                    assert identity is not None
                    assert code not in lookup, (
                        f"Duplicate work-order code: {code}"
                    )

                    lookup[code] = identity

    assert total_rows == EXPECTED_WORKORDERS
    assert len(lookup) == EXPECTED_WORKORDERS

    print(
        "Gold Work Order lookup:",
        f"{len(lookup):,}"
    )
    print("Work Order business key uniqueness: PASS")

    return lookup


# ============================================================
# TRANSFORM ONE SILVER LABOR BATCH
# ============================================================

def transform_batch(batch, workorder_lookup):
    source = pa.Table.from_batches([batch])

    references = source.column(
        "boo_event"
    ).to_pylist()

    resolved_ids = []
    statuses = []

    for reference in references:
        if reference is None or not str(reference).strip():
            resolved_ids.append(None)
            statuses.append("MISSING_REFERENCE")

        elif reference in workorder_lookup:
            resolved_ids.append(
                workorder_lookup[reference]
            )
            statuses.append("MATCHED")

        else:
            resolved_ids.append(None)
            statuses.append("UNMATCHED")

    derived = {
        "workorder_identity": pa.array(
            resolved_ids,
            type=pa.int64(),
        ),
        "workorder_match_status": pa.array(
            statuses,
            type=pa.string(),
        ),
    }

    columns = []

    for field in GOLD_SCHEMA:
        name = field.name

        if name in derived:
            column = derived[name]
        else:
            column = source.column(
                SOURCE_MAPPING[name]
            )

        columns.append(column)

    result = pa.Table.from_arrays(
        columns,
        schema=GOLD_SCHEMA,
    )

    assert result.num_rows == batch.num_rows

    return result


# ============================================================
# BUILD GOLD FACT_LABOR
# ============================================================

def build_gold_fact_labor():
    print("=" * 68)
    print("STEP 7.8 - BUILD GOLD FACT_LABOR")
    print("=" * 68)

    check_outputs()

    silver_files = inspect_sources()
    workorder_lookup = load_workorder_lookup()

    # Only create staging after source validation.
    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=False,
    )

    counts = Counter()

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
                    gold = transform_batch(
                        batch,
                        workorder_lookup,
                    )

                    writer.write_table(gold)

                    counts["rows"] += gold.num_rows

                    for status in gold.column(
                        "workorder_match_status"
                    ).to_pylist():
                        counts[status] += 1

                    for column, name in [
                        ("negative_hours_flag", "negative_hours"),
                        ("negative_cost_flag", "negative_cost"),
                        ("zero_rate_flag", "zero_rate"),
                        ("silver_dq_issue", "dq_issues"),
                    ]:
                        counts[name] += sum(
                            value is True
                            for value in gold.column(
                                column
                            ).to_pylist()
                        )

        if (
            (index + 1) % 25 == 0
            or index + 1 == EXPECTED_FILES
        ):
            print(
                f"Files: {index + 1}/{EXPECTED_FILES}"
                f" | Rows: {counts['rows']:,}",
                flush=True,
            )

    # ========================================================
    # VALIDATE BUSINESS COUNTS
    # ========================================================

    assert counts["rows"] == EXPECTED_ROWS

    assert counts["MATCHED"] == 3_217_345
    assert counts["UNMATCHED"] == 13_078
    assert counts["MISSING_REFERENCE"] == 0

    assert counts["negative_hours"] == 74_609
    assert counts["negative_cost"] == 71_374
    assert counts["zero_rate"] == 188_165

    assert counts["dq_issues"] == 0

    # ========================================================
    # VALIDATE STAGED PARQUET
    # ========================================================

    staged_files = sorted(
        STAGING_DIR.glob("*.parquet")
    )

    assert len(staged_files) == EXPECTED_FILES

    verified_rows = 0

    for path in staged_files:
        assert pq.read_schema(path).equals(
            GOLD_SCHEMA,
            check_metadata=False,
        ), f"Gold schema mismatch: {path.name}"

        verified_rows += pq.read_metadata(
            path
        ).num_rows

    assert verified_rows == EXPECTED_ROWS

    print("\nSTAGED GOLD LABOR VALIDATION")
    print("Labor rows:", f"{verified_rows:,}")
    print("Parquet files:", len(staged_files))
    print("Matched Work Orders:", f"{counts['MATCHED']:,}")
    print("Unmatched Work Orders:", f"{counts['UNMATCHED']:,}")
    print("Negative hours:", f"{counts['negative_hours']:,}")
    print("Negative costs:", f"{counts['negative_cost']:,}")
    print("Zero rates:", f"{counts['zero_rate']:,}")
    print("Staged validation: PASS")

    # ========================================================
    # PUBLISH WITHOUT OVERWRITING
    # ========================================================

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(FINAL_DIR)

    STAGING_DIR.rename(FINAL_DIR)

    # ========================================================
    # WRITE COMPLETION MANIFEST
    # ========================================================

    manifest = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "fact_labor",
        "status": "COMPLETED",
        "source": SILVER_DIR.relative_to(ROOT).as_posix(),
        "workorder_source": (
            WORKORDER_DIR.relative_to(ROOT).as_posix()
        ),
        "output": FINAL_DIR.relative_to(ROOT).as_posix(),
        "rows": verified_rows,
        "columns": len(GOLD_SCHEMA),
        "parquet_files": len(staged_files),
        "matched_workorder_rows": counts["MATCHED"],
        "unmatched_workorder_rows": counts["UNMATCHED"],
        "missing_reference_rows": counts["MISSING_REFERENCE"],
        "negative_hours_rows": counts["negative_hours"],
        "negative_cost_rows": counts["negative_cost"],
        "zero_rate_rows": counts["zero_rate"],
        "silver_dq_issue_rows": counts["dq_issues"],
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST.open(
        "x",
        encoding="utf-8",
    ) as handle:
        json.dump(manifest, handle, indent=2)

    print("\nGOLD FACT_LABOR BUILD: SUCCESS")

    return manifest


# ============================================================
# KEDRO PIPELINE
# ============================================================

def create_gold_fact_labor_pipeline():
    return pipeline([
        node(
            func=build_gold_fact_labor,
            inputs=None,
            outputs="gold_fact_labor_manifest",
            name="build_gold_fact_labor",
            tags=["gold", "labor", "fact"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_fact_labor_manifest": MemoryDataset()
    })

    runner = SequentialRunner()

    runner.run(
        create_gold_fact_labor_pipeline(),
        catalog,
    )

    result = catalog.load("gold_fact_labor_manifest")

    assert result["status"] == "COMPLETED"

    print("KEDRO GOLD FACT_LABOR PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
