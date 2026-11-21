
"""
AssetFusion360 Capstone
Step 7.6 - Gold Asset-Code Bridge

Source: Gold dim_asset
Target: Gold bridge_asset_code

Grain: One row per distinct asset code.

Preserves all candidate asset identities.
Never automatically resolves ambiguous codes.
Does not modify existing Bronze, Silver, or Gold data.
"""

import gc
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


ROOT = Path(__file__).resolve().parents[3]

SOURCE = (
    ROOT / "data" / "04_gold_analytics"
    / "dim_asset" / "part-00000.parquet"
)

GOLD_ROOT = ROOT / "data" / "04_gold_analytics"

FINAL_DIR = GOLD_ROOT / "bridge_asset_code"
STAGING_DIR = GOLD_ROOT / "_staging_bridge_asset_code"

FINAL_FILE = FINAL_DIR / "part-00000.parquet"
STAGING_FILE = STAGING_DIR / "part-00000.parquet"

MANIFEST = (
    ROOT / "reports" / "data_profiling_reports"
    / "gold_bridge_asset_code_manifest.json"
)

EXPECTED_ASSET_ROWS = 148_522
EXPECTED_BRIDGE_ROWS = 148_521
BATCH_SIZE = 5000


GOLD_SCHEMA = pa.schema([
    pa.field(
        "asset_code", pa.string(), nullable=False
    ),
    pa.field(
        "asset_match_count", pa.int32(), nullable=False
    ),
    pa.field(
        "resolved_asset_identity", pa.int64()
    ),
    pa.field(
        "candidate_asset_identities",
        pa.list_(pa.int64())
    ),
    pa.field(
        "resolution_status",
        pa.string(), nullable=False
    ),
    pa.field(
        "is_ambiguous", pa.bool_(), nullable=False
    ),
    pa.field(
        "is_placeholder", pa.bool_(), nullable=False
    ),
])


def check_outputs():
    if not SOURCE.is_file():
        raise FileNotFoundError(
            f"Gold Asset Dimension missing: {SOURCE}"
        )

    for path in [FINAL_DIR, STAGING_DIR, MANIFEST]:
        if path.exists():
            raise FileExistsError(
                f"Existing artifact must be preserved: {path}"
            )

    print("Output safety: PASS")


def inspect_source():
    with pq.ParquetFile(str(SOURCE)) as reader:
        schema = reader.schema_arrow
        rows = reader.metadata.num_rows

    assert rows == EXPECTED_ASSET_ROWS, (
        f"Unexpected asset count: {rows}"
    )

    required = {
        "asset_identity": pa.int64(),
        "asset_code": pa.string(),
        "asset_code_match_count": pa.int32(),
        "asset_code_ambiguous": pa.bool_(),
        "asset_code_placeholder": pa.bool_(),
    }

    for name, expected_type in required.items():
        assert name in schema.names, (
            f"Missing Gold dimension column: {name}"
        )

        assert schema.field(name).type.equals(
            expected_type
        ), f"Unexpected type: {name}"

    print("Source dim_asset records:", f"{rows:,}")
    print("Source schema: PASS")


def build_gold_asset_bridge():
    print("=" * 65)
    print("STEP 7.6 - BUILD GOLD ASSET-CODE BRIDGE")
    print("=" * 65)

    check_outputs()
    inspect_source()

    code_to_assets = defaultdict(list)
    placeholder_by_code = {}
    source_match_counts = {}
    source_ambiguities = {}
    identities = set()
    total_assets = 0

    with pq.ParquetFile(str(SOURCE)) as reader:
        for batch in reader.iter_batches(
            batch_size=BATCH_SIZE,
            columns=[
                "asset_identity",
                "asset_code",
                "asset_code_match_count",
                "asset_code_ambiguous",
                "asset_code_placeholder",
            ],
        ):
            for row in batch.to_pylist():
                total_assets += 1

                identity = row["asset_identity"]
                code = row["asset_code"]

                assert identity is not None
                assert identity not in identities
                assert code is not None

                identities.add(identity)
                code_to_assets[code].append(identity)

                placeholder_by_code[code] = (
                    placeholder_by_code.get(code, False)
                    or row["asset_code_placeholder"]
                )

                if code in source_match_counts:
                    assert (
                        source_match_counts[code]
                        == row["asset_code_match_count"]
                    )
                    assert (
                        source_ambiguities[code]
                        == row["asset_code_ambiguous"]
                    )
                else:
                    source_match_counts[code] = (
                        row["asset_code_match_count"]
                    )
                    source_ambiguities[code] = (
                        row["asset_code_ambiguous"]
                    )

    assert total_assets == EXPECTED_ASSET_ROWS
    assert len(identities) == EXPECTED_ASSET_ROWS
    assert len(code_to_assets) == EXPECTED_BRIDGE_ROWS

    bridge_rows = []

    for code in sorted(code_to_assets):
        candidates = sorted(code_to_assets[code])
        count = len(candidates)

        assert count == source_match_counts[code]
        assert (count > 1) == source_ambiguities[code]

        is_ambiguous = count > 1

        bridge_rows.append({
            "asset_code": code,
            "asset_match_count": count,
            "resolved_asset_identity": (
                candidates[0] if count == 1 else None
            ),
            "candidate_asset_identities": candidates,
            "resolution_status": (
                "AMBIGUOUS" if is_ambiguous else "UNIQUE"
            ),
            "is_ambiguous": is_ambiguous,
            "is_placeholder": bool(
                placeholder_by_code[code]
            ),
        })

    ambiguous = [
        row for row in bridge_rows
        if row["is_ambiguous"]
    ]

    assert len(ambiguous) == 1
    assert ambiguous[0]["asset_code"] == "B223PA"
    assert ambiguous[0]["asset_match_count"] == 2
    assert ambiguous[0]["resolved_asset_identity"] is None
    assert (
        ambiguous[0]["candidate_asset_identities"]
        == [5514, 6133]
    )

    assert sum(
        row["asset_match_count"] for row in bridge_rows
    ) == EXPECTED_ASSET_ROWS

    print("Source asset records:", f"{total_assets:,}")
    print("Distinct asset codes:", f"{len(bridge_rows):,}")
    print("Ambiguous codes:", len(ambiguous))
    print("B223PA candidates: [5514, 6133]")

    # Publish only after verifying the in-memory bridge.
    STAGING_DIR.mkdir(parents=True, exist_ok=False)

    table = pa.Table.from_pylist(
        bridge_rows,
        schema=GOLD_SCHEMA,
    )

    pq.write_table(
        table,
        str(STAGING_FILE),
        compression="snappy",
    )

    del table
    gc.collect()

    with pq.ParquetFile(str(STAGING_FILE)) as reader:
        assert (
            reader.metadata.num_rows
            == EXPECTED_BRIDGE_ROWS
        )
        assert reader.schema_arrow.equals(
            GOLD_SCHEMA,
            check_metadata=False,
        )

        verified_rows = 0
        seen_codes = set()
        ambiguous_count = 0

        for batch in reader.iter_batches(
            batch_size=BATCH_SIZE
        ):
            for row in batch.to_pylist():
                verified_rows += 1
                code = row["asset_code"]

                assert code not in seen_codes
                seen_codes.add(code)

                candidates = row[
                    "candidate_asset_identities"
                ]

                assert len(candidates) == (
                    row["asset_match_count"]
                )

                if row["is_ambiguous"]:
                    ambiguous_count += 1
                    assert (
                        row["resolved_asset_identity"]
                        is None
                    )
                else:
                    assert (
                        row["resolved_asset_identity"]
                        == candidates[0]
                    )

        assert verified_rows == EXPECTED_BRIDGE_ROWS
        assert ambiguous_count == 1

    gc.collect()

    if FINAL_DIR.exists():
        raise FileExistsError(
            "Final bridge directory appeared during build"
        )

    STAGING_DIR.rename(FINAL_DIR)

    assert FINAL_FILE.is_file()

    manifest_data = {
        "project": "AssetFusion360",
        "layer": "Gold",
        "table": "bridge_asset_code",
        "status": "COMPLETED",
        "source": SOURCE.relative_to(ROOT).as_posix(),
        "output": FINAL_FILE.relative_to(ROOT).as_posix(),
        "rows": EXPECTED_BRIDGE_ROWS,
        "columns": len(GOLD_SCHEMA),
        "source_asset_rows": EXPECTED_ASSET_ROWS,
        "unique_codes": EXPECTED_BRIDGE_ROWS - 1,
        "ambiguous_codes": 1,
        "ambiguous_code": "B223PA",
        "candidate_asset_identities": [5514, 6133],
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
        json.dump(
            manifest_data,
            handle,
            indent=2,
        )

    print("\nGOLD BRIDGE SUMMARY")
    print("Bridge rows:", f"{verified_rows:,}")
    print("Gold columns:", len(GOLD_SCHEMA))
    print("Ambiguous codes:", ambiguous_count)
    print("Unique asset codes:", f"{verified_rows - 1:,}")
    print("Validation: PASS")
    print("GOLD ASSET BRIDGE BUILD: SUCCESS")

    return manifest_data


def create_gold_asset_bridge_pipeline():
    return pipeline([
        node(
            func=build_gold_asset_bridge,
            inputs=None,
            outputs="gold_asset_bridge_manifest",
            name="build_gold_asset_bridge",
            tags=["gold", "assets", "bridge"],
        )
    ])


def run_kedro_pipeline():
    catalog = DataCatalog({
        "gold_asset_bridge_manifest": MemoryDataset()
    })

    runner = SequentialRunner()

    runner.run(
        create_gold_asset_bridge_pipeline(),
        catalog,
    )

    result = catalog.load(
        "gold_asset_bridge_manifest"
    )

    assert result["status"] == "COMPLETED"

    print("KEDRO GOLD ASSET BRIDGE PIPELINE: SUCCESS")


if __name__ == "__main__":
    run_kedro_pipeline()
