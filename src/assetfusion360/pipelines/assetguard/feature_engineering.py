"""
AssetFusion360 Capstone
AssetGuard - Predictive Maintenance

Step 9.7: Temporal Feature Engineering

Purpose:
    Generate asset-level historical features and
    a 30-day corrective maintenance prediction target.

Source:
    Gold fact_workorders

Output:
    AssetGuard ML-ready Parquet dataset.

Technology:
    Python, DuckDB, Parquet
"""

from pathlib import Path
import json
import time

import duckdb


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[4]

GOLD_DIR = ROOT / "data" / "04_gold_analytics"

WORKORDER_DIR = GOLD_DIR / "fact_workorders"

OUTPUT_DIR = (
    ROOT
    / "data"
    / "05_predictive_maintenance_features"
)

OUTPUT_FILE = (
    OUTPUT_DIR / "assetguard_training_features.parquet"
)

VALIDATION_FILE = (
    OUTPUT_DIR / "assetguard_feature_validation.json"
)

TEMP_DIR = OUTPUT_DIR / "duckdb_temp"

START_DATE = "2015-01-01"
LAST_SNAPSHOT = "2026-09-01"

OBSERVATION_DAYS = 365
PREDICTION_DAYS = 30


# ============================================================
# 2. OUTPUT SAFETY
# ============================================================

def check_environment():

    if not WORKORDER_DIR.exists():
        raise FileNotFoundError(WORKORDER_DIR)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    for path in [OUTPUT_FILE, VALIDATION_FILE]:

        if path.exists():
            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    print("Environment validation: PASS")


# ============================================================
# 3. LOAD GOLD DATA
# ============================================================

def load_workorders(con):

    files = sorted(
        WORKORDER_DIR.rglob("*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            "No work-order Parquet files found."
        )

    print(f"Parquet files: {len(files):,}")

    con.read_parquet(
        [str(file.resolve()) for file in files]
    ).create_view("gold_workorders")

    con.execute("""
        CREATE TEMP TABLE eligible_workorders AS

        SELECT
            workorder_identity,
            resolved_asset_identity AS asset_identity,
            CAST(created_at AS DATE) AS event_date,

            CASE
                WHEN job_type = 'CM' THEN 1
                ELSE 0
            END AS is_corrective

        FROM gold_workorders

        WHERE asset_resolution_status = 'UNIQUE'

          AND resolved_asset_identity IS NOT NULL

          AND created_at IS NOT NULL

          AND workorder_identity IS NOT NULL
    """)

    count = con.sql("""
        SELECT COUNT(*)
        FROM eligible_workorders
    """).fetchall()[0][0]

    print(f"Eligible work orders: {count:,}")


# ============================================================
# 4. DAILY MAINTENANCE AGGREGATION
# ============================================================

def aggregate_daily_events(con):

    con.execute("""
        CREATE TEMP TABLE daily_events AS

        SELECT
            asset_identity,
            event_date,

            COUNT(*) AS wo_count,

            SUM(is_corrective) AS cm_count

        FROM eligible_workorders

        GROUP BY
            asset_identity,
            event_date
    """)

    print("Daily maintenance aggregation: PASS")


# ============================================================
# 5. CREATE MONTHLY PREDICTION SNAPSHOTS
# ============================================================

def create_snapshots(con):

    con.execute(f"""
        CREATE TEMP TABLE snapshots AS

        SELECT
            CAST(snapshot_date AS DATE)
                AS snapshot_date

        FROM generate_series(
            DATE '{START_DATE}',
            DATE '{LAST_SNAPSHOT}',
            INTERVAL '1 month'
        ) AS t(snapshot_date)
    """)

    count = con.sql("""
        SELECT COUNT(*)
        FROM snapshots
    """).fetchall()[0][0]

    print(f"Monthly snapshots: {count:,}")


# ============================================================
# 6. HISTORICAL FEATURE ENGINEERING
# ============================================================

def build_historical_features(con):

    con.execute("""
        CREATE TEMP TABLE historical_features AS

        SELECT

            d.asset_identity,
            s.snapshot_date,

            SUM(d.wo_count) FILTER (
                WHERE d.event_date >=
                    s.snapshot_date - INTERVAL '30 days'
            ) AS wo_count_30d,

            SUM(d.wo_count) FILTER (
                WHERE d.event_date >=
                    s.snapshot_date - INTERVAL '90 days'
            ) AS wo_count_90d,

            SUM(d.wo_count) FILTER (
                WHERE d.event_date >=
                    s.snapshot_date - INTERVAL '180 days'
            ) AS wo_count_180d,

            SUM(d.wo_count) AS wo_count_365d,

            SUM(d.cm_count) FILTER (
                WHERE d.event_date >=
                    s.snapshot_date - INTERVAL '30 days'
            ) AS cm_count_30d,

            SUM(d.cm_count) FILTER (
                WHERE d.event_date >=
                    s.snapshot_date - INTERVAL '90 days'
            ) AS cm_count_90d,

            SUM(d.cm_count) AS cm_count_365d,

            DATE_DIFF(
                'day',
                MAX(d.event_date),
                s.snapshot_date
            ) AS days_since_last_wo,

            DATE_DIFF(
                'day',
                MAX(d.event_date) FILTER (
                    WHERE d.cm_count > 0
                ),
                s.snapshot_date
            ) AS days_since_last_cm

        FROM snapshots s

        INNER JOIN daily_events d

            ON d.event_date >=
                s.snapshot_date - INTERVAL '365 days'

            AND d.event_date < s.snapshot_date

        GROUP BY
            d.asset_identity,
            s.snapshot_date
    """)

    print("Historical feature engineering: PASS")


# ============================================================
# 7. FUTURE TARGET ENGINEERING
# ============================================================

def build_future_targets(con):

    con.execute("""
        CREATE TEMP TABLE future_targets AS

        SELECT

            d.asset_identity,
            s.snapshot_date,

            SUM(d.cm_count) AS future_cm_count

        FROM snapshots s

        INNER JOIN daily_events d

            ON d.event_date >= s.snapshot_date

            AND d.event_date <
                s.snapshot_date + INTERVAL '30 days'

        WHERE d.cm_count > 0

        GROUP BY
            d.asset_identity,
            s.snapshot_date
    """)

    print("Future target engineering: PASS")


# ============================================================
# 8. FINAL ML DATASET
# ============================================================

def create_training_dataset(con):

    con.execute("""
        CREATE TEMP TABLE training_features AS

        SELECT

            h.asset_identity,
            h.snapshot_date,

            COALESCE(h.wo_count_30d, 0)
                AS wo_count_30d,

            COALESCE(h.wo_count_90d, 0)
                AS wo_count_90d,

            COALESCE(h.wo_count_180d, 0)
                AS wo_count_180d,

            COALESCE(h.wo_count_365d, 0)
                AS wo_count_365d,

            COALESCE(h.cm_count_30d, 0)
                AS cm_count_30d,

            COALESCE(h.cm_count_90d, 0)
                AS cm_count_90d,

            COALESCE(h.cm_count_365d, 0)
                AS cm_count_365d,

            h.days_since_last_wo,

            COALESCE(h.days_since_last_cm, 366)
                AS days_since_last_cm,

            CAST(h.days_since_last_cm IS NULL AS INTEGER)
                AS no_cm_history_365d,

            COALESCE(h.cm_count_90d, 0) * 1.0
                / NULLIF(h.wo_count_90d, 0)
                AS corrective_ratio_90d,

            MONTH(h.snapshot_date)
                AS snapshot_month,

            CASE

                WHEN COALESCE(
                    f.future_cm_count, 0
                ) > 0 THEN 1

                ELSE 0

            END AS target_cm_next_30d,

            CASE

                WHEN h.snapshot_date
                    + INTERVAL '30 days'
                    <= DATE '2024-01-01'
                    THEN 'train'

                WHEN h.snapshot_date
                    >= DATE '2024-01-01'

                    AND h.snapshot_date
                        + INTERVAL '30 days'
                        <= DATE '2025-01-01'
                    THEN 'validation'

                WHEN h.snapshot_date
                    >= DATE '2025-01-01'
                    THEN 'test'

                ELSE 'excluded'

            END AS dataset_split

        FROM historical_features h

        LEFT JOIN future_targets f

            ON h.asset_identity = f.asset_identity

            AND h.snapshot_date = f.snapshot_date
    """)

    print("Final ML dataset creation: PASS")


# ============================================================
# 9. DATA VALIDATION
# ============================================================

def validate_dataset(con):

    summary = con.sql("""
        SELECT

            dataset_split,

            COUNT(*) AS records,

            COUNT(DISTINCT asset_identity)
                AS distinct_assets,

            SUM(target_cm_next_30d)
                AS positive_targets,

            COUNT(*) - SUM(target_cm_next_30d)
                AS negative_targets,

            ROUND(
                AVG(target_cm_next_30d) * 100,
                4
            ) AS positive_target_pct,

            MIN(snapshot_date) AS first_snapshot,

            MAX(snapshot_date) AS last_snapshot

        FROM training_features

        GROUP BY dataset_split

        ORDER BY dataset_split
    """).df()

    print("\nDATASET SPLIT SUMMARY")
    print(summary.to_string(index=False))

    duplicate_groups = con.sql("""
        SELECT COUNT(*)

        FROM (
            SELECT
                asset_identity,
                snapshot_date

            FROM training_features

            GROUP BY 1, 2

            HAVING COUNT(*) > 1
        )
    """).fetchall()[0][0]

    invalid_history = con.sql("""
        SELECT COUNT(*)

        FROM training_features

        WHERE days_since_last_wo < 1

           OR wo_count_365d < wo_count_180d

           OR wo_count_180d < wo_count_90d

           OR wo_count_90d < wo_count_30d

           OR cm_count_365d < cm_count_90d

           OR cm_count_90d < cm_count_30d

           OR cm_count_365d > wo_count_365d

           OR cm_count_90d > wo_count_90d

           OR cm_count_30d > wo_count_30d

           OR target_cm_next_30d NOT IN (0, 1)
    """).fetchall()[0][0]

    latest_event = con.sql("""
        SELECT MAX(event_date)
        FROM eligible_workorders
    """).fetchall()[0][0]

    incomplete_windows = con.sql(f"""
        SELECT COUNT(*)

        FROM training_features

        WHERE snapshot_date + INTERVAL '30 days'
            > DATE '{latest_event}' + INTERVAL '1 day'
    """).fetchall()[0][0]

    print(f"\nDuplicate groups: {duplicate_groups:,}")
    print(f"Invalid historical features: {invalid_history:,}")
    print(f"Incomplete target windows: {incomplete_windows:,}")

    assert duplicate_groups == 0
    assert invalid_history == 0
    assert incomplete_windows == 0

    for split in ["train", "validation", "test"]:

        counts = con.sql(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT target_cm_next_30d)

            FROM training_features

            WHERE dataset_split = '{split}'
        """).fetchall()[0]

        assert counts[0] > 0, f"Empty split: {split}"
        assert counts[1] == 2, f"Single-class split: {split}"

    print("Feature dataset validation: PASS")

    return {
        "status": "PASSED",
        "duplicate_groups": duplicate_groups,
        "invalid_historical_features": invalid_history,
        "incomplete_target_windows": incomplete_windows,
        "latest_observed_event": str(latest_event),
        "observation_days": OBSERVATION_DAYS,
        "prediction_days": PREDICTION_DAYS,
        "event_date_source": "created_at",
        "target": "target_cm_next_30d",
        "dataset_summary": summary.to_dict(
            orient="records"
        )
    }


# ============================================================
# 10. EXPORT VALIDATED DATASET
# ============================================================

def export_dataset(con, validation):

    output_path = OUTPUT_FILE.as_posix().replace("'", "''")

    con.execute(f"""
        COPY (

            SELECT *

            FROM training_features

            WHERE dataset_split != 'excluded'

            ORDER BY
                snapshot_date,
                asset_identity

        )

        TO '{output_path}'

        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """)

    with VALIDATION_FILE.open(
        "x",
        encoding="utf-8"
    ) as file:

        json.dump(
            validation,
            file,
            indent=4,
            default=str
        )

    print(f"\nFeature dataset: {OUTPUT_FILE}")
    print(f"Validation report: {VALIDATION_FILE}")


# ============================================================
# 11. MAIN EXECUTION
# ============================================================

def main():

    start = time.time()

    print("=" * 80)
    print("ASSETGUARD FEATURE ENGINEERING")
    print("=" * 80)

    check_environment()

    con = duckdb.connect()

    try:

        con.execute("SET threads = 4")

        con.execute(
            "SET memory_limit = '2GB'"
        )

        temp_path = TEMP_DIR.as_posix().replace("'", "''")

        con.execute(
            f"SET temp_directory = '{temp_path}'"
        )

        load_workorders(con)

        aggregate_daily_events(con)

        create_snapshots(con)

        build_historical_features(con)

        build_future_targets(con)

        create_training_dataset(con)

        validation = validate_dataset(con)

        export_dataset(con, validation)

    finally:

        con.close()

    elapsed = time.time() - start

    print("\n" + "=" * 80)
    print("ASSETGUARD FEATURE ENGINEERING: SUCCESS")
    print(f"Execution time: {elapsed:.2f} seconds")
    print("=" * 80)


if __name__ == "__main__":
    main()