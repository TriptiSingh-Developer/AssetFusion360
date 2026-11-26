"""
AssetFusion360 Capstone
AssetGuard - Batch Risk Prediction

Purpose:
    Generate maintenance risk scores from the
    latest available historical maintenance data.

Outputs:
    Asset risk predictions
    Maintenance prioritization
    Individual SHAP risk drivers
    Prediction validation report
"""

from pathlib import Path
import json
import time

import duckdb
import joblib
import pandas as pd
import numpy as np
import shap


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[4]

GOLD_DIR = ROOT / "data" / "04_gold_analytics"

MODEL_DIR = (
    ROOT / "data"
    / "06_trained_ml_models"
    / "assetguard_baseline"
)

OUTPUT_DIR = (
    ROOT / "data"
    / "07_asset_risk_predictions"
    / "latest_batch"
)

MODEL_FILE = MODEL_DIR / "best_model.joblib"

METADATA_FILE = MODEL_DIR / "model_metadata.json"

PREDICTION_FILE = (
    OUTPUT_DIR / "assetguard_latest_predictions.parquet"
)

CSV_FILE = (
    OUTPUT_DIR / "assetguard_latest_predictions.csv"
)

SHAP_FILE = (
    OUTPUT_DIR / "assetguard_latest_risk_drivers.parquet"
)

REPORT_FILE = (
    OUTPUT_DIR / "prediction_summary.json"
)


# ============================================================
# 2. ENVIRONMENT VALIDATION
# ============================================================

def check_environment():

    for path in [MODEL_FILE, METADATA_FILE]:

        if not path.exists():
            raise FileNotFoundError(path)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    for path in [
        PREDICTION_FILE,
        CSV_FILE,
        SHAP_FILE,
        REPORT_FILE
    ]:

        if path.exists():

            raise FileExistsError(
                f"Existing output preserved: {path}"
            )

    print("Environment validation: PASS")


# ============================================================
# 3. LOAD MODEL
# ============================================================

def load_model():

    model = joblib.load(MODEL_FILE)

    with METADATA_FILE.open(
        encoding="utf-8"
    ) as file:

        metadata = json.load(file)

    print("Model:", metadata["model_name"])
    print("Threshold:", metadata["threshold"])

    return model, metadata


# ============================================================
# 4. LOAD GOLD TABLES
# ============================================================

def load_gold_tables(con):

    for table in [
        "fact_workorders",
        "dim_asset"
    ]:

        files = sorted(
            (GOLD_DIR / table).rglob("*.parquet")
        )

        if not files:
            raise FileNotFoundError(table)

        con.read_parquet(
            [str(f.resolve()) for f in files]
        ).create_view(table)

        print(
            f"{table}: {len(files):,} Parquet files"
        )


# ============================================================
# 5. DETERMINE SCORING DATE
# ============================================================

def get_scoring_date(con):

    latest_date = con.sql("""
        SELECT MAX(CAST(created_at AS DATE))
        FROM fact_workorders
    """).fetchall()[0][0]

    scoring_date = (
        pd.Timestamp(latest_date)
        + pd.Timedelta(days=1)
    )

    print("Latest source date:", latest_date)

    print(
        "Prediction cutoff:",
        scoring_date.date()
    )

    return scoring_date


# ============================================================
# 6. BUILD SCORING FEATURES
# ============================================================

def build_scoring_features(con, scoring_date):

    cutoff = scoring_date.strftime("%Y-%m-%d")

    query = f"""
        WITH eligible_workorders AS (

            SELECT
                resolved_asset_identity
                    AS asset_identity,

                CAST(created_at AS DATE)
                    AS event_date,

                CASE
                    WHEN job_type = 'CM' THEN 1
                    ELSE 0
                END AS is_corrective

            FROM fact_workorders

            WHERE asset_resolution_status = 'UNIQUE'

              AND resolved_asset_identity IS NOT NULL

              AND workorder_identity IS NOT NULL

              AND created_at IS NOT NULL

              AND CAST(created_at AS DATE)
                  < DATE '{cutoff}'

              AND CAST(created_at AS DATE)
                  >= DATE '{cutoff}'
                     - INTERVAL '365 days'
        ),

        historical_features AS (

            SELECT

                asset_identity,

                COUNT(*) FILTER (
                    WHERE event_date >=
                        DATE '{cutoff}'
                        - INTERVAL '30 days'
                ) AS wo_count_30d,

                COUNT(*) FILTER (
                    WHERE event_date >=
                        DATE '{cutoff}'
                        - INTERVAL '90 days'
                ) AS wo_count_90d,

                COUNT(*) FILTER (
                    WHERE event_date >=
                        DATE '{cutoff}'
                        - INTERVAL '180 days'
                ) AS wo_count_180d,

                COUNT(*) AS wo_count_365d,

                COUNT(*) FILTER (
                    WHERE is_corrective = 1
                      AND event_date >=
                          DATE '{cutoff}'
                          - INTERVAL '30 days'
                ) AS cm_count_30d,

                COUNT(*) FILTER (
                    WHERE is_corrective = 1
                      AND event_date >=
                          DATE '{cutoff}'
                          - INTERVAL '90 days'
                ) AS cm_count_90d,

                SUM(is_corrective)
                    AS cm_count_365d,

                DATE_DIFF(
                    'day',
                    MAX(event_date),
                    DATE '{cutoff}'
                ) AS days_since_last_wo,

                DATE_DIFF(
                    'day',
                    MAX(event_date) FILTER (
                        WHERE is_corrective = 1
                    ),
                    DATE '{cutoff}'
                ) AS days_since_last_cm

            FROM eligible_workorders

            GROUP BY asset_identity
        )

        SELECT

            h.asset_identity,

            DATE '{cutoff}' AS snapshot_date,

            h.wo_count_30d,
            h.wo_count_90d,
            h.wo_count_180d,
            h.wo_count_365d,

            h.cm_count_30d,
            h.cm_count_90d,
            h.cm_count_365d,

            h.days_since_last_wo,

            COALESCE(
                h.days_since_last_cm,
                366
            ) AS days_since_last_cm,

            CAST(
                h.days_since_last_cm IS NULL
                AS INTEGER
            ) AS no_cm_history_365d,

            h.cm_count_90d * 1.0
                / NULLIF(h.wo_count_90d, 0)
                AS corrective_ratio_90d,

            MONTH(DATE '{cutoff}')
                AS snapshot_month,

            a.asset_code,
            a.asset_status,
            a.parent_asset_code

        FROM historical_features h

        LEFT JOIN dim_asset a

            ON h.asset_identity =
               a.asset_identity
    """

    df = con.sql(query).df()

    assert len(df) > 0

    assert not df[
        "asset_identity"
    ].duplicated().any()

    assert (
        df["days_since_last_wo"] >= 1
    ).all()

    print(
        f"Eligible assets for scoring: "
        f"{len(df):,}"
    )

    print("Scoring features: PASS")

    return df


# ============================================================
# 7. GENERATE RISK SCORES
# ============================================================

def generate_predictions(
    df,
    model,
    metadata
):

    features = metadata["features"]

    X = df[features].astype("float32")

    scores = model.predict_proba(X)[:, 1]

    threshold = metadata["threshold"]

    df["risk_score"] = scores

    df["predicted_class"] = (
        scores >= threshold
    ).astype("int8")

    df["risk_category"] = np.select(

        [
            scores >= threshold,

            scores >= 0.50,

            scores >= 0.25
        ],

        [
            "High",
            "Medium",
            "Moderate"
        ],

        default="Low"
    )

    df["recommended_action"] = (
        df["risk_category"].map({

            "High":
                "Prioritize maintenance review",

            "Medium":
                "Schedule condition assessment",

            "Moderate":
                "Monitor maintenance indicators",

            "Low":
                "Continue routine monitoring"
        })
    )

    df["model_name"] = metadata["model_name"]

    df["decision_threshold"] = threshold

    df["score_calibrated"] = False

    df["prediction_start"] = (
        df["snapshot_date"]
    )

    df["prediction_end_exclusive"] = (
        df["snapshot_date"]
        + pd.Timedelta(days=30)
    )

    assert np.isfinite(scores).all()

    assert (
        (scores >= 0) & (scores <= 1)
    ).all()

    print("Risk score generation: PASS")

    return df, X


# ============================================================
# 8. GENERATE INDIVIDUAL SHAP EXPLANATIONS
# ============================================================

def generate_risk_drivers(df, X, model):

    explainer = shap.TreeExplainer(model)

    explanation = explainer(X)

    values = explanation.values

    assert values.shape == X.shape
    assert np.isfinite(values).all()

    top_indices = np.argsort(
        np.abs(values),
        axis=1
    )[:, ::-1][:, :5]

    records = []

    feature_names = X.columns.to_numpy()

    feature_values = X.to_numpy()

    asset_ids = df[
        "asset_identity"
    ].to_numpy()

    risk_scores = df[
        "risk_score"
    ].to_numpy()

    for rank in range(5):

        indices = top_indices[:, rank]

        selected_shap = values[
            np.arange(len(df)),
            indices
        ]

        selected_features = feature_values[
            np.arange(len(df)),
            indices
        ]

        records.append(

            pd.DataFrame({

                "asset_identity": asset_ids,

                "snapshot_date":
                    df["snapshot_date"].to_numpy(),

                "risk_score": risk_scores,

                "feature_rank": rank + 1,

                "feature":
                    feature_names[indices],

                "feature_value":
                    selected_features,

                "shap_value":
                    selected_shap,

                "risk_direction": np.select(

                    [
                        selected_shap > 0,
                        selected_shap < 0
                    ],

                    [
                        "Increases Risk",
                        "Decreases Risk"
                    ],

                    default="Neutral"
                )
            })
        )

    drivers = pd.concat(
        records,
        ignore_index=True
    )

    print(
        f"Individual risk drivers: "
        f"{len(drivers):,}"
    )

    return drivers


# ============================================================
# 9. EXPORT OUTPUTS
# ============================================================

def export_results(df, drivers, metadata):

    df = df.sort_values(
        "risk_score",
        ascending=False
    )

    df["risk_rank"] = np.arange(
        1,
        len(df) + 1
    )

    df.to_parquet(
        PREDICTION_FILE,
        index=False
    )

    df.to_csv(
        CSV_FILE,
        index=False
    )

    drivers.to_parquet(
        SHAP_FILE,
        index=False
    )

    summary = (

        df.groupby("risk_category")

        .agg(
            assets=("asset_identity", "count"),
            average_score=("risk_score", "mean"),
            minimum_score=("risk_score", "min"),
            maximum_score=("risk_score", "max")
        )

        .reset_index()
    )

    print("\nRISK DISTRIBUTION")

    print(
        summary.to_string(index=False)
    )

    report = {

        "status": "PASSED",

        "model": metadata["model_name"],

        "total_scored_assets": len(df),

        "high_risk_assets": int(
            df["predicted_class"].sum()
        ),

        "decision_threshold":
            metadata["threshold"],

        "prediction_start":
            str(df["prediction_start"].iloc[0]),

        "prediction_end_exclusive":
            str(
                df[
                    "prediction_end_exclusive"
                ].iloc[0]
            ),

        "score_calibrated": False,

        "risk_distribution":
            summary.to_dict(orient="records"),

        "eligibility":
            "Assets with maintenance history "
            "in previous 365 days",

        "forecast_type":
            "As-of-data-cutoff forecast"
    }

    with REPORT_FILE.open(
        "x",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=4,
            default=str
        )

    print("\nTOP 10 RISK ASSETS")

    print(

        df[[
            "asset_identity",
            "asset_code",
            "risk_score",
            "risk_category",
            "cm_count_365d",
            "days_since_last_cm"
        ]]

        .head(10)

        .to_string(index=False)
    )

    print("\nOutput directory:")
    print(OUTPUT_DIR)


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

def main():

    start = time.time()

    print("=" * 80)
    print("ASSETGUARD BATCH RISK PREDICTION")
    print("=" * 80)

    check_environment()

    model, metadata = load_model()

    con = duckdb.connect()

    try:

        con.execute("SET threads = 4")
        con.execute("SET memory_limit = '2GB'")

        load_gold_tables(con)

        scoring_date = get_scoring_date(con)

        df = build_scoring_features(
            con,
            scoring_date
        )

    finally:

        con.close()

    df, X = generate_predictions(
        df,
        model,
        metadata
    )

    drivers = generate_risk_drivers(
        df,
        X,
        model
    )

    export_results(
        df,
        drivers,
        metadata
    )

    print("\n" + "=" * 80)

    print(
        "ASSETGUARD BATCH RISK PREDICTION: SUCCESS"
    )

    print(
        f"Execution time: "
        f"{time.time() - start:.2f} seconds"
    )

    print("=" * 80)


if __name__ == "__main__":
    main()