"""
AssetFusion360 Capstone
AssetGuard - Explainable AI using SHAP

Purpose:
    Explain the trained XGBoost maintenance risk model.
    Generate global and individual prediction explanations.
"""

from pathlib import Path
import json
import time

import numpy as np
import pandas as pd
import joblib
import shap
import mlflow

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[4]

FEATURE_FILE = (
    ROOT / "data"
    / "05_predictive_maintenance_features"
    / "assetguard_training_features.parquet"
)

MODEL_DIR = (
    ROOT / "data"
    / "06_trained_ml_models"
    / "assetguard_baseline"
)

OUTPUT_DIR = (
    ROOT / "data"
    / "07_data_quality_reports"
    / "assetguard_explainability"
)

MODEL_FILE = MODEL_DIR / "best_model.joblib"

METADATA_FILE = MODEL_DIR / "model_metadata.json"

SAMPLE_SIZE = 10000

RANDOM_STATE = 42


# ============================================================
# 2. ENVIRONMENT VALIDATION
# ============================================================

def check_environment():

    for file in [
        FEATURE_FILE,
        MODEL_FILE,
        METADATA_FILE
    ]:

        if not file.exists():
            raise FileNotFoundError(file)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    if any(OUTPUT_DIR.iterdir()):
        raise FileExistsError(
            f"Existing outputs preserved: {OUTPUT_DIR}"
        )

    print("Environment validation: PASS")


# ============================================================
# 3. LOAD MODEL AND METADATA
# ============================================================

def load_model():

    model = joblib.load(MODEL_FILE)

    with METADATA_FILE.open(
        encoding="utf-8"
    ) as file:

        metadata = json.load(file)

    print("Model:", metadata["model_name"])

    print(
        "Features:",
        len(metadata["features"])
    )

    return model, metadata


# ============================================================
# 4. LOAD EXPLANATION DATASET
# ============================================================

def load_explanation_data(metadata):

    features = metadata["features"]

    df = pd.read_parquet(
        FEATURE_FILE,
        columns=[
            "asset_identity",
            "snapshot_date",
            "dataset_split",
            metadata["target"]
        ] + features
    )

    test = df.loc[
        df["dataset_split"] == "test"
    ]

    sample = test.sample(
        n=min(SAMPLE_SIZE, len(test)),
        random_state=RANDOM_STATE
    ).reset_index(drop=True)

    X = sample[features].astype("float32")

    print(
        f"SHAP sample records: {len(sample):,}"
    )

    return sample, X


# ============================================================
# 5. CALCULATE SHAP VALUES
# ============================================================

def calculate_shap(model, X):

    explainer = shap.TreeExplainer(model)

    explanation = explainer(X)

    print(
        "SHAP dimensions:",
        explanation.values.shape
    )

    assert explanation.values.shape == X.shape

    assert np.isfinite(
        explanation.values
    ).all()

    print("SHAP validation: PASS")

    return explanation


# ============================================================
# 6. GLOBAL FEATURE IMPORTANCE
# ============================================================

def create_feature_importance(explanation, X):

    importance = pd.DataFrame({

        "feature": X.columns,

        "mean_absolute_shap": np.abs(
            explanation.values
        ).mean(axis=0)

    }).sort_values(
        "mean_absolute_shap",
        ascending=False
    )

    importance.to_csv(
        OUTPUT_DIR / "shap_feature_importance.csv",
        index=False
    )

    print("\nGLOBAL FEATURE IMPORTANCE")

    print(
        importance.to_string(index=False)
    )

    return importance


# ============================================================
# 7. SHAP VISUALIZATIONS
# ============================================================

def create_global_plots(explanation):

    shap.plots.bar(
        explanation,
        max_display=12,
        show=False
    )

    plt.tight_layout()

    plt.savefig(
        OUTPUT_DIR / "shap_global_importance.png",
        dpi=200,
        bbox_inches="tight"
    )

    plt.close("all")

    shap.plots.beeswarm(
        explanation,
        max_display=12,
        show=False
    )

    plt.tight_layout()

    plt.savefig(
        OUTPUT_DIR / "shap_summary_plot.png",
        dpi=200,
        bbox_inches="tight"
    )

    plt.close("all")

    print("Global SHAP plots: CREATED")


# ============================================================
# 8. INDIVIDUAL ASSET EXPLANATIONS
# ============================================================

def create_asset_explanations(
    model,
    metadata,
    sample,
    X,
    explanation
):

    scores = model.predict_proba(X)[:, 1]

    threshold = metadata["threshold"]

    records = []

    for i in range(len(sample)):

        values = explanation.values[i]

        top_indices = np.argsort(
            np.abs(values)
        )[::-1][:5]

        for rank, index in enumerate(
            top_indices,
            start=1
        ):

            records.append({

                "asset_identity": int(
                    sample.loc[i, "asset_identity"]
                ),

                "snapshot_date": sample.loc[
                    i, "snapshot_date"
                ],

                "risk_score": float(scores[i]),

                "predicted_class": int(
                    scores[i] >= threshold
                ),

                "feature_rank": rank,

                "feature": X.columns[index],

                "feature_value": float(
                    X.iloc[i, index]
                ),

                "shap_value": float(
                    values[index]
                ),

                "risk_direction": (
                    "Increases Risk"
                    if values[index] > 0
                    else "Decreases Risk"
                    if values[index] < 0
                    else "Neutral"
                )
            })

    explanations = pd.DataFrame(records)

    explanations.to_parquet(
        OUTPUT_DIR / "asset_shap_explanations.parquet",
        index=False
    )

    high_risk_index = int(
        np.argmax(scores)
    )

    low_risk_index = int(
        np.argmin(scores)
    )

    for label, index in [
        ("high_risk", high_risk_index),
        ("low_risk", low_risk_index)
    ]:

        shap.plots.waterfall(
            explanation[index],
            max_display=12,
            show=False
        )

        plt.tight_layout()

        plt.savefig(
            OUTPUT_DIR / f"shap_{label}_asset.png",
            dpi=200,
            bbox_inches="tight"
        )

        plt.close("all")

    print(
        f"Individual explanation records: "
        f"{len(explanations):,}"
    )

    print("Individual SHAP explanations: CREATED")


# ============================================================
# 9. MLFLOW INTEGRATION
# ============================================================

def log_explainability(metadata):

    mlflow.set_tracking_uri(
        "sqlite:///assetguard_mlflow.db"
    )

    with mlflow.start_run(
        run_id=metadata["mlflow_run_id"]
    ):

        mlflow.log_artifacts(
            str(OUTPUT_DIR),
            artifact_path="explainability"
        )

        mlflow.log_param(
            "shap_sample_size",
            SAMPLE_SIZE
        )

    print("MLflow SHAP logging: PASS")


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

def main():

    start = time.time()

    print("=" * 80)
    print("ASSETGUARD EXPLAINABLE AI")
    print("=" * 80)

    check_environment()

    model, metadata = load_model()

    sample, X = load_explanation_data(
        metadata
    )

    explanation = calculate_shap(
        model,
        X
    )

    create_feature_importance(
        explanation,
        X
    )

    create_global_plots(
        explanation
    )

    create_asset_explanations(
        model,
        metadata,
        sample,
        X,
        explanation
    )

    log_explainability(metadata)

    print("\nOUTPUT DIRECTORY:")
    print(OUTPUT_DIR)

    print("\n" + "=" * 80)
    print("ASSETGUARD EXPLAINABLE AI: SUCCESS")

    print(
        f"Execution time: "
        f"{time.time() - start:.2f} seconds"
    )

    print("=" * 80)


if __name__ == "__main__":
    main()