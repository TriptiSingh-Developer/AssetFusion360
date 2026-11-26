"""
AssetFusion360 Capstone
AssetGuard - Predictive Maintenance

Step 10: Machine Learning Model Training

Models:
    Logistic Regression
    Random Forest
    XGBoost
    LightGBM

Methodology:
    Chronological dataset splitting
    Class imbalance handling
    Validation-based model selection
    Decision threshold optimization
    Independent test evaluation
    MLflow experiment tracking
"""

from pathlib import Path
import json
import time

import numpy as np
import pandas as pd
import joblib
import mlflow

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

from sklearn.model_selection import train_test_split

from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    precision_recall_curve
)

from xgboost import XGBClassifier
from lightgbm import LGBMClassifier


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[4]

FEATURE_FILE = (
    ROOT
    / "data"
    / "05_predictive_maintenance_features"
    / "assetguard_training_features.parquet"
)

MODEL_DIR = (
    ROOT / "data" / "06_trained_ml_models"
    / "assetguard_baseline"
)

PREDICTION_DIR = (
    ROOT / "data" / "07_asset_risk_predictions"
    / "assetguard_baseline"
)

REPORT_DIR = (
    ROOT / "data" / "07_data_quality_reports"
    / "assetguard_baseline"
)

MODEL_FILE = MODEL_DIR / "best_model.joblib"

METADATA_FILE = MODEL_DIR / "model_metadata.json"

COMPARISON_FILE = REPORT_DIR / "model_comparison.csv"

TEST_METRICS_FILE = REPORT_DIR / "test_metrics.json"

PREDICTION_FILE = (
    PREDICTION_DIR / "assetguard_test_predictions.parquet"
)

RANDOM_STATE = 42

N_JOBS = 4

TARGET = "target_cm_next_30d"

FEATURES = [
    "wo_count_30d",
    "wo_count_90d",
    "wo_count_180d",
    "wo_count_365d",
    "cm_count_30d",
    "cm_count_90d",
    "cm_count_365d",
    "days_since_last_wo",
    "days_since_last_cm",
    "no_cm_history_365d",
    "corrective_ratio_90d",
    "snapshot_month"
]


# ============================================================
# 2. ENVIRONMENT VALIDATION
# ============================================================

def check_environment():

    if not FEATURE_FILE.exists():
        raise FileNotFoundError(FEATURE_FILE)

    for directory in [
        MODEL_DIR,
        PREDICTION_DIR,
        REPORT_DIR
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    for file in [
        MODEL_FILE,
        METADATA_FILE,
        COMPARISON_FILE,
        TEST_METRICS_FILE,
        PREDICTION_FILE
    ]:
        if file.exists():
            raise FileExistsError(
                f"Existing output preserved: {file}"
            )

    print("Environment validation: PASS")


# ============================================================
# 3. LOAD FEATURE DATASET
# ============================================================

def load_dataset():

    columns = [
        "asset_identity",
        "snapshot_date",
        "dataset_split",
        TARGET
    ] + FEATURES

    df = pd.read_parquet(
        FEATURE_FILE,
        columns=columns
    )

    df[FEATURES] = df[FEATURES].astype("float32")

    df[TARGET] = df[TARGET].astype("int8")

    print(f"Total records: {len(df):,}")
    print(f"Total model features: {len(FEATURES)}")

    return df


# ============================================================
# 4. CHRONOLOGICAL DATASET SPLITTING
# ============================================================

def split_dataset(df):

    train = df.loc[
        df["dataset_split"] == "train"
    ]

    validation = df.loc[
        df["dataset_split"] == "validation"
    ]

    test = df.loc[
        df["dataset_split"] == "test"
    ]

    X_train = train[FEATURES]
    y_train = train[TARGET]

    X_val = validation[FEATURES]
    y_val = validation[TARGET]

    X_test = test[FEATURES]
    y_test = test[TARGET]

    assert train["snapshot_date"].max() < (
        validation["snapshot_date"].min()
    )

    assert validation["snapshot_date"].max() < (
        test["snapshot_date"].min()
    )

    print(f"Training records: {len(train):,}")
    print(f"Validation records: {len(validation):,}")
    print(f"Testing records: {len(test):,}")

    return (
        X_train, y_train,
        X_val, y_val,
        X_test, y_test,
        test
    )


# ============================================================
# 5. MODEL CONFIGURATION
# ============================================================

def create_models(y_train):

    negative_count = int((y_train == 0).sum())
    positive_count = int((y_train == 1).sum())

    positive_weight = (
        negative_count / positive_count
    )

    print(
        f"Positive class weight: "
        f"{positive_weight:.4f}"
    )

    models = {

        "Logistic Regression": Pipeline([
            (
                "imputer",
                SimpleImputer(strategy="median")
            ),
            (
                "scaler",
                StandardScaler()
            ),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=300,
                    random_state=RANDOM_STATE
                )
            )
        ]),

        "Random Forest": Pipeline([
            (
                "imputer",
                SimpleImputer(strategy="median")
            ),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=100,
                    max_depth=14,
                    min_samples_leaf=20,
                    class_weight="balanced",
                    n_jobs=N_JOBS,
                    random_state=RANDOM_STATE
                )
            )
        ]),

        "XGBoost": XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=positive_weight,
            objective="binary:logistic",
            eval_metric="logloss",
            tree_method="hist",
            random_state=RANDOM_STATE,
            n_jobs=N_JOBS
        ),

        "LightGBM": LGBMClassifier(
            n_estimators=200,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=8,
            scale_pos_weight=positive_weight,
            subsample=0.8,
            subsample_freq=1,
            colsample_bytree=0.8,
            random_state=RANDOM_STATE,
            n_jobs=N_JOBS,
            verbosity=-1
        )
    }

    return models


# ============================================================
# 6. EVALUATION METRICS
# ============================================================

def evaluate_model(y_true, scores, threshold):

    predictions = (
        scores >= threshold
    ).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_true,
        predictions,
        labels=[0, 1]
    ).ravel()

    return {
        "threshold": float(threshold),

        "accuracy": float(
            accuracy_score(y_true, predictions)
        ),

        "balanced_accuracy": float(
            balanced_accuracy_score(
                y_true, predictions
            )
        ),

        "precision": float(
            precision_score(
                y_true,
                predictions,
                zero_division=0
            )
        ),

        "recall": float(
            recall_score(
                y_true,
                predictions,
                zero_division=0
            )
        ),

        "f1_score": float(
            f1_score(
                y_true,
                predictions,
                zero_division=0
            )
        ),

        "roc_auc": float(
            roc_auc_score(y_true, scores)
        ),

        "average_precision": float(
            average_precision_score(y_true, scores)
        ),

        "true_negative": int(tn),
        "false_positive": int(fp),
        "false_negative": int(fn),
        "true_positive": int(tp)
    }


# ============================================================
# 7. THRESHOLD OPTIMIZATION
# ============================================================

def optimize_threshold(y_true, scores):

    precision, recall, thresholds = (
        precision_recall_curve(
            y_true,
            scores
        )
    )

    precision = precision[:-1]
    recall = recall[:-1]

    f1_scores = np.divide(
        2 * precision * recall,
        precision + recall,
        out=np.zeros_like(precision),
        where=(precision + recall) != 0
    )

    best_index = int(np.argmax(f1_scores))

    return float(thresholds[best_index])


# ============================================================
# 8. TRAIN AND COMPARE MODELS
# ============================================================

def train_models(
    models,
    X_train,
    y_train,
    X_val,
    y_val
):

    results = []

    best_model = None
    best_model_name = None
    best_threshold = None
    best_run_id = None

    best_ap = -1.0

    for name, model in models.items():

        print("\n" + "=" * 80)
        print(f"TRAINING: {name}")
        print("=" * 80)

        start = time.time()

        X_fit = X_train
        y_fit = y_train

        # Limit Random Forest training time
        # using a stratified training-only sample.

        if (
            name == "Random Forest"
            and len(X_train) > 300000
        ):

            X_fit, _, y_fit, _ = (
                train_test_split(
                    X_train,
                    y_train,
                    train_size=300000,
                    stratify=y_train,
                    random_state=RANDOM_STATE
                )
            )

        with mlflow.start_run(
            run_name=name
        ) as run:

            mlflow.log_params({
                "model_name": name,
                "training_records": len(X_fit),
                "feature_count": len(FEATURES),
                "random_state": RANDOM_STATE
            })

            model.fit(X_fit, y_fit)

            validation_scores = (
                model.predict_proba(X_val)[:, 1]
            )

            threshold = optimize_threshold(
                y_val,
                validation_scores
            )

            metrics = evaluate_model(
                y_val,
                validation_scores,
                threshold
            )

            elapsed = time.time() - start

            mlflow.log_metrics({
                f"validation_{key}": value
                for key, value in metrics.items()
            })

            mlflow.log_metric(
                "training_seconds", elapsed
            )

            result = {
                "model": name,
                "training_records": len(X_fit),
                "training_seconds": round(elapsed, 2),
                **metrics
            }

            results.append(result)

            print(
                pd.DataFrame([result])
                .T.to_string(header=False)
            )

            if (
                metrics["average_precision"]
                > best_ap
            ):

                best_ap = metrics[
                    "average_precision"
                ]

                best_model = model
                best_model_name = name
                best_threshold = threshold
                best_run_id = run.info.run_id

                print(
                    f"New best model: {name}"
                )

    comparison = pd.DataFrame(
        results
    ).sort_values(
        "average_precision",
        ascending=False
    )

    comparison.to_csv(
        COMPARISON_FILE,
        index=False
    )

    return (
        best_model,
        best_model_name,
        best_threshold,
        best_run_id,
        comparison
    )


# ============================================================
# 9. INDEPENDENT TEST EVALUATION
# ============================================================

def evaluate_best_model(
    model,
    model_name,
    threshold,
    run_id,
    X_test,
    y_test,
    test
):

    scores = model.predict_proba(
        X_test
    )[:, 1]

    metrics = evaluate_model(
        y_test,
        scores,
        threshold
    )

    print("\n" + "=" * 80)
    print("FINAL TEST EVALUATION")
    print("=" * 80)

    print(f"Best model: {model_name}")

    print(
        pd.DataFrame([metrics])
        .T.to_string(header=False)
    )

    predictions = test[[
        "asset_identity",
        "snapshot_date",
        TARGET
    ]].copy()

    predictions["risk_score"] = scores

    predictions["predicted_class"] = (
        scores >= threshold
    ).astype("int8")

    predictions["model_name"] = model_name

    predictions["decision_threshold"] = threshold

    predictions.to_parquet(
        PREDICTION_FILE,
        index=False
    )

    with TEST_METRICS_FILE.open(
        "x",
        encoding="utf-8"
    ) as file:

        json.dump(
            {
                "model": model_name,
                **metrics
            },
            file,
            indent=4
        )

    joblib.dump(
        model,
        MODEL_FILE,
        compress=3
    )

    metadata = {
        "model_name": model_name,
        "features": FEATURES,
        "target": TARGET,
        "threshold": threshold,
        "selection_metric": "average_precision",
        "prediction_window_days": 30,
        "mlflow_run_id": run_id,
        "random_state": RANDOM_STATE,
        "score_calibrated": False,
        "model_stage": "baseline"
    }

    with METADATA_FILE.open(
        "x",
        encoding="utf-8"
    ) as file:

        json.dump(
            metadata,
            file,
            indent=4
        )

    with mlflow.start_run(run_id=run_id):

        mlflow.log_metrics({
            f"test_{key}": value
            for key, value in metrics.items()
        })

        mlflow.log_artifact(
            str(MODEL_FILE)
        )

        mlflow.log_artifact(
            str(METADATA_FILE)
        )

        mlflow.log_artifact(
            str(TEST_METRICS_FILE)
        )

        mlflow.log_artifact(
            str(COMPARISON_FILE)
        )

    return metrics


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

def main():

    start = time.time()

    print("=" * 80)
    print("ASSETGUARD MACHINE LEARNING")
    print("=" * 80)

    check_environment()

    mlflow.set_tracking_uri(
        "sqlite:///assetguard_mlflow.db"
    )

    mlflow.set_experiment(
        "AssetFusion360_AssetGuard"
    )

    df = load_dataset()

    (
        X_train, y_train,
        X_val, y_val,
        X_test, y_test,
        test
    ) = split_dataset(df)

    models = create_models(y_train)

    (
        best_model,
        best_model_name,
        best_threshold,
        best_run_id,
        comparison
    ) = train_models(
        models,
        X_train,
        y_train,
        X_val,
        y_val
    )

    print("\nMODEL COMPARISON")

    print(
        comparison[[
            "model",
            "accuracy",
            "precision",
            "recall",
            "f1_score",
            "roc_auc",
            "average_precision",
            "training_seconds"
        ]].to_string(index=False)
    )

    evaluate_best_model(
        best_model,
        best_model_name,
        best_threshold,
        best_run_id,
        X_test,
        y_test,
        test
    )

    print("\nOUTPUT FILES")

    print(f"Best model: {MODEL_FILE}")
    print(f"Model metadata: {METADATA_FILE}")
    print(f"Model comparison: {COMPARISON_FILE}")
    print(f"Test metrics: {TEST_METRICS_FILE}")
    print(f"Predictions: {PREDICTION_FILE}")

    print("\n" + "=" * 80)
    print("ASSETGUARD MODEL TRAINING: SUCCESS")
    print(
        f"Total execution time: "
        f"{time.time() - start:.2f} seconds"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()