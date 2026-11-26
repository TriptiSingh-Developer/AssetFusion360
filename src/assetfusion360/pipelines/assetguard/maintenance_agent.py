"""
AssetFusion360 Capstone
AssetGuard - Maintenance Intelligence Assistant

A local tool-based maintenance assistant using:
    XGBoost risk predictions
    SHAP explanations
    Historical maintenance records
    DuckDB analytical queries
    Rule-based tool orchestration
"""

from pathlib import Path
import json
import re

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[4]

GOLD_DIR = ROOT / "data" / "04_gold_analytics"

RISK_DIR = (
    ROOT / "data"
    / "07_asset_risk_predictions"
    / "latest_batch"
)

PREDICTION_FILE = (
    RISK_DIR / "assetguard_latest_predictions.parquet"
)

SHAP_FILE = (
    RISK_DIR / "assetguard_latest_risk_drivers.parquet"
)

SUMMARY_FILE = RISK_DIR / "prediction_summary.json"


FEATURE_LABELS = {
    "cm_count_365d":
        "Corrective work orders in the past year",

    "cm_count_90d":
        "Corrective work orders in the past 90 days",

    "cm_count_30d":
        "Corrective work orders in the past 30 days",

    "wo_count_365d":
        "Total work orders in the past year",

    "wo_count_180d":
        "Total work orders in the past 180 days",

    "wo_count_90d":
        "Total work orders in the past 90 days",

    "wo_count_30d":
        "Total work orders in the past 30 days",

    "days_since_last_cm":
        "Days since last corrective maintenance",

    "days_since_last_wo":
        "Days since last work order",

    "snapshot_month":
        "Seasonal maintenance pattern",

    "no_cm_history_365d":
        "No corrective maintenance history",

    "corrective_ratio_90d":
        "Corrective maintenance ratio"
}


class AssetGuardAgent:

    def __init__(self):

        for path in [
            PREDICTION_FILE,
            SHAP_FILE,
            SUMMARY_FILE
        ]:
            if not path.exists():
                raise FileNotFoundError(path)

        self.con = duckdb.connect()

        self.con.execute("SET threads = 4")

        self.con.execute(
            "SET memory_limit = '2GB'"
        )

        self.con.read_parquet(
            str(PREDICTION_FILE)
        ).create_view("predictions")

        self.con.read_parquet(
            str(SHAP_FILE)
        ).create_view("risk_drivers")

        files = sorted(
            (GOLD_DIR / "fact_workorders")
            .rglob("*.parquet")
        )

        self.con.read_parquet(
            [str(f.resolve()) for f in files]
        ).create_view("workorders")

        with SUMMARY_FILE.open(
            encoding="utf-8"
        ) as file:
            self.summary = json.load(file)

    # --------------------------------------------------------
    # TOOL 1: ASSET RISK ANALYSIS
    # --------------------------------------------------------


    def resolve_asset_code(self, asset_code):

        return self.con.execute(
            '''
            SELECT DISTINCT
                asset_identity,
                asset_code
            FROM predictions
            WHERE UPPER(asset_code) = UPPER(?)
            ''',
            [asset_code.strip()]
        ).df()

    def get_asset_risk(self, asset_id):

        return self.con.execute("""
            SELECT *
            FROM predictions
            WHERE asset_identity = ?
        """, [int(asset_id)]).df()

    # --------------------------------------------------------
    # TOOL 2: EXPLAINABLE AI
    # --------------------------------------------------------

    def explain_asset(self, asset_id):

        df = self.con.execute("""
            SELECT
                feature_rank,
                feature,
                feature_value,
                shap_value,
                risk_direction

            FROM risk_drivers

            WHERE asset_identity = ?

            ORDER BY feature_rank

        """, [int(asset_id)]).df()

        df["business_description"] = (
            df["feature"]
            .map(FEATURE_LABELS)
            .fillna(df["feature"])
        )

        return df

    # --------------------------------------------------------
    # TOOL 3: HISTORICAL MAINTENANCE
    # --------------------------------------------------------

    def get_maintenance_history(
        self,
        asset_id,
        limit=20
    ):

        cutoff = self.summary[
            "prediction_start"
        ][:10]

        return self.con.execute("""
            SELECT
                workorder_code,
                workorder_description,
                workorder_type,
                workorder_priority,
                job_type,
                created_at,

                CASE
                    WHEN completed_at < CAST(? AS DATE)
                    THEN completed_at
                    ELSE NULL
                END AS completion_before_cutoff

            FROM workorders

            WHERE resolved_asset_identity = ?

              AND asset_resolution_status = 'UNIQUE'

              AND created_at < CAST(? AS DATE)

            ORDER BY created_at DESC

            LIMIT ?

        """, [
            cutoff,
            int(asset_id),
            cutoff,
            min(max(int(limit), 1), 100)
        ]).df()

    # --------------------------------------------------------
    # TOOL 4: MAINTENANCE RECOMMENDATION
    # --------------------------------------------------------

    def recommend_action(self, asset_id):

        result = self.get_asset_risk(asset_id)

        if result.empty:
            return {
                "status": "Asset not found"
            }

        asset = result.iloc[0]

        actions = {

            "High": [
                "Prioritize technician review.",
                "Review recurring corrective work orders.",
                "Assess unresolved maintenance issues.",
                "Confirm condition through inspection."
            ],

            "Medium": [
                "Schedule a condition assessment.",
                "Review recent corrective maintenance.",
                "Evaluate maintenance scheduling needs."
            ],

            "Moderate": [
                "Monitor maintenance frequency.",
                "Review changes in corrective workload."
            ],

            "Low": [
                "Continue routine monitoring.",
                "Maintain the existing inspection schedule."
            ]
        }

        category = asset["risk_category"]

        return {

            "asset_identity": int(asset_id),

            "asset_code": asset["asset_code"],

            "risk_category": category,

            "risk_score": float(asset["risk_score"]),

            "recommended_actions": actions[category],

            "approval_required": True,

            "forecast_start":
                self.summary["prediction_start"],

            "forecast_end_exclusive":
                self.summary[
                    "prediction_end_exclusive"
                ],

            "disclaimer":
                "Decision support only. "
                "Confirm current asset condition "
                "before taking action."
        }

    # --------------------------------------------------------
    # TOOL 5: SIMILAR HISTORICAL WORK ORDERS
    # --------------------------------------------------------

    def find_similar_workorders(
        self,
        search_text,
        limit=20
    ):

        keywords = re.findall(
            r"[a-zA-Z0-9]+",
            search_text.lower()
        )

        stop_words = {
            "the", "a", "an", "is", "was",
            "in", "on", "for", "of", "and",
            "to", "find", "similar", "workorders",
            "work", "orders"
        }

        keywords = list(dict.fromkeys(
            word for word in keywords
            if len(word) > 2
            and word not in stop_words
        ))[:10]

        if not keywords:
            return pd.DataFrame()

        expressions = [
            """
            CASE
                WHEN CONTAINS(
                    LOWER(
                        COALESCE(
                            workorder_description, ''
                        )
                    ),
                    ?
                )
                THEN 1
                ELSE 0
            END
            """
            for _ in keywords
        ]

        similarity = " + ".join(expressions)

        query = f"""
            WITH matches AS (

                SELECT
                    workorder_code,
                    resolved_asset_identity,
                    workorder_description,
                    workorder_priority,
                    job_type,
                    created_at,

                    ({similarity})
                        AS keyword_match_score

                FROM workorders

                WHERE job_type = 'CM'

                  AND asset_resolution_status = 'UNIQUE'

                  AND created_at < CAST(? AS DATE)
            )

            SELECT *
            FROM matches

            WHERE keyword_match_score > 0

            ORDER BY
                keyword_match_score DESC,
                created_at DESC

            LIMIT ?
        """

        params = keywords + [
            self.summary["prediction_start"][:10],
            min(max(int(limit), 1), 100)
        ]

        return self.con.execute(
            query,
            params
        ).df()

    # --------------------------------------------------------
    # TOOL 6: PORTFOLIO RISK ANALYSIS
    # --------------------------------------------------------

    def get_portfolio_summary(self):

        return self.con.sql("""
            SELECT
                risk_category,
                COUNT(*) AS asset_count,
                ROUND(
                    AVG(risk_score),
                    4
                ) AS average_risk_score

            FROM predictions

            GROUP BY risk_category

            ORDER BY average_risk_score DESC

        """).df()

    def get_top_risk_assets(self, limit=10):

        return self.con.execute("""
            SELECT
                asset_identity,
                asset_code,
                risk_rank,
                risk_score,
                risk_category,
                cm_count_365d,
                days_since_last_cm

            FROM predictions

            ORDER BY risk_rank

            LIMIT ?

        """, [
            min(max(int(limit), 1), 100)
        ]).df()

    # --------------------------------------------------------
    # AGENT ORCHESTRATION
    # --------------------------------------------------------

    def run(self, question):

        question = question.strip()


        # Recognize structured asset codes.

        asset_codes = re.findall(
            r"\b[A-Z0-9]+(?:-[A-Z0-9]+){2,}\b",
            question,
            flags=re.IGNORECASE
        )

        asset_codes = list(dict.fromkeys(
            value.upper() for value in asset_codes
        ))

        if len(asset_codes) > 1:
            return {
                "intent": "unsupported_question",
                "message":
                    "Please ask about one asset at a time."
            }

        if asset_codes:

            asset_code = asset_codes[0]

            matches = self.resolve_asset_code(asset_code)

            if matches.empty:

                return {
                    "intent": "asset_not_found",
                    "message":
                        f"Asset code {asset_code} is not "
                        "in the scored population."
                }

            if len(matches) > 1:

                return {
                    "intent": "ambiguous_asset_code",
                    "message":
                        "Multiple assets share this code. "
                        "Please provide the numerical asset ID.",
                    "data": matches
                }

            asset_id = int(
                matches.iloc[0]["asset_identity"]
            )

            question = re.sub(
                r"\b" + re.escape(asset_code) + r"\b",
                f"asset {asset_id}",
                question,
                flags=re.IGNORECASE
            )

        text = question.lower()


        if "similar" in text:

            return {
                "intent": "similar_workorders",
                "data": self.find_similar_workorders(
                    question
                )
            }

        asset_match = re.search(
            r"\basset(?:\s+id)?\s*[:#]?\s*(\d+)\b",
            text
        )

        if asset_match:

            asset_id = int(asset_match.group(1))

            risk = self.get_asset_risk(asset_id)

            if risk.empty:
                return {
                    "intent": "asset_not_found",
                    "message":
                        "Asset is not in the scored population."
                }

            if "history" in text:

                return {
                    "intent": "maintenance_history",
                    "data":
                        self.get_maintenance_history(
                            asset_id
                        )
                }

            return {

                "intent": "asset_assessment",

                "risk": risk,

                "explanations":
                    self.explain_asset(asset_id),

                "recommendation":
                    self.recommend_action(asset_id),

                "history":
                    self.get_maintenance_history(
                        asset_id,
                        limit=5
                    )
            }

        if (
            "top" in text
            or "high risk" in text
            or "highest risk" in text
        ):

            limit_match = re.search(
                r"\btop\s+(\d+)\b",
                text
            )

            limit = (
                int(limit_match.group(1))
                if limit_match
                else 10
            )

            return {
                "intent": "top_risk_assets",
                "data":
                    self.get_top_risk_assets(limit)
            }

        if any(
            word in text
            for word in [
                "summary",
                "portfolio",
                "distribution",
                "overview"
            ]
        ):

            return {
                "intent": "portfolio_summary",
                "data":
                    self.get_portfolio_summary()
            }


        # Recognize standalone maintenance descriptions.

        maintenance_pattern = (
            r"\b(leak\w*|broken|damaged|blocked|clogged|"
            r"malfunction\w*|repair|cracked|rusted|"
            r"failure|faulty|defective|not working|"
            r"not cooling|unable to flush)\b"
        )

        equipment_pattern = (
            r"\b(pipe\w*|plumbing|toilet\w*|door\w*|"
            r"lock\w*|playground|equipment|bench\w*|"
            r"electrical|power|lighting|light\w*|"
            r"drain\w*|hvac|air conditioning|"
            r"roof\w*|fence\w*|water|pump\w*)\b"
        )

        if (
            re.search(maintenance_pattern, text)
            and re.search(equipment_pattern, text)
        ):

            return {
                "intent": "similar_workorders",
                "data": self.find_similar_workorders(question)
            }

        return {
            "intent": "unsupported_question",
            "message":
                "Ask about an asset ID, "
                "maintenance history, top risk assets, "
                "portfolio summary or similar work orders."
        }

    def close(self):

        self.con.close()


# ============================================================
# LOCAL AGENT VALIDATION
# ============================================================

def main():

    print("=" * 80)
    print("ASSETGUARD MAINTENANCE INTELLIGENCE")
    print("=" * 80)

    agent = AssetGuardAgent()

    try:

        questions = [

            "Show portfolio summary",

            "Show top 5 high risk assets",

            "Why is asset 14167 high risk?",

            "Show maintenance history for asset 14167",

            "Find similar workorders for leaking pipe"
        ]

        for question in questions:

            print("\n" + "=" * 80)

            print("QUESTION:", question)

            result = agent.run(question)

            print("INTENT:", result["intent"])

            for key, value in result.items():

                if key == "intent":
                    continue

                print("\n" + key.upper())

                if isinstance(value, pd.DataFrame):

                    print(
                        value.head(10)
                        .to_string(index=False)
                    )

                elif isinstance(value, dict):

                    print(
                        json.dumps(
                            value,
                            indent=4,
                            default=str
                        )
                    )

                else:
                    print(value)

    finally:

        agent.close()

    print("\n" + "=" * 80)

    print(
        "ASSETGUARD MAINTENANCE ASSISTANT: SUCCESS"
    )

    print("=" * 80)


if __name__ == "__main__":

    main()