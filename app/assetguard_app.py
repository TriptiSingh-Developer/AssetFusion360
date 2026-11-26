
"""
AssetFusion360 | AssetGuard
Enterprise Predictive Maintenance Intelligence

Rutgers Business School - MITA Capstone
"""

from pathlib import Path
import sys
import json

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


# ============================================================
# 1. APPLICATION CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

LOGO_PATH = ROOT / "app" / "assets" / "assetfusion360_logo.png"

sys.path.insert(0, str(ROOT / "src"))

from assetfusion360.pipelines.assetguard.maintenance_agent import (
    AssetGuardAgent
)


from assetfusion360.pipelines.assetguard.llama_agent import (
    AssetGuardLlamaAgent
)

st.set_page_config(
    page_title="AssetFusion360 | AssetGuard",
    page_icon=str(LOGO_PATH) if LOGO_PATH.exists() else "🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA = ROOT / "data"

RISK_DIR = (
    DATA / "07_asset_risk_predictions" / "latest_batch"
)

REPORT_DIR = (
    DATA / "07_data_quality_reports" / "assetguard_baseline"
)

SHAP_DIR = (
    DATA / "07_data_quality_reports" / "assetguard_explainability"
)

MODEL_DIR = (
    DATA / "06_trained_ml_models" / "assetguard_baseline"
)

FEATURE_DIR = (
    DATA / "05_predictive_maintenance_features"
)

NAVY = "#082238"
TEAL = "#009CA6"
WHITE = "#FFFFFF"
LIGHT_GRAY = "#F4F7FA"

COLORS = {
    "High": "#DC2626",
    "Medium": "#EA580C",
    "Moderate": "#EAB308",
    "Low": "#16A34A"
}

RISK_ORDER = [
    "High",
    "Medium",
    "Moderate",
    "Low"
]


# ============================================================
# 2. APPLICATION STYLING
# ============================================================

st.html("""
<style>

.stApp {
    background-color: #FFFFFF;
    color: #082238;
}

.block-container {
    padding-top: 1.8rem;
    padding-bottom: 2rem;
    max-width: 1700px;
}

[data-testid="stHeader"] {
    background-color: #FFFFFF;
}

[data-testid="stAppViewContainer"] {
    background-color: #FFFFFF;
}

[data-testid="stSidebar"] {
    background-color: #F4F7FA;
    border-right: 1px solid #DCE4EC;
}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: #082238;
}

[data-testid="stMetric"] {
    background-color: #FFFFFF;
    border: 1px solid #DCE4EC;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 3px 10px rgba(8, 34, 56, 0.04);
}

[data-testid="stMetricValue"] {
    color: #009CA6;
    font-weight: 750;
}

[data-testid="stMetricLabel"] {
    color: #475569;
}

[data-testid="stSidebar"] [role="radiogroup"] {
    gap: 8px;
}

[data-testid="stSidebar"] [role="radiogroup"] label {
    padding: 6px 8px;
    border-radius: 6px;
}

[data-testid="stSidebar"] [role="radiogroup"] label:hover {
    background-color: #E1F3F4;
}

.hero {
    background: linear-gradient(
        120deg, #FFFFFF, #EAF8F8
    );
    padding: 28px;
    border-radius: 14px;
    margin-bottom: 24px;
    border: 1px solid #DCE4EC;
}

.hero-title {
    font-size: 32px;
    font-weight: 800;
    letter-spacing: 1.5px;
    color: #082238;
}

.hero-subtitle {
    font-size: 15px;
    color: #475569;
    margin-top: 8px;
}

.section-label {
    color: #009CA6;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
}

h1, h2, h3 {
    color: #082238;
}

.stButton > button {
    border-radius: 8px;
    border: 1px solid #009CA6;
}

.stDownloadButton > button {
    border-radius: 8px;
}

[data-testid="stChatMessage"] {
    background-color: #F8FAFC;
    border: 1px solid #E2E8F0;
    border-radius: 10px;
}

</style>
""")


# ============================================================
# 3. DATA LOADING
# ============================================================

def read_json(path):

    with path.open(encoding="utf-8") as file:

        return json.load(file)


@st.cache_data
def load_data():

    predictions = pd.read_parquet(
        RISK_DIR / "assetguard_latest_predictions.parquet"
    )

    drivers = pd.read_parquet(
        RISK_DIR / "assetguard_latest_risk_drivers.parquet"
    )

    comparison = pd.read_csv(
        REPORT_DIR / "model_comparison.csv"
    )

    importance = pd.read_csv(
        SHAP_DIR / "shap_feature_importance.csv"
    )

    summary = read_json(
        RISK_DIR / "prediction_summary.json"
    )

    metrics = read_json(
        REPORT_DIR / "test_metrics.json"
    )

    metadata = read_json(
        MODEL_DIR / "model_metadata.json"
    )

    validation = read_json(
        FEATURE_DIR / "assetguard_feature_validation.json"
    )

    return (
        predictions,
        drivers,
        comparison,
        importance,
        summary,
        metrics,
        metadata,
        validation
    )


try:

    (
        predictions,
        drivers,
        comparison,
        importance,
        summary,
        metrics,
        metadata,
        validation
    ) = load_data()

except Exception as error:

    st.error(
        f"Unable to load project data: {error}"
    )

    st.stop()


# ============================================================
# 4. SHARED UTILITIES
# ============================================================

def run_agent(method, *args):

    agent = AssetGuardAgent()

    try:

        return getattr(agent, method)(*args)

    finally:

        agent.close()




# ============================================================
# GENERATIVE AI QUOTA MANAGEMENT
# ============================================================

from datetime import datetime, timezone
from threading import Lock

DAILY_AI_LIMIT = 200
SESSION_AI_LIMIT = 200


@st.cache_resource
def get_ai_quota():

    return {
        "date": datetime.now(timezone.utc).date(),
        "requests": 0,
        "lock": Lock()
    }


def reserve_ai_request():

    quota = get_ai_quota()
    today = datetime.now(timezone.utc).date()

    with quota["lock"]:

        if quota["date"] != today:

            quota["date"] = today
            quota["requests"] = 0

        if st.session_state.get("ai_quota_date") != today:

            st.session_state["ai_quota_date"] = today
            st.session_state["ai_session_requests"] = 0

        session_requests = st.session_state.get(
            "ai_session_requests", 0
        )

        if session_requests + 2 > SESSION_AI_LIMIT:

            return False, "Session AI limit reached."

        if quota["requests"] + 2 > DAILY_AI_LIMIT:

            return False, "Daily AI limit reached."

        quota["requests"] += 2

        st.session_state["ai_session_requests"] = (
            session_requests + 2
        )

        return True, ""


def run_llama_assistant(question):

    import os

    allowed, reason = reserve_ai_request()

    if not allowed:

        result = run_agent("run", question)

        result["llm_status"] = "fallback"

        result["llm_message"] = (
            reason
            + " Original analytical evidence remains available."
        )

        return result

    account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.getenv("CLOUDFLARE_API_TOKEN")

    try:

        account_id = st.secrets.get(
            "CLOUDFLARE_ACCOUNT_ID", account_id
        )

        api_token = st.secrets.get(
            "CLOUDFLARE_API_TOKEN", api_token
        )

    except FileNotFoundError:

        pass

    assistant = AssetGuardLlamaAgent(
        account_id=account_id,
        api_token=api_token
    )

    try:

        return assistant.run(question)

    finally:

        assistant.close()


def page_header(title, description):

    st.title(title)

    st.caption(description)

    st.divider()


def chart_style(fig):

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(
            family="Arial",
            color="#082238",
            size=13
        ),
        margin=dict(
            l=25,
            r=25,
            t=45,
            b=25
        ),
        legend=dict(
            bgcolor="rgba(255,255,255,0)"
        )
    )

    fig.update_xaxes(
        gridcolor="#E8EDF2",
        linecolor="#DCE4EC"
    )

    fig.update_yaxes(
        gridcolor="#E8EDF2",
        linecolor="#DCE4EC"
    )

    return fig


def display_table(df):

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True
    )


def download_csv(df, filename):

    st.download_button(
        "Download CSV",
        data=df.to_csv(
            index=False
        ).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )


def asset_selector(key):

    assets = predictions.sort_values(
        "risk_rank"
    )

    asset_ids = assets[
        "asset_identity"
    ].tolist()

    labels = {

        int(row.asset_identity):

        f"{row.asset_identity} | "
        f"{row.asset_code} | "
        f"{row.risk_category}"

        for row in assets.itertuples()

    }

    return st.selectbox(
        "Select an asset",
        options=asset_ids,
        format_func=lambda value: labels[int(value)],
        key=key
    )


def show_asset_assessment(asset_id):

    asset = predictions.loc[

        predictions["asset_identity"] == asset_id

    ].iloc[0]

    st.subheader(
        str(asset["asset_code"])
    )

    st.caption(
        f"Asset Identity: {asset_id}"
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Risk Score",
        f"{asset['risk_score']:.4f}"
    )

    c2.metric(
        "Risk Category",
        str(asset["risk_category"])
    )

    c3.metric(
        "Portfolio Rank",
        f"#{int(asset['risk_rank']):,}"
    )

    c4.metric(
        "Annual Corrective Orders",
        f"{int(asset['cm_count_365d']):,}"
    )

    st.caption(
        "Uncalibrated model score. "
        "Not a physical failure probability."
    )

    return asset


# ============================================================
# 5. SIDEBAR NAVIGATION
# LOGO IS DISPLAYED ONLY HERE
# ============================================================

with st.sidebar:

    if LOGO_PATH.exists():

        st.image(
            str(LOGO_PATH),
            use_container_width=True
        )

    else:

        st.markdown("## AssetFusion360")

        st.caption("Logo file not found.")

    st.caption(
        "AssetGuard Intelligence Platform"
    )

    st.divider()

    page = st.radio(
        "Navigation",
        [
            "Executive Overview",
            "Asset Risk Explorer",
            "Asset 360",
            "Explainable AI",
            "Maintenance Case Search",
            "AI Assistant",
            "Model Governance"
        ],
        label_visibility="collapsed"
    )

    st.divider()

    st.markdown("### Model Information")

    st.write(
        "Algorithm:",
        metadata["model_name"]
    )

    st.write(
        "Test ROC-AUC:",
        f"{metrics['roc_auc']:.2%}"
    )

    st.write(
        "Model Threshold:",
        f"{metadata['threshold']:.4f}"
    )

    st.divider()

    st.markdown("### Forecast Window")

    start = pd.Timestamp(
        summary["prediction_start"]
    )

    end = pd.Timestamp(
        summary["prediction_end_exclusive"]
    ) - pd.Timedelta(days=1)

    st.write(
        start.strftime("%b %d, %Y"),
        "—",
        end.strftime("%b %d, %Y")
    )

    st.warning(
        "Historical cutoff forecast. "
        "Source refresh required for current scoring."
    )

    st.divider()

    st.caption(
        "Rutgers Business School"
    )

    st.caption(
        "Master of Information Technology & Analytics"
    )

    st.caption(
        "Capstone Project | 2026"
    )


# ============================================================
# PAGE 1: EXECUTIVE OVERVIEW
# NO DUPLICATE LOGO IN MAIN CONTENT
# ============================================================

if page == "Executive Overview":

    st.html("""
    <div class="hero">

        <div class="section-label">
            AssetGuard Intelligence Platform
        </div>

        <div class="hero-title">
            Executive Overview
        </div>

        <div class="hero-subtitle">
            Enterprise Asset Management |
            Predictive Maintenance |
            Explainable AI
        </div>

    </div>
    """)

    high_count = int(

        (
            predictions["risk_category"] == "High"
        ).sum()

    )

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        "Assets Scored",
        f"{len(predictions):,}"
    )

    c2.metric(
        "High-Risk Assets",
        f"{high_count:,}"
    )

    c3.metric(
        "High-Risk Share",
        f"{high_count / len(predictions):.1%}"
    )

    c4.metric(
        "Test ROC-AUC",
        f"{metrics['roc_auc']:.2%}"
    )

    c5.metric(
        "SHAP Risk Drivers",
        f"{len(drivers):,}"
    )

    st.write("")

    distribution = (

        predictions.groupby("risk_category")

        .size()

        .reindex(
            RISK_ORDER,
            fill_value=0
        )

        .reset_index(name="assets")

    )

    left, right = st.columns(2)

    with left:

        st.subheader(
            "Portfolio Risk Distribution"
        )

        fig = px.pie(
            distribution,
            names="risk_category",
            values="assets",
            hole=0.68,
            color="risk_category",
            color_discrete_map=COLORS
        )

        fig.add_annotation(
            text=f"{len(predictions):,}<br>Assets",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(
                size=22,
                color=NAVY
            )
        )

        fig.update_traces(
            textposition="outside",
            textinfo="percent+label",
            sort=False,
            marker=dict(
                line=dict(
                    color="white",
                    width=2
                )
            )
        )

        fig.update_layout(
            height=420
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

    with right:

        st.subheader(
            "Assets by Risk Category"
        )

        fig = px.bar(
            distribution,
            x="risk_category",
            y="assets",
            color="risk_category",
            color_discrete_map=COLORS,
            text="assets",
            category_orders={
                "risk_category": RISK_ORDER
            }
        )

        fig.update_traces(
            texttemplate="%{text:,}",
            textposition="outside",
            cliponaxis=False
        )

        fig.update_layout(
            height=420,
            showlegend=False,
            xaxis_title="Risk Category",
            yaxis_title="Number of Assets"
        )

        fig.update_yaxes(
            range=[
                0,
                max(distribution["assets"]) * 1.18
            ]
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

    st.divider()

    st.subheader(
        "Priority Maintenance Register"
    )

    st.caption(
        "Assets ranked by model-generated maintenance risk."
    )

    top = predictions.sort_values(
        "risk_rank"
    ).head(20)

    columns = [
        "risk_rank",
        "asset_identity",
        "asset_code",
        "risk_score",
        "risk_category",
        "cm_count_365d",
        "days_since_last_cm",
        "recommended_action"
    ]

    display_table(
        top[columns]
    )

    download_csv(
        top[columns],
        "priority_maintenance_register.csv"
    )

    st.caption(
        "Risk scores are uncalibrated model outputs, "
        "not physical failure probabilities."
    )


# ============================================================
# PAGE 2: ASSET RISK EXPLORER
# ============================================================

elif page == "Asset Risk Explorer":

    page_header(
        "Asset Risk Explorer",
        "Search, filter and prioritize the scored asset portfolio."
    )

    c1, c2, c3 = st.columns(3)

    categories = c1.multiselect(
        "Risk Categories",
        RISK_ORDER,
        default=RISK_ORDER
    )

    search = c2.text_input(
        "Search Asset ID or Asset Code"
    )

    score_range = c3.slider(
        "Model Score Range",
        0.0,
        1.0,
        (0.0, 1.0),
        0.01
    )

    filtered = predictions.loc[

        predictions["risk_category"].isin(
            categories
        )

        & predictions["risk_score"].between(
            score_range[0],
            score_range[1]
        )

    ].copy()

    if search:

        matched = (

            filtered["asset_identity"]
            .astype(str)
            .str.contains(
                search,
                case=False,
                regex=False,
                na=False
            )

            |

            filtered["asset_code"]
            .astype(str)
            .str.contains(
                search,
                case=False,
                regex=False,
                na=False
            )

        )

        filtered = filtered.loc[matched]

    filtered = filtered.sort_values(
        "risk_rank"
    )

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Matching Assets",
        f"{len(filtered):,}"
    )

    c2.metric(
        "Average Risk Score",
        f"{filtered['risk_score'].mean():.4f}"
        if len(filtered) else "N/A"
    )

    c3.metric(
        "High-Risk Assets",
        f"{(filtered['risk_category'] == 'High').sum():,}"
    )

    if not filtered.empty:

        fig = px.histogram(
            filtered,
            x="risk_score",
            color="risk_category",
            color_discrete_map=COLORS,
            nbins=50,
            title="Risk Score Distribution"
        )

        fig.update_layout(
            xaxis_title="Model Risk Score",
            yaxis_title="Number of Assets"
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

        st.subheader(
            "Filtered Asset Register"
        )

        display_table(

            filtered[[
                "risk_rank",
                "asset_identity",
                "asset_code",
                "asset_status",
                "risk_category",
                "risk_score",
                "wo_count_365d",
                "cm_count_365d",
                "days_since_last_cm",
                "recommended_action"
            ]]

        )

        download_csv(
            filtered,
            "filtered_asset_risk_register.csv"
        )

    else:

        st.info(
            "No assets match the selected filters."
        )


# ============================================================
# PAGE 3: ASSET 360
# ============================================================

elif page == "Asset 360":

    page_header(
        "Asset 360",
        "Individual asset risk, history and maintenance recommendations."
    )

    asset_id = asset_selector(
        "asset_360"
    )

    asset = show_asset_assessment(
        asset_id
    )

    tab1, tab2, tab3, tab4 = st.tabs([

        "Risk Drivers",
        "Maintenance Profile",
        "Work Order History",
        "Recommendations"

    ])

    with tab1:

        selected = drivers.loc[

            drivers["asset_identity"] == asset_id

        ].copy()

        selected = selected.sort_values(
            "feature_rank",
            ascending=False
        )

        feature_labels = {

            "cm_count_365d":
                "Annual Corrective Maintenance",

            "wo_count_365d":
                "Annual Work Order Frequency",

            "days_since_last_cm":
                "Corrective Maintenance Recency",

            "days_since_last_wo":
                "Work Order Recency",

            "cm_count_90d":
                "90-Day Corrective Maintenance",

            "cm_count_30d":
                "30-Day Corrective Maintenance",

            "wo_count_30d":
                "30-Day Work Order Frequency",

            "wo_count_90d":
                "90-Day Work Order Frequency",

            "wo_count_180d":
                "180-Day Work Order Frequency",

            "snapshot_month":
                "Seasonal Maintenance Pattern",

            "corrective_ratio_90d":
                "Corrective Maintenance Ratio",

            "no_cm_history_365d":
                "Absence of Corrective History"

        }

        selected["business_feature"] = (

            selected["feature"]

            .map(feature_labels)

            .fillna(selected["feature"])

        )

        fig = px.bar(
            selected,
            x="shap_value",
            y="business_feature",
            orientation="h",
            color="risk_direction",
            color_discrete_map={
                "Increases Risk": "#DC2626",
                "Decreases Risk": "#16A34A",
                "Neutral": "#94A3B8"
            },
            title="Individual SHAP Risk Drivers"
        )

        fig.update_layout(
            xaxis_title="SHAP Contribution",
            yaxis_title="",
            height=400
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

        display_table(

            selected[[
                "feature_rank",
                "business_feature",
                "feature_value",
                "shap_value",
                "risk_direction"
            ]].sort_values("feature_rank")

        )

        st.caption(
            "SHAP values explain contributions to the "
            "raw model margin, not percentage-point "
            "changes in probability."
        )

        download_csv(
            selected,
            f"asset_{asset_id}_risk_drivers.csv"
        )

    with tab2:

        profile = pd.DataFrame({

            "Period": [
                "30 Days",
                "90 Days",
                "365 Days"
            ],

            "Total Work Orders": [
                asset["wo_count_30d"],
                asset["wo_count_90d"],
                asset["wo_count_365d"]
            ],

            "Corrective Work Orders": [
                asset["cm_count_30d"],
                asset["cm_count_90d"],
                asset["cm_count_365d"]
            ]

        })

        fig = px.bar(
            profile,
            x="Period",
            y=[
                "Total Work Orders",
                "Corrective Work Orders"
            ],
            barmode="group",
            color_discrete_sequence=[
                "#082238",
                "#009CA6"
            ],
            title="Historical Maintenance Profile"
        )

        fig.update_layout(
            yaxis_title="Work Orders",
            legend_title=""
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

        c1, c2 = st.columns(2)

        c1.metric(
            "Days Since Last Work Order",
            int(asset["days_since_last_wo"])
        )

        cm_recency = (

            "None in 365 days"

            if asset["no_cm_history_365d"]

            else str(
                int(asset["days_since_last_cm"])
            )

        )

        c2.metric(
            "Corrective Maintenance Recency",
            cm_recency
        )

    with tab3:

        if st.button(
            "Load Maintenance History"
        ):

            with st.spinner(
                "Retrieving work orders..."
            ):

                history = run_agent(
                    "get_maintenance_history",
                    asset_id,
                    50
                )

            st.session_state[
                "asset_history_result"
            ] = history

            st.session_state[
                "asset_history_id"
            ] = asset_id

        if (

            st.session_state.get(
                "asset_history_id"
            ) == asset_id

        ):

            history = st.session_state[
                "asset_history_result"
            ]

            display_table(history)

            download_csv(
                history,
                f"asset_{asset_id}_history.csv"
            )

            st.caption(
                "Records created before the forecast cutoff."
            )

    with tab4:

        recommendation = run_agent(
            "recommend_action",
            asset_id
        )

        st.subheader(
            "Recommended Maintenance Actions"
        )

        for action in recommendation.get(
            "recommended_actions",
            []
        ):

            st.write("•", action)

        st.warning(
            "Human review required. "
            "Confirm current asset condition before action."
        )


# ============================================================
# PAGE 4: EXPLAINABLE AI
# ============================================================

elif page == "Explainable AI":

    page_header(
        "Explainable AI",
        "Model transparency, performance and SHAP interpretation."
    )

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        "ROC-AUC",
        f"{metrics['roc_auc']:.2%}"
    )

    c2.metric(
        "Average Precision",
        f"{metrics['average_precision']:.2%}"
    )

    c3.metric(
        "Recall",
        f"{metrics['recall']:.2%}"
    )

    c4.metric(
        "Precision",
        f"{metrics['precision']:.2%}"
    )

    c5.metric(
        "F1 Score",
        f"{metrics['f1_score']:.2%}"
    )

    left, right = st.columns(2)

    with left:

        st.subheader(
            "Global Feature Importance"
        )

        fig = px.bar(

            importance.sort_values(
                "mean_absolute_shap"
            ),

            x="mean_absolute_shap",
            y="feature",
            orientation="h",
            color_discrete_sequence=[
                "#009CA6"
            ]
        )

        fig.update_layout(
            height=500,
            xaxis_title="Mean Absolute SHAP Value",
            yaxis_title=""
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

    with right:

        st.subheader(
            "Test Confusion Matrix"
        )

        matrix = [

            [
                metrics["true_negative"],
                metrics["false_positive"]
            ],

            [
                metrics["false_negative"],
                metrics["true_positive"]
            ]

        ]

        fig = go.Figure(

            data=go.Heatmap(

                z=matrix,

                x=[
                    "Predicted Negative",
                    "Predicted Positive"
                ],

                y=[
                    "Actual Negative",
                    "Actual Positive"
                ],

                text=[

                    [
                        f"{int(value):,}"
                        for value in row
                    ]

                    for row in matrix

                ],

                texttemplate="%{text}",

                colorscale=[
                    [0, "#EAF8F8"],
                    [0.5, "#58B9BE"],
                    [1, "#087E86"]
                ],

                showscale=False

            )

        )

        fig.update_layout(
            height=500
        )

        fig.update_yaxes(
            autorange="reversed"
        )

        st.plotly_chart(
            chart_style(fig),
            use_container_width=True
        )

    st.divider()

    st.subheader(
        "Validation Model Comparison"
    )

    fig = px.bar(
        comparison,
        x="model",
        y=[
            "average_precision",
            "roc_auc",
            "f1_score"
        ],
        barmode="group",
        color_discrete_sequence=[
            "#009CA6",
            "#082238",
            "#60A5FA"
        ]
    )

    fig.update_yaxes(
        tickformat=".0%",
        range=[0, 1]
    )

    fig.update_layout(
        xaxis_title="Machine Learning Model",
        yaxis_title="Metric Score",
        legend_title=""
    )

    st.plotly_chart(
        chart_style(fig),
        use_container_width=True
    )

    display_table(comparison)

    download_csv(
        comparison,
        "assetguard_model_comparison.csv"
    )

    st.divider()

    st.subheader(
        "SHAP Summary Plot"
    )

    image = SHAP_DIR / "shap_summary_plot.png"

    if image.exists():

        st.image(
            str(image),
            use_container_width=True
        )

        st.download_button(

            "Download SHAP Summary Plot",

            data=image.read_bytes(),

            file_name="shap_summary_plot.png",

            mime="image/png"

        )

    st.caption(
        "Global SHAP results are based on a reproducible "
        "sample of 10,000 test observations. "
        "Feature importance does not establish causation."
    )


# ============================================================
# PAGE 5: MAINTENANCE CASE SEARCH
# ============================================================


elif page == "Maintenance Case Search":

    import time

    page_header(
        "Maintenance Case Search",
        "Semantic and keyword-based historical maintenance retrieval."
    )

    st.caption(
        "Search 849,667 unique maintenance descriptions "
        "representing 1,059,478 eligible corrective work orders."
    )

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Indexed Descriptions",
        "849,667"
    )

    c2.metric(
        "Represented Work Orders",
        "1,059,478"
    )

    c3.metric(
        "FAISS Index Size",
        "49.56 MB"
    )

    st.divider()

    with st.form("maintenance_search"):

        query = st.text_input(
            "Describe the Maintenance Issue",
            placeholder=(
                "Leaking pipe, broken door, "
                "damaged playground equipment..."
            ),
            max_chars=500
        )

        method = st.radio(
            "Retrieval Method",
            [
                "Semantic Search",
                "Keyword Search"
            ],
            horizontal=True
        )

        limit = st.slider(
            "Maximum Results",
            5,
            50,
            10,
            5
        )

        submitted = st.form_submit_button(
            "Search Historical Work Orders"
        )

    if submitted:

        st.session_state.pop(
            "case_results",
            None
        )

        if not query.strip():

            st.warning(
                "Enter a maintenance issue."
            )

        else:

            with st.spinner(
                "Searching historical work orders..."
            ):

                start_time = time.perf_counter()

                actual_method = method

                if method == "Semantic Search":

                    try:

                        from assetfusion360.pipelines.assetguard.llama_agent import (
                            get_semantic_engine,
                            _semantic_lock
                        )

                        with _semantic_lock:

                            engine = get_semantic_engine()

                            results = engine.search(
                                query,
                                limit=limit
                            )

                    except Exception:

                        actual_method = "Keyword Search (Fallback)"

                        results = run_agent(
                            "find_similar_workorders",
                            query,
                            limit
                        )

                else:

                    results = run_agent(
                        "find_similar_workorders",
                        query,
                        limit
                    )

                latency = (
                    time.perf_counter() - start_time
                ) * 1000

            st.session_state["case_results"] = results

            st.session_state["case_search_query"] = query

            st.session_state["case_search_method"] = actual_method

            st.session_state["case_search_latency"] = latency

    if "case_results" in st.session_state:

        results = st.session_state["case_results"]

        actual_method = st.session_state.get(
            "case_search_method",
            "Keyword Search"
        )

        st.subheader("Historical Maintenance Cases")

        st.caption(
            "Search: "
            + st.session_state.get(
                "case_search_query",
                ""
            )
        )

        if "Fallback" in actual_method:

            st.warning(
                "Semantic retrieval is temporarily unavailable. "
                "Keyword results are displayed instead."
            )

        c1, c2, c3 = st.columns(3)

        c1.metric(
            "Retrieved Cases",
            len(results)
        )

        c2.metric(
            "Search Time",
            f"{st.session_state.get('case_search_latency', 0):,.2f} ms"
        )

        c3.metric(
            "Retrieval Engine",
            actual_method
        )

        if not results.empty:

            if "semantic_similarity" in results.columns:

                st.metric(
                    "Highest Similarity Score",
                    f"{results['semantic_similarity'].max():.4f}"
                )

                st.caption(
                    "Approximate vector similarity scores "
                    "are not confidence probabilities."
                )

                chart_data = results.head(10).copy()

                chart_data["case_label"] = (
                    chart_data["workorder_code"].astype(str)
                    + " | "
                    + chart_data["workorder_description"]
                    .astype(str).str.slice(0, 65)
                )

                fig = px.bar(
                    chart_data.sort_values(
                        "semantic_similarity"
                    ),
                    x="semantic_similarity",
                    y="case_label",
                    orientation="h",
                    color_discrete_sequence=["#009CA6"]
                )

                fig.update_layout(
                    height=420,
                    xaxis_title="Approximate Similarity",
                    yaxis_title=""
                )

                st.plotly_chart(
                    chart_style(fig),
                    use_container_width=True,
                    key="semantic_case_similarity_chart"
                )

            display_table(results)

            download_csv(
                results,
                "historical_maintenance_cases.csv"
            )

        else:

            st.info(
                "No matching work orders found."
            )

    st.divider()

    st.subheader("Retrieval Performance Benchmark")

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Keyword Search",
        "763.68 ms"
    )

    c2.metric(
        "Semantic Search",
        "34.21 ms"
    )

    c3.metric(
        "Local Retrieval Speedup",
        "22.32x"
    )

    benchmark = pd.DataFrame({

        "Retrieval Method": [
            "Keyword Search",
            "Semantic Search"
        ],

        "Average Latency (ms)": [
            763.68,
            34.21
        ]

    })

    fig = px.bar(
        benchmark,
        x="Retrieval Method",
        y="Average Latency (ms)",
        color="Retrieval Method",
        text="Average Latency (ms)",
        color_discrete_map={
            "Keyword Search": "#94A3B8",
            "Semantic Search": "#009CA6"
        }
    )

    fig.update_traces(
        textposition="outside"
    )

    fig.update_layout(
        showlegend=False,
        height=350,
        yaxis_range=[0, 950]
    )

    st.plotly_chart(
        chart_style(fig),
        use_container_width=True,
        key="retrieval_benchmark_chart"
    )

    st.caption(
        "Local benchmark across 10 maintenance queries, "
        "excluding initial model loading. "
        "Cloud runtime performance may differ. "
        "Formal relevance assessment remains pending."
    )

    st.info(
        "Semantic retrieval uses MiniLM sentence embeddings "
        "and a compressed FAISS IVF-PQ index. "
        "Neither retrieval method consumes Llama API quota."
    )


# ============================================================
# PAGE 6: AI ASSISTANT
# ============================================================

elif page == "AI Assistant":

    page_header(
        "AssetGuard Intelligence Assistant",
        "Generative AI and evidence-grounded maintenance decision support."
    )

    st.info(
        "Ask about asset risk, maintenance history, "
        "SHAP explanations or similar historical cases."
    )

    if "agent_messages" not in st.session_state:

        st.session_state[
            "agent_messages"
        ] = []


    def render_response(result, response_id):

        if result.get("llm_status") == "success":

            st.caption(
                "Powered by Meta Llama 4 Scout | Cloudflare Workers AI"
            )

            st.subheader("AI-Generated Maintenance Assessment")

            st.markdown(result["llm_answer"])

            st.divider()

            st.subheader("Supporting Analytical Evidence")

        elif result.get("llm_status") == "fallback":

            st.info(
                result.get(
                    "llm_message",
                    "Displaying original analytical evidence."
                )
            )


        intent = result.get(
            "intent",
            ""
        )

        if intent == "asset_assessment":

            asset = result["risk"].iloc[0]

            st.markdown(
                f"**Asset {asset['asset_identity']} — "
                f"{asset['asset_code']}**"
            )

            c1, c2, c3 = st.columns(3)

            c1.metric(
                "Risk Category",
                str(asset["risk_category"])
            )

            c2.metric(
                "Model Risk Score",
                f"{asset['risk_score']:.4f}"
            )

            c3.metric(
                "Annual Corrective Orders",
                f"{int(asset['cm_count_365d']):,}"
            )

            st.markdown(
                "**Key Risk Drivers**"
            )

            explanations = result[
                "explanations"
            ].copy()

            display_table(
                explanations
            )

            fig = px.bar(
                explanations.sort_values(
                    "feature_rank",
                    ascending=False
                ),
                x="shap_value",
                y="business_description",
                orientation="h",
                color="risk_direction",
                color_discrete_map={
                    "Increases Risk": "#DC2626",
                    "Decreases Risk": "#16A34A",
                    "Neutral": "#94A3B8"
                }
            )

            fig.update_layout(
                height=350,
                xaxis_title="SHAP Contribution",
                yaxis_title=""
            )

            st.plotly_chart(
                chart_style(fig),
                use_container_width=True,
                key=f"agent_shap_chart_{response_id}"
            )

            st.markdown(
                "**Recommended Actions**"
            )

            for action in result[
                "recommendation"
            ]["recommended_actions"]:

                st.write("•", action)

            with st.expander(
                "Supporting Maintenance History"
            ):

                display_table(
                    result["history"]
                )

            st.caption(
                "Historical forecast. "
                "Model scores are uncalibrated. "
                "Human approval required."
            )

        elif "data" in result:

            st.write(

                intent.replace(
                    "_", " "
                ).title()

            )

            display_table(
                result["data"]
            )

        else:

            st.write(

                result.get(
                    "message",
                    "No result available."
                )

            )


    if st.button(
        "Clear Conversation"
    ):

        st.session_state[
            "agent_messages"
        ] = []

        st.rerun()

    examples = [

        "Show portfolio summary",

        "Show top 10 high risk assets",

        "Why is asset 14167 high risk?",

        "Show maintenance history for asset 14167",

        "Find similar workorders for leaking pipe"

    ]

    selected_example = st.selectbox(

        "Example Questions",

        ["Choose a question"] + examples

    )

    example_clicked = st.button(

        "Ask Selected Question",

        disabled=(

            selected_example == "Choose a question"

        )

    )

    for index, message in enumerate(
        st.session_state["agent_messages"]
    ):

        with st.chat_message(
            message["role"]
        ):

            if message["role"] == "user":

                st.write(
                    message["content"]
                )

            else:

                # A unique container prevents duplicate
                # chart element IDs in chat history.

                with st.container(
                    key=f"agent_response_{index}"
                ):

                    render_response(
                        message["content"],
                        index
                    )

    question = st.chat_input(
        "Ask AssetGuard..."
    )

    if example_clicked:

        question = selected_example

    if question:

        st.session_state[
            "agent_messages"
        ].append({

            "role": "user",

            "content": question

        })

        with st.spinner(
            "Analyzing maintenance intelligence..."
        ):

            try:

                result = run_llama_assistant(question)

            except Exception as error:

                result = {

                    "intent": "error",

                    "message": str(error)

                }

        st.session_state[
            "agent_messages"
        ].append({

            "role": "assistant",

            "content": result

        })

        st.rerun()

    st.caption(
        "Powered by Meta Llama 4 Scout, DuckDB, XGBoost and SHAP. "
        "Human review is required for maintenance decisions."
    )


# ============================================================
# PAGE 7: MODEL GOVERNANCE
# ============================================================

elif page == "Model Governance":

    page_header(
        "Model Governance",
        "Model methodology, traceability and responsible deployment."
    )

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Selected Algorithm",
        metadata["model_name"]
    )

    c2.metric(
        "Model Features",
        len(metadata["features"])
    )

    c3.metric(
        "Prediction Horizon",
        f"{metadata['prediction_window_days']} Days"
    )

    st.subheader(
        "Model Information"
    )

    st.json(metadata)

    st.divider()

    st.subheader(
        "Dataset Validation"
    )

    st.write(
        "Validation Status:",
        validation["status"]
    )

    split_summary = pd.DataFrame(
        validation["dataset_summary"]
    )

    display_table(
        split_summary
    )

    st.subheader(
        "Temporal Dataset Distribution"
    )

    fig = px.bar(

        split_summary,

        x="dataset_split",

        y="records",

        color="dataset_split",

        text="records",

        color_discrete_map={

            "train": "#009CA6",

            "validation": "#F59E0B",

            "test": "#082238"

        },

        category_orders={

            "dataset_split": [
                "train",
                "validation",
                "test"
            ]

        }

    )

    fig.update_traces(
        texttemplate="%{text:,}",
        textposition="outside"
    )

    fig.update_layout(
        showlegend=False,
        xaxis_title="Dataset Split",
        yaxis_title="Number of Observations"
    )

    fig.update_yaxes(
        range=[
            0,
            split_summary["records"].max() * 1.2
        ]
    )

    st.plotly_chart(
        chart_style(fig),
        use_container_width=True
    )

    st.divider()

    st.subheader(
        "Experiment Tracking"
    )

    st.write(
        "MLflow Run ID"
    )

    st.code(
        metadata["mlflow_run_id"]
    )

    st.write(
        "Run MLflow locally from the project root:"
    )

    st.code(

        "mlflow ui --backend-store-uri "
        "sqlite:///assetguard_mlflow.db "
        "--host 127.0.0.1 --port 5000",

        language="powershell"

    )

    st.divider()

    st.subheader(
        "Model Limitations"
    )

    limitations = [

        "Predictions represent corrective maintenance "
        "demand rather than physical equipment failure.",

        "Model risk scores are not calibrated probabilities.",

        "Historical work-order attributes may have changed "
        "after their original creation.",

        "Only assets with work orders in the previous "
        "365 days are eligible for scoring.",

        "The current batch uses work-order history through "
        "October 24, 2026.",

        "The batch forecast uses a different day of month "
        "from the historical training snapshots.",

        "Medium, Moderate and Low risk boundaries "
        "are provisional business rules.",

        "Random Forest was trained on a smaller "
        "stratified training sample.",

        "The maintenance assistant uses deterministic "
        "intent routing and keyword retrieval.",

        "Maintenance actions require human review "
        "and confirmation of present asset condition."

    ]

    for limitation in limitations:

        st.write("•", limitation)

    st.subheader(
        "Deployment Status"
    )

    st.info(
        "Local research prototype. "
        "Production deployment requires source refresh, "
        "monitoring, access controls and further validation."
    )


# ============================================================
# 6. APPLICATION FOOTER
# ============================================================

st.divider()

st.html("""
    <div style="
        text-align:center;
        color:#64748B;
        font-size:12px;
        padding:12px;
    ">

        <strong style="color:#082238;">
            AssetFusion360
        </strong>

        &nbsp; | &nbsp;

        AssetGuard Intelligence Platform

        &nbsp; | &nbsp;

        Rutgers Business School — MITA Capstone 2026

    </div>
    """)
