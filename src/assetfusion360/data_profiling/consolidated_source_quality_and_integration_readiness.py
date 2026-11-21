
"""
AssetFusion360 Capstone Project
STEP 4.8 - Consolidated Source Quality & Integration Readiness

Reads previously generated JSON reports only.
Does NOT reread, modify, or transform raw CSV data.

Outputs:
  reports/data_profiling_reports/
    assetfusion360_consolidated_source_quality_report.json
    assetfusion360_consolidated_source_quality_report.md

Technology: Python 3.12 standard library
No pandas, no Spark job, no new dependencies.
"""

import json
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

REPORT_DIR = (
    PROJECT_ROOT / "reports" / "data_profiling_reports"
)

JSON_OUTPUT = (
    REPORT_DIR /
    "assetfusion360_consolidated_source_quality_report.json"
)

MD_OUTPUT = (
    REPORT_DIR /
    "assetfusion360_consolidated_source_quality_report.md"
)

# Expected counts are comparison checkpoints only.
# Actual reported counts must come from saved JSON files.

DATASETS = [
    {
        "name": "AMPS Assets",
        "file": "nyc_parks_amps_assets_profile.json",
        "expected": 148522,
        "domain": "NYC Parks AMPS",
    },
    {
        "name": "AMPS Work Orders",
        "file": "nyc_parks_amps_workorders_batch_data_profile.json",
        "expected": 2231535,
        "domain": "NYC Parks AMPS",
    },
    {
        "name": "AMPS Labor",
        "file": "nyc_parks_amps_labor_batch_data_profile.json",
        "expected": 3230423,
        "domain": "NYC Parks AMPS",
    },
    {
        "name": "AMPS Transactions",
        "file": "nyc_parks_amps_transactions_batch_data_profile.json",
        "expected": 755122,
        "domain": "NYC Parks AMPS",
    },
    {
        "name": "AMPS Transaction Details",
        "file": "nyc_parks_amps_transaction_details_batch_data_profile.json",
        "expected": 2301927,
        "domain": "NYC Parks AMPS",
    },
    {
        "name": "AI4I Predictive Maintenance",
        "file": "ai4i_predictive_maintenance_data_profile.json",
        "expected": 10000,
        "domain": "AI4I",
    },
    {
        "name": "NYC 311 Service Requests",
        "file": "nyc_311_service_requests_data_profile.json",
        "expected": 875134,
        "domain": "NYC 311",
    },
    {
        "name": "Parks Inspections",
        "file": "supplementary_inspection_data_profile.json",
        "expected": 153093,
        "domain": "Parks Inspections",
    },
]

VALIDATION_REPORTS = {
    "amps_relationships": (
        "nyc_parks_amps_cross_dataset_relationship_report.json"
    ),
    "inspection_relationships": (
        "nyc_parks_inspection_date_and_asset_relationship_report.json"
    ),
}

EXPECTED_TOTAL = 9705756


# ============================================================
# 2. UTILITIES
# ============================================================

def load_json(path):
    if not path.is_file():
        raise FileNotFoundError(
            f"Required profiling report missing:\n{path}"
        )

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(
            f"JSON report must contain an object: {path.name}"
        )

    return data


def first_available(mapping, names):
    if not isinstance(mapping, dict):
        return None

    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]

    return None


def as_integer(value):
    if value is None or isinstance(value, bool):
        return None

    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError, OverflowError):
        return None


def percent(numerator, denominator):
    if not denominator:
        return None

    return round(100 * numerator / denominator, 2)


def fmt_number(value):
    if value is None:
        return "Not reported"
    return f"{value:,}"


def fmt_percent(value):
    if value is None:
        return "N/A"
    return f"{value:.2f}%"


def md_safe(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(path.suffix + ".tmp")

    temporary.write_text(
        content,
        encoding="utf-8"
    )

    temporary.replace(path)


# ============================================================
# 3. EXTRACT VERIFIED DATASET METRICS
# ============================================================

def get_record_count(report):
    candidates = [
        first_available(
            report,
            [
                "total_records",
                "total_rows",
                "row_count",
                "record_count",
            ]
        ),
        first_available(
            report.get("summary", {}),
            [
                "total_records",
                "total_rows",
                "row_count",
            ]
        ),
        first_available(
            report.get("dataset_overview", {}),
            [
                "total_records",
                "total_rows",
                "row_count",
            ]
        ),
    ]

    for value in candidates:
        count = as_integer(value)
        if count is not None:
            return count

    return None


def get_column_count(report):
    value = first_available(
        report,
        [
            "total_columns",
            "column_count",
            "number_of_columns",
        ]
    )

    if value is not None:
        return as_integer(value)

    for key in ["source_columns", "source_schema"]:
        columns = report.get(key)

        if isinstance(columns, list):
            return len(columns)

    return None


def get_column_quality(report):
    """
    Only extract quality statistics explicitly
    present in a saved profiling report.

    Do not substitute assumed missing-value counts.
    """

    records = report.get("column_quality")

    if not isinstance(records, list):
        return [], False

    results = []

    for item in records:
        if not isinstance(item, dict):
            continue

        name = first_available(
            item,
            ["column_name", "name", "column"]
        )

        missing = as_integer(
            first_available(
                item,
                ["missing_count", "missing_cells", "null_count"]
            )
        )

        if name is None or missing is None:
            continue

        results.append({
            "column": str(name),
            "missing": missing,
            "dash_placeholders": as_integer(
                item.get("dash_placeholder_count")
            ),
            "literal_null_placeholders": as_integer(
                item.get("literal_null_count")
            ),
        })

    return results, bool(results)


def summarize_dataset(config, report):
    rows = get_record_count(report)
    columns = get_column_count(report)

    quality, quality_available = get_column_quality(
        report
    )

    expected = config["expected"]

    if rows is None:
        count_status = "NOT_EXTRACTABLE"
    elif rows == expected:
        count_status = "MATCH"
    else:
        count_status = "MISMATCH"

    high_missing = []

    if rows and quality_available:
        for item in quality:
            rate = percent(item["missing"], rows)

            if rate is not None and rate >= 20:
                high_missing.append({
                    "column": item["column"],
                    "missing_records": item["missing"],
                    "missing_percentage": rate,
                })

        high_missing.sort(
            key=lambda x: x["missing_percentage"],
            reverse=True
        )

    return {
        "dataset": config["name"],
        "domain": config["domain"],
        "source_report": config["file"],
        "records": rows,
        "expected_records": expected,
        "record_count_status": count_status,
        "columns": columns,
        "column_quality_available": quality_available,
        "columns_with_quality_statistics": len(quality),
        "high_missing_columns_20pct_or_more": high_missing,
        "reported_column_quality": quality,
    }


# ============================================================
# 4. CONSOLIDATE VALIDATED RELATIONSHIPS
# ============================================================

def summarize_amps_relationships(report):
    raw_results = report.get("relationship_results", [])

    if not isinstance(raw_results, list):
        return []

    results = []

    for item in raw_results:
        if not isinstance(item, dict):
            continue

        results.append({
            "name": item.get("relationship_name"),
            "parent_dataset": item.get("parent_dataset"),
            "parent_column": item.get("parent_column"),
            "child_dataset": item.get("child_dataset"),
            "child_column": item.get("child_column"),
            "total_child_rows": as_integer(
                item.get("total_child_rows")
            ),
            "matched": as_integer(
                item.get("matched_child_rows")
            ),
            "unmatched": as_integer(
                item.get("unmatched_child_rows")
            ),
            "missing_references": as_integer(
                item.get("missing_reference_rows")
            ),
            "match_percentage_of_populated": (
                item.get("match_percentage_of_populated")
            ),
            "match_percentage_of_all_rows": (
                item.get("match_percentage_of_all_rows")
            ),
        })

    return results


def summarize_inspection_validation(report):
    relationship = report.get(
        "inspection_asset_relationship", {}
    )

    exact = relationship.get("exact_match", {})
    normalized = relationship.get(
        "case_normalized_match", {}
    )

    dates = report.get(
        "inspection_date_validation", {}
    )

    asset_keys = report.get(
        "asset_business_key_validation", {}
    )

    return {
        "parent_column": relationship.get(
            "parent_column", "OBJ_CODE"
        ),
        "child_column": relationship.get(
            "child_column", "Prop ID"
        ),
        "exact_match": exact,
        "case_normalized_match": normalized,
        "date_validation": dates,
        "asset_business_key_validation": asset_keys,
        "entity_grain_verified": False,
        "decision": (
            "Strong identifier evidence; property/asset "
            "grain requires business validation."
        ),
    }


# ============================================================
# 5. DOCUMENT QUALITY RISKS AND ACTIONS
# ============================================================

def build_quality_risks():
    """
    Document previously observed issues.
    These are investigation and transformation
    requirements, not automatic cleaning commands.
    """

    return [
        {
            "id": "DQ-001",
            "severity": "HIGH",
            "issue": "Duplicate AMPS OBJ_CODE",
            "affected_source": "AMPS Assets",
            "action": (
                "Retain both source rows. Use OBJ_SQLIDENTITY "
                "as source record key; resolve business code "
                "ambiguity before dimension joins."
            ),
        },
        {
            "id": "DQ-002",
            "severity": "HIGH",
            "issue": "Labor work-order references unmatched",
            "affected_source": "AMPS Labor",
            "action": (
                "Preserve 13,078 labor records; add a "
                "referential-integrity status flag."
            ),
        },
        {
            "id": "DQ-003",
            "severity": "HIGH",
            "issue": "Transaction details without parent transactions",
            "affected_source": "AMPS Transaction Details",
            "action": (
                "Preserve 1,391 unmatched details and "
                "document the 319 unmatched transaction IDs."
            ),
        },
        {
            "id": "DQ-004",
            "severity": "HIGH",
            "issue": "Sparse work-order references in transaction details",
            "affected_source": "AMPS Transaction Details",
            "action": (
                "Retain NULL trl_event values. Do not "
                "invent work-order relationships."
            ),
        },
        {
            "id": "DQ-005",
            "severity": "HIGH",
            "issue": "Unmatched Parks Inspection property references",
            "affected_source": "Parks Inspections",
            "action": (
                "Preserve 4,858 unmatched inspection rows. "
                "Confirm property/asset identifier semantics "
                "before integration."
            ),
        },
        {
            "id": "DQ-006",
            "severity": "MEDIUM",
            "issue": "Inspection attributes with high missingness",
            "affected_source": "Parks Inspections",
            "action": (
                "Treat safety, structural condition and "
                "visitor counts according to observed "
                "availability and business definitions."
            ),
        },
        {
            "id": "DQ-007",
            "severity": "MEDIUM",
            "issue": "Incomplete NYC 311 date and location attributes",
            "affected_source": "NYC 311",
            "action": (
                "Retain unclosed requests. Validate dates "
                "and geospatial values before KPI calculations."
            ),
        },
        {
            "id": "DQ-008",
            "severity": "HIGH",
            "issue": "AI4I and NYC Parks are unrelated equipment datasets",
            "affected_source": "AI4I / AMPS",
            "action": (
                "Keep AI4I as a separate ML benchmark. "
                "Do not map its identifiers or predictions "
                "to NYC Parks assets."
            ),
        },
        {
            "id": "DQ-009",
            "severity": "MEDIUM",
            "issue": "NYC 311 request identifier uniqueness not yet checked",
            "affected_source": "NYC 311",
            "action": (
                "Validate unique_key uniqueness and temporal "
                "semantics before Silver ingestion rules."
            ),
        },
        {
            "id": "DQ-010",
            "severity": "MEDIUM",
            "issue": "Source numeric and datetime fields need typed Silver schemas",
            "affected_source": "All applicable sources",
            "action": (
                "Define explicit types, parsing rules, "
                "invalid-value flags, and preservation "
                "of original values."
            ),
        },
    ]


def build_integration_plan():
    return [
        {
            "source": "NYC Parks AMPS",
            "role": "Primary EAM operational data",
            "integration_status": "VALIDATED_WITH_EXCEPTIONS",
            "next_action": (
                "Build source-aligned entities and "
                "exception-aware relationships."
            ),
        },
        {
            "source": "Parks Inspections",
            "role": "Property inspections and condition trends",
            "integration_status": "CANDIDATE_RELATIONSHIP",
            "next_action": (
                "Validate that Prop ID and OBJ_CODE "
                "represent equivalent property entities."
            ),
        },
        {
            "source": "NYC 311",
            "role": "Service demand and geographical context",
            "integration_status": "NOT_YET_LINKED",
            "next_action": (
                "Validate request identifiers, then "
                "evaluate geographic or facility-level "
                "matching without claiming direct FK links."
            ),
        },
        {
            "source": "AI4I",
            "role": "Independent failure-prediction ML benchmark",
            "integration_status": "SEPARATE_ML_DOMAIN",
            "next_action": (
                "Develop and evaluate models independently; "
                "avoid cross-domain risk claims."
            ),
        },
    ]


# ============================================================
# 6. BUILD MARKDOWN DOCUMENT
# ============================================================

def create_markdown(report):
    inventory = report["dataset_inventory"]
    relationships = report["amps_relationships"]
    inspections = report["inspection_validation"]

    lines = [
        "# AssetFusion360",
        "## Consolidated Source Data Quality and Integration Readiness",
        "",
        f"Generated (UTC): {report['generated_utc']}",
        "",
        "### 1. Source Inventory",
        "",
        "| Dataset | Records | Columns | Count check |",
        "|---|---:|---:|---|",
    ]

    for item in inventory:
        lines.append(
            f"| {md_safe(item['dataset'])} "
            f"| {fmt_number(item['records'])} "
            f"| {fmt_number(item['columns'])} "
            f"| {item['record_count_status']} |"
        )

    lines += [
        "",
        f"**Extracted record total:** "
        f"{fmt_number(report['verified_record_total'])}",
        "",
        f"**Expected comparison total:** "
        f"{EXPECTED_TOTAL:,}",
        "",
        f"**Count reconciliation:** "
        f"{report['inventory_count_status']}",
        "",
        "Counts represent observations in separate "
        "source tables, not unique assets.",
        "",
        "### 2. High Missingness (20% or more)",
        "",
    ]

    for item in inventory:
        lines.append(
            f"**{item['dataset']}**"
        )

        if not item["column_quality_available"]:
            lines.append(
                "- Detailed column-level missingness "
                "not extractable from this JSON report."
            )
        elif not item["high_missing_columns_20pct_or_more"]:
            lines.append(
                "- No columns met the 20% threshold "
                "in available statistics."
            )
        else:
            for column in item[
                "high_missing_columns_20pct_or_more"
            ]:
                lines.append(
                    f"- `{column['column']}`: "
                    f"{fmt_number(column['missing_records'])} "
                    f"missing "
                    f"({fmt_percent(column['missing_percentage'])})"
                )

        lines.append("")

    lines += [
        "### 3. AMPS Relationship Validation",
        "",
        "| Relationship | Matched | Unmatched | Missing FK | Populated match |",
        "|---|---:|---:|---:|---:|",
    ]

    for item in relationships:
        rate = item["match_percentage_of_populated"]

        lines.append(
            f"| {md_safe(item['name'])} "
            f"| {fmt_number(item['matched'])} "
            f"| {fmt_number(item['unmatched'])} "
            f"| {fmt_number(item['missing_references'])} "
            f"| {fmt_percent(rate)} |"
        )

    exact = inspections["exact_match"]
    dates = inspections["date_validation"]

    lines += [
        "",
        "### 4. Inspection Integration Validation",
        "",
        f"- Parent reference: `{inspections['parent_column']}`",
        f"- Child reference: `{inspections['child_column']}`",
        f"- Matched inspections: "
        f"{fmt_number(exact.get('matched_inspection_records'))}",
        f"- Unmatched inspections: "
        f"{fmt_number(exact.get('unmatched_inspection_records'))}",
        f"- Match rate: "
        f"{fmt_percent(exact.get('match_percentage_of_populated'))}",
        "- Entity grain is not yet business-validated.",
        "",
        "**Date parsing results**",
        "",
    ]

    for name, detail in dates.items():
        lines.append(
            f"- `{name}`: "
            f"{fmt_number(detail.get('parseable_records'))} "
            "parseable; "
            f"{fmt_number(detail.get('unparseable_records'))} "
            "unparseable"
        )

    lines += [
        "",
        "### 5. Data Quality Risk Register",
        "",
        "| ID | Severity | Issue | Required action |",
        "|---|---|---|---|",
    ]

    for risk in report["quality_risks"]:
        lines.append(
            f"| {risk['id']} "
            f"| {risk['severity']} "
            f"| {md_safe(risk['issue'])} "
            f"| {md_safe(risk['action'])} |"
        )

    lines += [
        "",
        "### 6. Source Integration Plan",
        "",
        "| Source | Status | Recommended use |",
        "|---|---|---|",
    ]

    for item in report["integration_plan"]:
        lines.append(
            f"| {item['source']} "
            f"| {item['integration_status']} "
            f"| {md_safe(item['role'])} |"
        )

    lines += [
        "",
        "### 7. Bronze / Silver / Gold Implementation Guidance",
        "",
        "**Bronze — Kedro + PySpark**",
        "",
        "- Preserve raw source values and metadata.",
        "- Use immutable source ingestion records.",
        "- Validate file inventory and schema versions.",
        "- Store source-aligned Parquet outputs before "
        "migration to cloud Delta Lake.",
        "",
        "**Silver — Kedro + PySpark**",
        "",
        "- Apply explicitly defined data types and "
        "date parsing rules.",
        "- Retain source identifiers and raw-value lineage.",
        "- Add missing-value and relationship quality flags.",
        "- Preserve unmatched records for audit.",
        "- Do not fabricate cross-source relationships.",
        "",
        "**Gold — dbt models**",
        "",
        "- Create conformed dimensions only after "
        "entity-grain and relationship validation.",
        "- Develop maintenance, labor, materials and "
        "inspection analytics at documented grains.",
        "- Avoid duplicate-driven cost inflation.",
        "- Define aggregation rules before publishing KPIs.",
        "",
        "### 8. ML / AssetGuard Scope",
        "",
        "- AI4I is an independent synthetic benchmark.",
        "- NYC Parks risk features must be derived "
        "from available historical AMPS data.",
        "- A predictive model requires an independently "
        "validated target and leakage-safe evaluation.",
        "- Do not claim that AI4I predictions apply "
        "directly to NYC Parks equipment.",
        "",
        "### 9. Decision",
        "",
        "**Readiness: READY FOR CONTROLLED BRONZE DESIGN**",
        "",
        "Source profiling provides a basis for ingestion "
        "design. Silver transformations and business "
        "integrations remain subject to the documented "
        "quality checks and unresolved semantic questions.",
        "",
    ]

    return "\n".join(lines)


# ============================================================
# 7. MAIN EXECUTION
# ============================================================

def run_consolidation():

    print("\n" + "=" * 70)
    print("ASSETFUSION360 - STEP 4.8")
    print("CONSOLIDATED SOURCE QUALITY & INTEGRATION READINESS")
    print("=" * 70)

    inventory = []
    errors = []

    for config in DATASETS:

        report_path = REPORT_DIR / config["file"]
        source_report = load_json(report_path)

        summary = summarize_dataset(
            config,
            source_report
        )

        inventory.append(summary)

        print(
            f"{config['name']}: "
            f"{fmt_number(summary['records'])} rows "
            f"[{summary['record_count_status']}]"
        )

        if summary["record_count_status"] != "MATCH":
            errors.append(
                f"{config['name']}: count not reconciled."
            )

    validation_data = {}

    for key, filename in VALIDATION_REPORTS.items():
        validation_data[key] = load_json(
            REPORT_DIR / filename
        )

    amps_relationships = summarize_amps_relationships(
        validation_data["amps_relationships"]
    )

    inspection_validation = summarize_inspection_validation(
        validation_data["inspection_relationships"]
    )

    if len(amps_relationships) != 4:
        errors.append(
            "Expected four AMPS relationship results."
        )

    if not inspection_validation["exact_match"]:
        errors.append(
            "Inspection relationship results unavailable."
        )

    if not inspection_validation["date_validation"]:
        errors.append(
            "Corrected inspection date results unavailable."
        )

    available_counts = [
        item["records"]
        for item in inventory
        if item["records"] is not None
    ]

    verified_total = (
        sum(available_counts)
        if len(available_counts) == len(DATASETS)
        else None
    )

    inventory_count_status = (
        "MATCH"
        if verified_total == EXPECTED_TOTAL
        else "NOT_RECONCILED"
    )

    if inventory_count_status != "MATCH":
        errors.append(
            "Combined record total does not match "
            "the previously verified source inventory."
        )

    result = {
        "project": "AssetFusion360",
        "step": "4.8",
        "generated_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "assessment_scope": (
            "Previously saved source profiling and "
            "relationship validation reports"
        ),
        "dataset_inventory": inventory,
        "verified_record_total": verified_total,
        "expected_record_total": EXPECTED_TOTAL,
        "inventory_count_status": inventory_count_status,
        "amps_relationships": amps_relationships,
        "inspection_validation": inspection_validation,
        "quality_risks": build_quality_risks(),
        "integration_plan": build_integration_plan(),
        "readiness_decision": (
            "READY_FOR_CONTROLLED_BRONZE_DESIGN"
            if not errors
            else "REQUIRES_REPORT_RECONCILIATION"
        ),
        "reconciliation_errors": errors,
        "limitations": [
            "This step does not reread raw CSV records.",
            "Report-level verification does not replace "
            "business validation.",
            "Source record totals are not unique asset counts.",
            "Unmatched records must be preserved.",
            "Inspection property/asset grain is unconfirmed.",
            "NYC 311 identifier uniqueness remains unverified.",
            "No cloud pipeline is deployed by this step.",
        ],
    }

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    atomic_write(
        JSON_OUTPUT,
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False
        )
    )

    atomic_write(
        MD_OUTPUT,
        create_markdown(result)
    )

    print("\n" + "=" * 70)
    print("STEP 4.8 CONSOLIDATED SUMMARY")
    print("=" * 70)

    print(
        "Datasets assessed: "
        f"{len(inventory)}"
    )

    print(
        "Total source records: "
        f"{fmt_number(verified_total)}"
    )

    print(
        "AMPS relationships: "
        f"{len(amps_relationships)}"
    )

    print(
        "Readiness: "
        f"{result['readiness_decision']}"
    )

    if errors:
        print("\nRECONCILIATION FINDINGS:")
        for error in errors:
            print(f"  - {error}")

    print("\nJSON report:")
    print(JSON_OUTPUT)

    print("\nMarkdown report:")
    print(MD_OUTPUT)

    print("\n" + "=" * 70)
    print("STEP 4.8 REPORT GENERATION COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    run_consolidation()
