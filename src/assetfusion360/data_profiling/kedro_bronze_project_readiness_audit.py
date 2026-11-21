
"""
AssetFusion360 Capstone Project

STEP 5.1 - Existing Kedro / Bronze Project Readiness Audit

Purpose:
  1. Inspect existing Kedro project configuration.
  2. Identify package and framework versions.
  3. Discover existing pipeline modules.
  4. Inspect catalog configuration safely.
  5. Inventory existing Bronze artifacts.
  6. Verify the AMPS Assets source.
  7. Verify previous profiling reports.
  8. Save a JSON audit report.

Read-only except for the new report JSON.
No raw data, pipeline or catalog files are modified.
"""

import importlib.metadata
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import tomllib
except ImportError:
    tomllib = None


# ============================================================
# 1. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_ROOT = PROJECT_ROOT / "data" / "01_raw_source_data"

ASSETS_FILE = (
    SOURCE_ROOT
    / "nyc_parks_asset_management"
    / "AMPS_Assets.csv"
)

SRC_DIR = PROJECT_ROOT / "src"

PACKAGE_DIR = SRC_DIR / "assetfusion360"

PIPELINES_DIR = PACKAGE_DIR / "pipelines"

CONFIG_DIR = PROJECT_ROOT / "conf"

BRONZE_DIR = (
    PROJECT_ROOT / "data" / "02_bronze_ingested_data"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

REPORT_FILE = (
    REPORT_DIR / "kedro_bronze_project_readiness_audit.json"
)

REQUIRED_REPORTS = [
    "assetfusion360_consolidated_source_quality_report.json",
    "nyc_parks_amps_assets_profile.json",
    "nyc_parks_amps_cross_dataset_relationship_report.json",
    "nyc_parks_inspection_date_and_asset_relationship_report.json",
]


# ============================================================
# 2. UTILITIES
# ============================================================

def relative_path(path):
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def version_of(package):
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None


def get_file_info(path):
    return {
        "path": relative_path(path),
        "exists": path.is_file(),
        "size_bytes": (
            path.stat().st_size if path.is_file() else None
        ),
    }


def save_report(data):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    temporary = REPORT_FILE.with_suffix(".json.tmp")

    temporary.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    os.replace(temporary, REPORT_FILE)


# ============================================================
# 3. READ PYPROJECT CONFIGURATION
# ============================================================

def inspect_pyproject():
    path = PROJECT_ROOT / "pyproject.toml"

    result = {
        "exists": path.is_file(),
        "project_name": None,
        "project_version": None,
        "kedro_package_name": None,
        "kedro_source_dir": None,
        "configuration_parse_error": None,
    }

    if not path.is_file():
        return result

    if tomllib is None:
        result["configuration_parse_error"] = (
            "tomllib unavailable"
        )
        return result

    try:
        with path.open("rb") as handle:
            config = tomllib.load(handle)

        project = config.get("project", {})
        kedro_config = (
            config.get("tool", {}).get("kedro", {})
        )

        result["project_name"] = project.get("name")
        result["project_version"] = project.get("version")
        result["kedro_package_name"] = kedro_config.get(
            "package_name"
        )
        result["kedro_source_dir"] = kedro_config.get(
            "source_dir"
        )

    except Exception as exc:
        result["configuration_parse_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    return result


# ============================================================
# 4. INSPECT KEDRO CONFIGURATION FILES
# ============================================================

def inspect_configuration():

    result = {
        "conf_directory_exists": CONFIG_DIR.is_dir(),
        "environments": [],
        "catalog_files": [],
        "parameter_files": [],
        "other_configuration_files": [],
    }

    if not CONFIG_DIR.is_dir():
        return result

    for folder in sorted(CONFIG_DIR.iterdir()):
        if folder.is_dir():
            result["environments"].append(folder.name)

    for path in sorted(CONFIG_DIR.rglob("*")):
        if not path.is_file():
            continue

        # Do not read or expose configuration values,
        # credentials, connection strings or secrets.
        name = path.name.lower()
        info = get_file_info(path)

        if (
            name.startswith("catalog")
            and path.suffix.lower() in {".yml", ".yaml"}
        ):
            result["catalog_files"].append(info)

        elif (
            name.startswith("parameters")
            and path.suffix.lower() in {".yml", ".yaml"}
        ):
            result["parameter_files"].append(info)

        else:
            result["other_configuration_files"].append(
                relative_path(path)
            )

    return result


# ============================================================
# 5. INSPECT EXISTING PIPELINES
# ============================================================

def inspect_pipelines():

    registry_file = PACKAGE_DIR / "pipeline_registry.py"

    result = {
        "package_directory_exists": PACKAGE_DIR.is_dir(),
        "pipeline_registry": get_file_info(registry_file),
        "pipeline_directories": [],
        "pipeline_python_modules": [],
        "candidate_bronze_modules": [],
    }

    if not PIPELINES_DIR.is_dir():
        return result

    for folder in sorted(PIPELINES_DIR.iterdir()):
        if folder.is_dir():
            result["pipeline_directories"].append(
                relative_path(folder)
            )

    for path in sorted(PIPELINES_DIR.rglob("*.py")):
        name = relative_path(path)
        result["pipeline_python_modules"].append(name)

        if any(
            word in name.lower()
            for word in [
                "bronze",
                "ingest",
                "assets",
                "raw",
            ]
        ):
            result["candidate_bronze_modules"].append(
                name
            )

    return result


# ============================================================
# 6. INVENTORY BRONZE DATA ARTIFACTS
# ============================================================

def inspect_bronze():

    result = {
        "bronze_directory_exists": BRONZE_DIR.is_dir(),
        "top_level_items": [],
        "file_count": 0,
        "total_file_bytes": 0,
        "file_extensions": {},
        "asset_related_artifacts": [],
        "sample_paths": [],
    }

    if not BRONZE_DIR.is_dir():
        return result

    result["top_level_items"] = [
        item.name
        for item in sorted(BRONZE_DIR.iterdir())
    ]

    for path in BRONZE_DIR.rglob("*"):
        if not path.is_file():
            continue

        result["file_count"] += 1
        result["total_file_bytes"] += path.stat().st_size

        extension = path.suffix.lower() or "[no extension]"

        result["file_extensions"][extension] = (
            result["file_extensions"].get(extension, 0) + 1
        )

        name = relative_path(path)

        if "asset" in name.lower():
            if len(result["asset_related_artifacts"]) < 30:
                result["asset_related_artifacts"].append(name)

        if len(result["sample_paths"]) < 30:
            result["sample_paths"].append(name)

    return result


# ============================================================
# 7. INSPECT EXISTING DATA DIRECTORY STRUCTURE
# ============================================================

def inspect_data_directories():

    data_dir = PROJECT_ROOT / "data"

    if not data_dir.is_dir():
        return []

    return [
        {
            "directory": relative_path(path),
            "exists": True,
        }
        for path in sorted(data_dir.iterdir())
        if path.is_dir()
    ]


# ============================================================
# 8. VERIFY PREVIOUS QUALITY REPORTS
# ============================================================

def inspect_reports():

    result = []

    for name in REQUIRED_REPORTS:
        path = REPORT_DIR / name

        info = get_file_info(path)
        info["json_readable"] = False
        info["status"] = None

        if path.is_file():
            try:
                with path.open(
                    "r", encoding="utf-8"
                ) as handle:
                    data = json.load(handle)

                info["json_readable"] = True

                if isinstance(data, dict):
                    info["status"] = (
                        data.get("readiness_decision")
                        or data.get("status")
                        or data.get("validation_status")
                    )

            except (ValueError, OSError) as exc:
                info["error"] = str(exc)

        result.append(info)

    return result


# ============================================================
# 9. GENERATE AUDIT
# ============================================================

def run_audit():

    print("\n" + "=" * 68)
    print("ASSETFUSION360 - STEP 5.1")
    print("KEDRO / BRONZE PROJECT READINESS AUDIT")
    print("=" * 68)

    versions = {
        "python": sys.version.split()[0],
        "kedro": version_of("kedro"),
        "pyspark": version_of("pyspark"),
        "kedro_datasets": version_of("kedro-datasets"),
        "pyyaml": version_of("PyYAML"),
    }

    pyproject = inspect_pyproject()
    config = inspect_configuration()
    pipelines = inspect_pipelines()
    bronze = inspect_bronze()
    reports = inspect_reports()

    assets = get_file_info(ASSETS_FILE)

    warnings = []

    if versions["kedro"] is None:
        warnings.append("Kedro is not installed.")

    if versions["pyspark"] is None:
        warnings.append("PySpark is not installed.")

    if not assets["exists"]:
        warnings.append(
            "AMPS_Assets.csv is missing from its expected path."
        )

    if not config["conf_directory_exists"]:
        warnings.append(
            "Kedro conf directory not found."
        )

    if not pipelines["package_directory_exists"]:
        warnings.append(
            "assetfusion360 Python package directory not found."
        )

    for report in reports:
        if not report["json_readable"]:
            warnings.append(
                f"Quality report missing or unreadable: "
                f"{report['path']}"
            )

    # Existing Bronze artifacts are not an error.
    # They signal that a new ingestion pipeline
    # must avoid overwriting established outputs.

    readiness = (
        "READY_FOR_PIPELINE_DESIGN"
        if not warnings
        else "REQUIRES_CONFIGURATION_REVIEW"
    )

    audit = {
        "project": "AssetFusion360",
        "step": "5.1",
        "generated_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "project_root": str(PROJECT_ROOT),
        "installed_versions": versions,
        "pyproject": pyproject,
        "kedro_configuration": config,
        "existing_pipelines": pipelines,
        "existing_bronze_artifacts": bronze,
        "existing_data_directories": (
            inspect_data_directories()
        ),
        "amps_assets_source": assets,
        "existing_quality_reports": reports,
        "readiness": readiness,
        "warnings": warnings,
        "notes": [
            "No existing pipeline files were edited.",
            "No Kedro configuration values were disclosed.",
            "Raw CSV records were not reprocessed.",
            "No Bronze datasets were overwritten.",
            "This audit does not execute Kedro pipelines.",
            "Existing Bronze outputs require review "
            "before choosing new output paths.",
        ],
    }

    save_report(audit)

    print("\nINSTALLED FRAMEWORKS")
    print("-" * 50)

    for name, version in versions.items():
        print(f"{name}: {version or 'Not detected'}")

    print("\nPROJECT CONFIGURATION")
    print("-" * 50)

    print(
        "Kedro package name: "
        f"{pyproject['kedro_package_name']}"
    )

    print(
        "Configuration environments: "
        f"{config['environments']}"
    )

    print(
        "Catalog files: "
        f"{len(config['catalog_files'])}"
    )

    print(
        "Pipeline registry exists: "
        f"{pipelines['pipeline_registry']['exists']}"
    )

    print(
        "Pipeline Python modules: "
        f"{len(pipelines['pipeline_python_modules'])}"
    )

    print("\nEXISTING BRONZE ARTIFACTS")
    print("-" * 50)

    print(
        "Bronze directory exists: "
        f"{bronze['bronze_directory_exists']}"
    )

    print(
        f"Bronze files discovered: {bronze['file_count']:,}"
    )

    print(
        f"Total Bronze file bytes: "
        f"{bronze['total_file_bytes']:,}"
    )

    print("Top-level Bronze items:")
    for item in bronze["top_level_items"]:
        print(f"  - {item}")

    print("\nCANDIDATE EXISTING BRONZE MODULES")
    for name in pipelines["candidate_bronze_modules"]:
        print(f"  - {name}")

    print("\nSOURCE VALIDATION")
    print("-" * 50)

    print(
        f"AMPS Assets source exists: {assets['exists']}"
    )

    print(
        "Required quality reports readable: "
        f"{sum(x['json_readable'] for x in reports)}"
        f"/{len(reports)}"
    )

    print("\n" + "=" * 68)
    print("STEP 5.1 AUDIT SUMMARY")
    print("=" * 68)

    print(f"Readiness: {readiness}")

    if warnings:
        print("\nReview findings:")
        for warning in warnings:
            print(f"  - {warning}")

    print("\nAudit report saved to:")
    print(REPORT_FILE)

    print("\nSTEP 5.1 AUDIT COMPLETED")


if __name__ == "__main__":
    run_audit()
