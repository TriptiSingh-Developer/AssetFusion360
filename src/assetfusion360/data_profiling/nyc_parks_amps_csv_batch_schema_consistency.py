
"""
AssetFusion360 Capstone Project

NYC Parks AMPS - CSV Batch Schema Consistency Inspection

STEP 4.6A

Purpose:
    1. Discover all source files.
    2. Identify individual datasets and batch groups.
    3. Inspect every CSV header.
    4. Compare schemas within each batch group.
    5. Detect missing, duplicate, or reordered columns.
    6. Identify empty files and unreadable headers.
    7. Save detailed JSON and CSV reports.

This script reads CSV headers only.
It does not load millions of data rows.

Original source files are never modified.
"""

import csv
import json

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# 1. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw_source_data"
    / "nyc_parks_asset_management"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "data_profiling_reports"
)

JSON_REPORT = (
    REPORT_DIR
    / "nyc_parks_amps_batch_schema_consistency_report.json"
)

CSV_REPORT = (
    REPORT_DIR
    / "nyc_parks_amps_batch_schema_inventory.csv"
)

EXPECTED_SOURCE_FILE_COUNT = 1710


# ============================================================
# 2. IDENTIFY DATASET GROUP
# ============================================================

def identify_dataset_group(file_path):
    """
    Batch files are grouped by their relative directory.

    Standalone CSV files in the source root receive
    individual group names so unrelated schemas
    are not compared against one another.
    """

    relative_path = file_path.relative_to(SOURCE_DIR)

    if relative_path.parent == Path("."):
        return f"standalone/{file_path.stem}"

    return relative_path.parent.as_posix()


# ============================================================
# 3. READ CSV HEADER SAFELY
# ============================================================

def inspect_csv_header(file_path):
    """
    Read the first CSV record as the header.

    utf-8-sig handles files with or without
    a UTF-8 byte-order mark.

    The header's exact column order is retained.
    """

    result = {
        "file_name": file_path.name,
        "relative_path": (
            file_path.relative_to(SOURCE_DIR).as_posix()
        ),
        "dataset_group": identify_dataset_group(file_path),
        "file_size_bytes": file_path.stat().st_size,
        "column_count": 0,
        "columns": [],
        "duplicate_column_names": [],
        "blank_column_positions": [],
        "read_error": None,
        "schema_status": "NOT_CHECKED",
    }

    if result["file_size_bytes"] == 0:
        result["read_error"] = "Empty file"
        result["schema_status"] = "EMPTY_FILE"
        return result

    try:
        with file_path.open(
            "r",
            encoding="utf-8-sig",
            errors="strict",
            newline=""
        ) as source:

            reader = csv.reader(source)

            header = next(reader, None)

            if not header:
                result["read_error"] = (
                    "Missing or empty CSV header"
                )
                result["schema_status"] = "EMPTY_HEADER"
                return result

            result["columns"] = header
            result["column_count"] = len(header)

            counts = Counter(header)

            result["duplicate_column_names"] = [
                name
                for name, count in counts.items()
                if count > 1
            ]

            result["blank_column_positions"] = [
                position
                for position, name in enumerate(
                    header, start=1
                )
                if not name.strip()
            ]

    except (
        UnicodeError,
        csv.Error,
        OSError
    ) as error:

        result["read_error"] = (
            f"{type(error).__name__}: {error}"
        )

        result["schema_status"] = "READ_ERROR"

    return result


# ============================================================
# 4. COMPARE COLUMN DEFINITIONS
# ============================================================

def compare_headers(actual, expected):
    """
    Compare one batch header against the most
    common header within its dataset group.
    """

    actual_set = set(actual)
    expected_set = set(expected)

    missing_columns = [
        name for name in expected
        if name not in actual_set
    ]

    additional_columns = [
        name for name in actual
        if name not in expected_set
    ]

    same_columns = (
        len(actual) == len(expected)
        and actual_set == expected_set
    )

    same_order = actual == expected

    return {
        "missing_columns": missing_columns,
        "additional_columns": additional_columns,
        "same_column_names": same_columns,
        "same_column_order": same_order,
    }


# ============================================================
# 5. INSPECT ALL NYC PARKS AMPS CSV FILES
# ============================================================

def inspect_all_source_files():

    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Source directory not found: {SOURCE_DIR}"
        )

    all_files = sorted(
        (
            path
            for path in SOURCE_DIR.rglob("*")
            if path.is_file()
        ),
        key=lambda path: str(path).lower()
    )

    csv_files = [
        path for path in all_files
        if path.suffix.lower() == ".csv"
    ]

    non_csv_files = [
        path.relative_to(SOURCE_DIR).as_posix()
        for path in all_files
        if path.suffix.lower() != ".csv"
    ]

    print("\n" + "=" * 65)
    print("NYC PARKS AMPS - SOURCE FILE INVENTORY")
    print("=" * 65)

    print(f"All source files: {len(all_files):,}")
    print(f"CSV files: {len(csv_files):,}")
    print(f"Non-CSV files: {len(non_csv_files):,}")

    if len(all_files) != EXPECTED_SOURCE_FILE_COUNT:
        print(
            "\nNOTICE: Source file count differs "
            "from the earlier inventory."
        )

    if not csv_files:
        raise ValueError(
            "No CSV files found in the source directory."
        )

    results = []

    for index, file_path in enumerate(
        csv_files, start=1
    ):

        result = inspect_csv_header(file_path)
        results.append(result)

        if (
            index % 100 == 0
            or index == len(csv_files)
        ):
            print(
                f"Inspected {index:,} / "
                f"{len(csv_files):,} CSV headers"
            )

    return (
        len(all_files),
        results,
        non_csv_files
    )


# ============================================================
# 6. ANALYZE SCHEMA CONSISTENCY BY DATASET GROUP
# ============================================================

def analyze_schema_consistency(results):

    grouped = defaultdict(list)

    for result in results:
        grouped[result["dataset_group"]].append(result)

    group_summaries = []

    for group_name, records in sorted(
        grouped.items()
    ):

        valid_records = [
            record
            for record in records
            if record["read_error"] is None
        ]

        schema_counts = Counter(
            tuple(record["columns"])
            for record in valid_records
        )

        if schema_counts:
            canonical_schema, canonical_frequency = (
                schema_counts.most_common(1)[0]
            )
        else:
            canonical_schema = ()
            canonical_frequency = 0

        canonical_columns = list(
            canonical_schema
        )

        for record in records:

            if record["read_error"]:
                continue

            comparison = compare_headers(
                record["columns"],
                canonical_columns
            )

            record.update(comparison)

            if record["duplicate_column_names"]:
                record["schema_status"] = (
                    "DUPLICATE_COLUMN_NAMES"
                )

            elif record["blank_column_positions"]:
                record["schema_status"] = (
                    "BLANK_COLUMN_NAMES"
                )

            elif comparison["same_column_order"]:
                record["schema_status"] = (
                    "CONSISTENT"
                )

            elif comparison["same_column_names"]:
                record["schema_status"] = (
                    "COLUMN_ORDER_MISMATCH"
                )

            else:
                record["schema_status"] = (
                    "COLUMN_MISMATCH"
                )

        status_distribution = Counter(
            record["schema_status"]
            for record in records
        )

        mismatched_files = [
            {
                "file": record["relative_path"],
                "status": record["schema_status"],
                "columns": record["columns"],
                "read_error": record["read_error"],
                "missing_columns": record.get(
                    "missing_columns", []
                ),
                "additional_columns": record.get(
                    "additional_columns", []
                ),
            }
            for record in records
            if record["schema_status"] != "CONSISTENT"
        ]

        group_summaries.append({
            "dataset_group": group_name,
            "total_csv_files": len(records),
            "readable_csv_headers": len(valid_records),
            "schema_variant_count": len(schema_counts),
            "canonical_schema_file_count": (
                canonical_frequency
            ),
            "canonical_column_count": (
                len(canonical_columns)
            ),
            "canonical_columns": canonical_columns,
            "schema_status_distribution": (
                dict(status_distribution)
            ),
            "all_headers_consistent": (
                len(valid_records) == len(records)
                and len(schema_counts) == 1
                and all(
                    record["schema_status"] == "CONSISTENT"
                    for record in records
                )
            ),
            "mismatched_files": mismatched_files,
        })

    return group_summaries


# ============================================================
# 7. SAVE JSON REPORT
# ============================================================

def save_json_report(
    total_source_files,
    results,
    non_csv_files,
    group_summaries
):

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    status_counts = Counter(
        record["schema_status"]
        for record in results
    )

    report = {
        "project": "AssetFusion360",
        "profiling_step": "4.6A",
        "dataset": "NYC Parks AMPS Source Collection",
        "generated_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "source_directory": str(
            SOURCE_DIR.relative_to(PROJECT_ROOT)
        ),
        "total_source_files": total_source_files,
        "total_csv_files": len(results),
        "total_non_csv_files": len(non_csv_files),
        "non_csv_file_paths": non_csv_files,
        "total_dataset_groups": len(group_summaries),
        "overall_schema_status": dict(status_counts),
        "dataset_group_summaries": group_summaries,
        "file_inventory": results,
    }

    with JSON_REPORT.open(
        "w",
        encoding="utf-8"
    ) as output:

        json.dump(
            report,
            output,
            indent=2,
            ensure_ascii=False
        )

    return report


# ============================================================
# 8. SAVE FLAT CSV INVENTORY REPORT
# ============================================================

def save_csv_report(results):

    fieldnames = [
        "dataset_group",
        "relative_path",
        "file_name",
        "file_size_bytes",
        "column_count",
        "schema_status",
        "duplicate_column_names",
        "blank_column_positions",
        "missing_columns",
        "additional_columns",
        "read_error",
    ]

    with CSV_REPORT.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as output:

        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for result in results:

            row = {}

            for field in fieldnames:

                value = result.get(field)

                if isinstance(value, list):
                    value = json.dumps(value)

                row[field] = value

            writer.writerow(row)


# ============================================================
# 9. PRINT FINAL DATASET GROUP SUMMARY
# ============================================================

def print_final_summary(report):

    print("\n" + "=" * 65)
    print("NYC PARKS AMPS - BATCH SCHEMA CONSISTENCY RESULTS")
    print("=" * 65)

    print(
        f"\nTotal source files: "
        f"{report['total_source_files']:,}"
    )

    print(
        f"Total CSV files: "
        f"{report['total_csv_files']:,}"
    )

    print(
        f"Dataset groups: "
        f"{report['total_dataset_groups']:,}"
    )

    for group in report["dataset_group_summaries"]:

        print("\n" + "-" * 60)

        print(
            f"DATASET GROUP: {group['dataset_group']}"
        )

        print(
            f"CSV files: {group['total_csv_files']:,}"
        )

        print(
            f"Column count: "
            f"{group['canonical_column_count']}"
        )

        print(
            f"Schema variants: "
            f"{group['schema_variant_count']}"
        )

        print(
            f"Consistent: "
            f"{group['all_headers_consistent']}"
        )

        print("Status counts:")

        for status, count in (
            group["schema_status_distribution"].items()
        ):
            print(f"  {status}: {count}")

        if group["mismatched_files"]:

            print("Files requiring review:")

            for file in (
                group["mismatched_files"][:10]
            ):
                print(
                    f"  {file['file']} "
                    f"[{file['status']}]"
                )

    print("\n" + "=" * 65)
    print("BATCH SCHEMA INSPECTION COMPLETED")
    print("=" * 65)

    print("\nJSON report:")
    print(JSON_REPORT)

    print("\nCSV inventory:")
    print(CSV_REPORT)


# ============================================================
# 10. MAIN EXECUTION
# ============================================================

def main():

    (
        total_source_files,
        results,
        non_csv_files
    ) = inspect_all_source_files()

    group_summaries = analyze_schema_consistency(
        results
    )

    report = save_json_report(
        total_source_files,
        results,
        non_csv_files,
        group_summaries
    )

    save_csv_report(results)

    print_final_summary(report)


if __name__ == "__main__":
    main()
