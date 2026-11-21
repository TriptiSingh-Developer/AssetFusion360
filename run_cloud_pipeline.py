
# ============================================================
# STEP 9.4A — ASSETFUSION360 AUTOMATED DATA UPLOAD
# Discover Bronze and Silver datasets and upload files to Databricks.
# Supports dry runs, batch uploads and skipping existing remote files.
# Local source data and Gold tables are never modified.
# ============================================================

import argparse
import getpass
import os
from pathlib import Path

# Step 1: Define Databricks destination and local source folders
BASE = "/Volumes/workspace/assetfusion360/source_files"

SOURCES = {
    "gold": "data/04_gold_analytics",
    "bronze": "data/02_bronze_ingested_data",
    "silver": "data/03_silver_cleaned_data",
}

EXTENSIONS = {".parquet", ".csv"}


# Step 2: Automatically discover source files
def inventory(root):
    found = []

    for layer, relative in SOURCES.items():
        folder = root / relative

        if not folder.is_dir():
            print(f"[MISSING] {layer}: {relative} (skipped)")
            continue

        layer_files = sorted(
            (
                p for p in folder.rglob("*")
                if p.is_file()
                and p.suffix.lower() in EXTENSIONS
            ),
            key=lambda p: str(p).lower(),
        )

        size = sum(p.stat().st_size for p in layer_files)

        print(
            f"{layer.upper():6s} "
            f"{len(layer_files):5d} files | "
            f"{size / 1024**2:,.1f} MiB"
        )

        found.extend(
            (layer, folder, p)
            for p in layer_files
        )

    return found


# Step 3: Configure the automated execution
def main():

    parser = argparse.ArgumentParser(
        description="AssetFusion360 automated file upload"
    )

    parser.add_argument(
        "--upload",
        action="store_true",
        help="Connect to Databricks and upload files"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Upload all discovered files"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum files per upload run"
    )

    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be >= 1")

    if args.all and not args.upload:
        parser.error("--all requires --upload")

    root = Path(__file__).resolve().parent

    print("=" * 65)
    print("ASSETFUSION360 — AUTOMATED BRONZE/SILVER UPLOAD")
    print("=" * 65)

    # Step 4: Discover Bronze and Silver data
    items = inventory(root)

    print(f"\nTotal discovered: {len(items):,} files")

    if not items:
        raise SystemExit(
            "No source files found. Check project directories."
        )

    # Step 5: Dry-run mode — no authentication required
    if not args.upload:

        print("\nDRY RUN: No files will be uploaded or modified")

        for layer, folder, path in items[:5]:
            print(
                f"  {layer}/"
                f"{path.relative_to(folder).as_posix()}"
            )

        print("\nSTEP 9.4 UPLOAD PLAN: PASS")
        return

    # Step 6: Read Databricks connection settings
    host = os.environ.get(
        "DATABRICKS_HOST", ""
    ).strip().rstrip("/")

    if not host.startswith("https://"):
        raise SystemExit(
            "Set DATABRICKS_HOST before uploading"
        )

    token = os.environ.get(
        "DATABRICKS_TOKEN"
    ) or getpass.getpass(
        "Databricks access token (hidden): "
    )

    if not token:
        raise SystemExit(
            "No token supplied. Upload cancelled."
        )

    # Step 7: Connect securely to Databricks
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(
        host=host,
        token=token,
        auth_type="pat"
    )

    client.current_user.me()

    # Step 8: Select files for batch upload
    selected = items if args.all else items[:args.limit]

    existing = {}
    uploaded = 0
    skipped = 0

    # Step 9: Upload files while preserving existing files
    for layer, folder, source in selected:

        relative = source.relative_to(folder).as_posix()
        remote = f"{BASE}/{layer}/{relative}"
        remote_dir = remote.rsplit("/", 1)[0]

        if remote_dir not in existing:

            client.files.create_directory(remote_dir)

            existing[remote_dir] = {
                entry.name
                for entry in client.files.list_directory_contents(
                    remote_dir
                )
            }

        if source.name in existing[remote_dir]:

            skipped += 1
            print(f"SKIP: {layer}/{relative}")
            continue

        client.files.upload_from(
            remote,
            str(source),
            overwrite=False,
            use_parallel=False
        )

        existing[remote_dir].add(source.name)
        uploaded += 1

        print(f"UPLOADED: {layer}/{relative}")

    # Step 10: Print execution summary
    print("\n" + "=" * 65)
    print("AUTOMATED UPLOAD SUMMARY")
    print("=" * 65)

    print("Selected files:", len(selected))
    print("Uploaded:", uploaded)
    print("Already existing:", skipped)

    print("\nSTEP 9.4 AUTOMATED UPLOAD: PASS")
    print("LOCAL SOURCE DATA UNCHANGED")


if __name__ == "__main__":
    main()
