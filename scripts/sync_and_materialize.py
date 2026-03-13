"""
Syncs Parquet feature files from S3/MinIO → local feature_store/data/,
appends new data to the offline store, then runs feast materialize to
push the latest features into Redis (online store).

Uses S3 date partitions (year=/month=/day=/) to only download recent files.
Tracks last sync timestamp in a local state file to avoid re-downloading.

Usage:
    python scripts/sync_and_materialize.py                    # sync last 1 day
    python scripts/sync_and_materialize.py --lookback-days 3  # sync last 3 days
    python scripts/sync_and_materialize.py --apply            # also runs feast apply
    python scripts/sync_and_materialize.py --full             # ignore state, full re-sync

In production this would be an Airflow task on a schedule.
"""

import os
import json
import argparse
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_BUCKET = os.getenv("FEATURE_S3_BUCKET", "feature-store")
S3_PREFIX = os.getenv("FEATURE_S3_PREFIX", "ticker_features")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

FEATURE_STORE_DIR = Path(__file__).parent.parent / "feature_store"
DATA_DIR = FEATURE_STORE_DIR / "data"
STATE_FILE = DATA_DIR / ".sync_state.json"
STAGING_DIR = DATA_DIR / "staging"


def load_sync_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_sync_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_partition_prefixes(lookback_days: int) -> list[str]:
    """Generate S3 prefixes for each day in the lookback window."""
    now = datetime.now(timezone.utc)
    prefixes = []
    for delta in range(lookback_days):
        dt = now - timedelta(days=delta)
        prefix = (
            f"{S3_PREFIX}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        )
        prefixes.append(prefix)
    return prefixes


def download_parquet_files(
    s3_client,
    lookback_days: int,
    synced_keys: set[str],
) -> tuple[list[Path], set[str]]:
    """Download only new parquet files from recent date partitions."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    prefixes = get_partition_prefixes(lookback_days)
    downloaded = []
    new_keys = set()
    skipped = 0

    paginator = s3_client.get_paginator("list_objects_v2")

    for prefix in prefixes:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue

                if key in synced_keys:
                    skipped += 1
                    continue

                local_path = STAGING_DIR / key.replace("/", "_")
                s3_client.download_file(S3_BUCKET, key, str(local_path))
                downloaded.append(local_path)
                new_keys.add(key)

    print(
        f"Downloaded {len(downloaded)} new files, "
        f"skipped {skipped} already-synced "
        f"(scanned {len(prefixes)} day partitions)"
    )
    return downloaded, new_keys


def merge_parquet_files(new_files: list[Path]) -> Path:
    """Append new parquet files to the existing offline store file."""
    output_path = DATA_DIR / "ticker_features.parquet"

    existing_table = None
    if output_path.exists():
        try:
            existing_table = pq.read_table(output_path)
        except Exception as e:
            print(f"Existing parquet corrupt, rebuilding: {e}")

    new_tables = []
    for f in new_files:
        try:
            new_tables.append(pq.read_table(f))
        except Exception as e:
            print(f"Skipping corrupt file {f}: {e}")

    if not new_tables and existing_table is None:
        print("No data available")
        return output_path

    if not new_tables:
        print("No new data to merge")
        return output_path

    all_tables = [existing_table] + new_tables if existing_table else new_tables
    merged = pa.concat_tables(all_tables, promote_options="default")
    merged = merged.sort_by("event_timestamp")

    pq.write_table(merged, output_path, compression="snappy")

    new_row_count = sum(t.num_rows for t in new_tables)
    print(
        f"Appended {new_row_count} rows from {len(new_tables)} files "
        f"→ {output_path} (total: {merged.num_rows} rows)"
    )
    return output_path


def cleanup_staging():
    """Remove staging files after successful merge."""
    if STAGING_DIR.exists():
        for f in STAGING_DIR.glob("*.parquet"):
            f.unlink()


def run_feast_apply():
    print("Running feast apply...")
    result = subprocess.run(
        ["feast", "apply"],
        cwd=str(FEATURE_STORE_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"feast apply failed: {result.stderr}")
        raise RuntimeError("feast apply failed")


def run_feast_materialize():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Running feast materialize-incremental {now}...")

    result = subprocess.run(
        ["feast", "materialize-incremental", now],
        cwd=str(FEATURE_STORE_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"feast materialize failed: {result.stderr}")
        raise RuntimeError("feast materialize failed")
    print("Materialization complete — features are now in Redis")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Run feast apply before materializing")
    parser.add_argument("--lookback-days", type=int, default=1,
                        help="How many days of partitions to scan (default: 1)")
    parser.add_argument("--full", action="store_true",
                        help="Ignore sync state, re-download everything in lookback window")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="us-east-1",
    )

    state = load_sync_state()
    synced_keys = set(state.get("synced_keys", [])) if not args.full else set()

    new_files, new_keys = download_parquet_files(s3, args.lookback_days, synced_keys)

    if new_files:
        merge_parquet_files(new_files)
        cleanup_staging()

        synced_keys.update(new_keys)
        save_sync_state({
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "synced_keys": list(synced_keys),
        })
    else:
        print("No new files to process")

    if args.apply:
        run_feast_apply()

    run_feast_materialize()


if __name__ == "__main__":
    main()
