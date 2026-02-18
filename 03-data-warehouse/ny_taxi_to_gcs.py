"""
NYC Taxi Data → Google Cloud Storage Uploader
=============================================
Flexibly download any combination of taxi type, year, month,
and file format, then upload to a GCS bucket.

Supported taxi types : green, yellow, fhv, fhvhv
Supported file formats: parquet, csv.gz
Supported years       : 2019–2024 (extend VALID_YEARS as needed)
"""

import os
import sys
import time
import argparse
import urllib.request
from itertools import product
from concurrent.futures import ThreadPoolExecutor

from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden


# ─────────────────────────────────────────────
# DATA SOURCE CONFIGURATION
# Each taxi type has two possible sources:
#   "tlc"  → official TLC CloudFront CDN  (parquet only, up-to-date)
#   "dtc"  → DataTalksClub GitHub mirror  (csv.gz only, frozen snapshots
#             used in the DE Zoomcamp — use this when you need to match
#             course exercise answers exactly)
# ─────────────────────────────────────────────
SOURCES = {
    "tlc": {
        "green":  "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet",
        "yellow": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet",
        "fhv":    "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet",
        "fhvhv":  "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet",
    },
    "dtc": {
        "green":  "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz",
        "yellow": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month}.csv.gz",
        "fhv":    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz",
        "fhvhv":  "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_{year}-{month}.csv.gz",
    },
}

VALID_YEARS      = list(range(2019, 2025))
VALID_TAXI_TYPES = ["green", "yellow", "fhv", "fhvhv"]
VALID_SOURCES    = ["tlc", "dtc"]

CHUNK_SIZE  = 8 * 1024 * 1024  # 8 MB for resumable GCS uploads
MAX_RETRIES = 3


# ─────────────────────────────────────────────
# GCS CLIENT
# ─────────────────────────────────────────────
def build_client(credentials_file: str | None) -> storage.Client:
    if credentials_file:
        return storage.Client.from_service_account_json(credentials_file)
    # Falls back to Application Default Credentials (gcloud auth)
    return storage.Client()


# ─────────────────────────────────────────────
# BUCKET HELPERS
# ─────────────────────────────────────────────
def ensure_bucket(client: storage.Client, bucket_name: str) -> storage.Bucket:
    """Return existing bucket or create a new one. Exits on unrecoverable errors."""
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' found.")
        return bucket
    except NotFound:
        print(f"⚠️  Bucket '{bucket_name}' not found. Creating...")
        bucket = client.create_bucket(bucket_name)
        print(f"✅ Created bucket '{bucket_name}'.")
        return bucket
    except Forbidden:
        print(
            f"❌ Bucket '{bucket_name}' exists but is inaccessible "
            f"(likely owned by another project). Choose a different name."
        )
        sys.exit(1)


# ─────────────────────────────────────────────
# DOWNLOAD
# ─────────────────────────────────────────────
def build_url(taxi_type: str, year: int, month: str, source: str) -> str:
    template = SOURCES[source][taxi_type]
    return template.format(year=year, month=month)


def build_filename(taxi_type: str, year: int, month: str, source: str) -> str:
    ext = "csv.gz" if source == "dtc" else "parquet"
    return f"{taxi_type}_tripdata_{year}-{month}.{ext}"


def download_file(
    taxi_type: str,
    year: int,
    month: str,
    source: str,
    download_dir: str,
) -> str | None:
    url      = build_url(taxi_type, year, month, source)
    filename = build_filename(taxi_type, year, month, source)
    filepath = os.path.join(download_dir, filename)

    if os.path.exists(filepath):
        print(f"⏭️  Already exists, skipping download: {filename}")
        return filepath

    print(f"⬇️  Downloading {url} ...")
    try:
        urllib.request.urlretrieve(url, filepath)
        size_mb = os.path.getsize(filepath) / 1024 / 1024
        print(f"✅ Downloaded: {filename}  ({size_mb:.1f} MB)")
        return filepath
    except Exception as exc:
        print(f"❌ Failed to download {url}: {exc}")
        # Remove partial file if it exists
        if os.path.exists(filepath):
            os.remove(filepath)
        return None


# ─────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────
def upload_to_gcs(
    filepath: str,
    bucket: storage.Bucket,
    gcs_prefix: str,
    overwrite: bool,
) -> bool:
    filename  = os.path.basename(filepath)
    blob_name = f"{gcs_prefix}/{filename}" if gcs_prefix else filename
    blob      = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    if not overwrite and blob.exists():
        print(f"⏭️  Already in GCS, skipping upload: {blob_name}")
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"⬆️  Uploading {filename} → gs://{bucket.name}/{blob_name}  (attempt {attempt})")
            blob.upload_from_filename(filepath)

            if blob.exists():
                size_mb = blob.size / 1024 / 1024
                print(f"✅ Uploaded: gs://{bucket.name}/{blob_name}  ({size_mb:.1f} MB)")
                return True
            else:
                print(f"⚠️  Verification failed for {blob_name}, retrying...")
        except Exception as exc:
            print(f"❌ Upload error (attempt {attempt}): {exc}")
        time.sleep(5 * attempt)   # exponential-ish back-off

    print(f"❌ Gave up on {filename} after {MAX_RETRIES} attempts.")
    return False


# ─────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────
def parse_int_list(raw: str, name: str, valid: list[int]) -> list[int]:
    """Parse comma-separated integers and validate against allowed values."""
    try:
        values = [int(v.strip()) for v in raw.split(",")]
    except ValueError:
        print(f"❌ '{raw}' is not a valid list of integers for --{name}.")
        sys.exit(1)
    bad = [v for v in values if v not in valid]
    if bad:
        print(f"❌ Invalid {name} value(s): {bad}. Valid: {valid}")
        sys.exit(1)
    return values


def parse_month_list(raw: str) -> list[str]:
    """Parse comma-separated months (1-12 or 'all') into zero-padded strings."""
    if raw.lower() == "all":
        return [f"{m:02d}" for m in range(1, 13)]
    try:
        months = [int(v.strip()) for v in raw.split(",")]
    except ValueError:
        print(f"❌ '{raw}' is not a valid month list.")
        sys.exit(1)
    bad = [m for m in months if not (1 <= m <= 12)]
    if bad:
        print(f"❌ Invalid month value(s): {bad}. Months must be 1–12.")
        sys.exit(1)
    return [f"{m:02d}" for m in months]


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ny_taxi_to_gcs",
        description="Download NYC taxi Parquet/CSV.GZ files and upload them to GCS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
# Green & yellow, 2019 and 2020, all months, official TLC Parquet → my-bucket
  python ny_taxi_to_gcs.py \\
      --taxi-types green,yellow \\
      --years 2019,2020 \\
      --months all \\
      --source tlc \\
      --bucket my-bucket

# FHV 2019 Jan–Mar, DataTalksClub CSV.GZ mirror (matches Zoomcamp answers)
  python ny_taxi_to_gcs.py \\
      --taxi-types fhv \\
      --years 2019 \\
      --months 1,2,3 \\
      --source dtc \\
      --bucket my-bucket \\
      --gcs-prefix raw/fhv

# Use a service-account key file instead of Application Default Credentials
  python ny_taxi_to_gcs.py ... --credentials gcs.json
""",
    )

    p.add_argument(
        "--bucket", required=True,
        help="GCS bucket name (created automatically if it does not exist).",
    )
    p.add_argument(
        "--taxi-types", default="green",
        help=f"Comma-separated taxi types. Options: {', '.join(VALID_TAXI_TYPES)}. Default: green",
    )
    p.add_argument(
        "--years", default=str(VALID_YEARS[-1]),
        help=f"Comma-separated years. Options: {VALID_YEARS[0]}–{VALID_YEARS[-1]}. Default: {VALID_YEARS[-1]}",
    )
    p.add_argument(
        "--months", default="all",
        help="Comma-separated months (1–12) or 'all'. Default: all",
    )
    p.add_argument(
        "--source", default="tlc", choices=VALID_SOURCES,
        help=(
            "Data source. 'tlc' = official TLC CDN (Parquet, up-to-date); "
            "'dtc' = DataTalksClub GitHub mirror (CSV.GZ, frozen for Zoomcamp). "
            "Default: tlc"
        ),
    )
    p.add_argument(
        "--download-dir", default="./data",
        help="Local directory for downloaded files. Default: ./data",
    )
    p.add_argument(
        "--gcs-prefix", default="",
        help="Optional folder prefix inside the bucket, e.g. 'raw/green'. Default: bucket root",
    )
    p.add_argument(
        "--workers", type=int, default=4,
        help="Number of parallel download/upload workers. Default: 4",
    )
    p.add_argument(
        "--credentials", default=None,
        help="Path to a GCP service-account JSON key. Omit to use Application Default Credentials.",
    )
    p.add_argument(
        "--skip-download", action="store_true",
        help="Skip downloading; upload only files already present in --download-dir.",
    )
    p.add_argument(
        "--skip-upload", action="store_true",
        help="Skip GCS upload; only download files locally.",
    )
    p.add_argument(
        "--no-overwrite", action="store_true",
        help="Do not re-upload files that already exist in GCS.",
    )
    p.add_argument(
        "--keep-local", action="store_true",
        help="Keep local files after uploading. By default they are deleted to save space.",
    )
    return p


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main() -> None:
    parser = build_arg_parser()
    args   = parser.parse_args()

    # ── Validate and normalise inputs ──────────────────────────────────────
    taxi_types = [t.strip() for t in args.taxi_types.split(",")]
    bad_types  = [t for t in taxi_types if t not in VALID_TAXI_TYPES]
    if bad_types:
        print(f"❌ Invalid taxi type(s): {bad_types}. Valid: {VALID_TAXI_TYPES}")
        sys.exit(1)

    years  = parse_int_list(args.years, "years", VALID_YEARS)
    months = parse_month_list(args.months)

    # The DTC mirror only has CSV.GZ; warn the user if they select tlc
    # (tlc only has parquet — that's already enforced by the URL templates)

    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  NYC Taxi → GCS  |  Configuration Summary")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"  Taxi types  : {taxi_types}")
    print(f"  Years       : {years}")
    print(f"  Months      : {months}")
    print(f"  Source      : {args.source}  ({'parquet' if args.source == 'tlc' else 'csv.gz'})")
    print(f"  Bucket      : {args.bucket}")
    print(f"  GCS prefix  : '{args.gcs_prefix}' (empty = bucket root)")
    print(f"  Download dir: {args.download_dir}")
    print(f"  Workers     : {args.workers}")
    print(f"  Overwrite   : {not args.no_overwrite}")
    print(f"  Keep local  : {args.keep_local}")
    total = len(taxi_types) * len(years) * len(months)
    print(f"  Total files : {total}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

    os.makedirs(args.download_dir, exist_ok=True)

    # ── Build all (taxi_type, year, month) combos ──────────────────────────
    tasks = list(product(taxi_types, years, months))

    # ── Set up GCS (unless upload is skipped) ─────────────────────────────
    bucket = None
    if not args.skip_upload:
        client = build_client(args.credentials)
        bucket = ensure_bucket(client, args.bucket)

    # ── PHASE 1: Download ──────────────────────────────────────────────────
    local_files: list[str] = []

    if args.skip_download:
        print("⏭️  --skip-download set; collecting already-downloaded files...\n")
        for taxi_type, year, month in tasks:
            filename = build_filename(taxi_type, year, month, args.source)
            filepath = os.path.join(args.download_dir, filename)
            if os.path.exists(filepath):
                local_files.append(filepath)
            else:
                print(f"⚠️  File not found locally, will be skipped: {filename}")
    else:
        print(f"⬇️  Downloading {total} file(s) with {args.workers} workers...\n")

        def _download(task):
            taxi_type, year, month = task
            return download_file(taxi_type, year, month, args.source, args.download_dir)

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            results = list(ex.map(_download, tasks))

        local_files = [f for f in results if f is not None]
        failed_dl   = total - len(local_files)
        print(f"\n✅ Download phase: {len(local_files)} succeeded, {failed_dl} failed.\n")

    # ── PHASE 2: Upload ────────────────────────────────────────────────────
    if args.skip_upload:
        print("⏭️  --skip-upload set; skipping GCS upload.")
    elif not local_files:
        print("⚠️  No local files to upload.")
    else:
        print(f"⬆️  Uploading {len(local_files)} file(s) with {args.workers} workers...\n")
        overwrite = not args.no_overwrite

        def _upload(fp):
            success = upload_to_gcs(fp, bucket, args.gcs_prefix, overwrite)
            if success and not args.keep_local:
                os.remove(fp)
                print(f"🗑️  Deleted local file: {os.path.basename(fp)}")
            return success

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            upload_results = list(ex.map(_upload, local_files))

        failed_ul = upload_results.count(False)
        print(f"\n✅ Upload phase: {upload_results.count(True)} succeeded, {failed_ul} failed.")

    print("\n🚀 Done.\n")


if __name__ == "__main__":
    main()