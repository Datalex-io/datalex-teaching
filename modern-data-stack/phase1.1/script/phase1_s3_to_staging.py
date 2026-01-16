from __future__ import annotations

import os
import json
import uuid
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Tuple

import boto3
import psycopg2


# -----------------------------
# Config
# -----------------------------
@dataclass(frozen=True)
class Config:
    s3_bucket: str
    s3_prefix: str  # e.g. "saas-exports"
    batch_date: str  # "YYYY-MM-DD"
    pg_dsn: str

    # Optional: AWS auth profile (ignored if using MinIO)
    aws_profile: str | None = None

    # Optional (for MinIO / custom S3)
    s3_endpoint_url: str | None = None
    aws_region: str | None = None

    # Optional: MinIO typically needs path-style addressing
    s3_force_path_style: bool = False

    # Optional: verbose prints
    verbose: bool = True


# Maps S3 file names to staging tables + load type
# FULL => truncate then load
# APPEND => insert only
FILE_MAP: Dict[str, Tuple[str, str]] = {
    "full/tenants.csv": ("staging.tenant", "FULL"),
    "full/users.csv": ("staging.user", "FULL"),
    "full/features.csv": ("staging.feature", "FULL"),
    "incremental/feature_usage.csv": ("staging.feature_usage", "APPEND"),
}


def log(cfg: Config, msg: str):
    if cfg.verbose:
        print(msg, flush=True)


def build_s3_client(cfg: Config):
    """
    Supports:
    - AWS: default credentials OR a named profile (AWS_PROFILE / aws_profile)
    - MinIO: set S3_ENDPOINT_URL + credentials via env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
    """
    session_kwargs = {}
    if cfg.aws_profile:
        session_kwargs["profile_name"] = cfg.aws_profile
    if cfg.aws_region:
        session_kwargs["region_name"] = cfg.aws_region

    session = boto3.session.Session(**session_kwargs)

    client_kwargs = {}
    if cfg.s3_endpoint_url:
        client_kwargs["endpoint_url"] = cfg.s3_endpoint_url

    # MinIO often requires path-style addressing
    if cfg.s3_force_path_style:
        from botocore.config import Config as BotoConfig

        client_kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})

    return session.client("s3", **client_kwargs)


def incoming_prefix(cfg: Config) -> str:
    return f"{cfg.s3_prefix}/incoming/batch_date={cfg.batch_date}/"


def archive_prefix(cfg: Config, run_id: str) -> str:
    return f"{cfg.s3_prefix}/archive/batch_date={cfg.batch_date}/run_id={run_id}/"


def manifest_key(cfg: Config, run_id: str) -> str:
    return f"{cfg.s3_prefix}/manifests/batch_date={cfg.batch_date}/run_id={run_id}.json"


def list_csv_keys(s3, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".csv"):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return sorted(keys)


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def count_csv_rows(data: bytes) -> int:
    # Approx: header + one row per line (OK for typical CSV without embedded newlines)
    lines = data.splitlines()
    return max(0, len(lines) - 1)


def ensure_staging_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()


def copy_csv_bytes_to_postgres(conn, table: str, csv_bytes: bytes, truncate: bool):
    """
    Loads CSV bytes into Postgres using COPY for speed.
    """
    import io

    with conn.cursor() as cur:
        if truncate:
            cur.execute(f"TRUNCATE {table};")
        bio = io.BytesIO(csv_bytes)
        cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", bio)
    conn.commit()


def move_s3_object(s3, bucket: str, src_key: str, dst_key: str):
    # Copy then delete = "move" in S3
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dst_key)
    s3.delete_object(Bucket=bucket, Key=src_key)


def main():
    cfg = Config(
        s3_bucket=os.environ["S3_BUCKET"],
        s3_prefix=os.environ.get("S3_PREFIX", "saas-exports"),
        batch_date=os.environ["BATCH_DATE"],  # "YYYY-MM-DD"
        pg_dsn=os.environ["PG_DSN"],
        aws_profile=os.environ.get("AWS_PROFILE") or None,
        s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_region=os.environ.get("AWS_REGION") or None,
        s3_force_path_style=os.environ.get("S3_FORCE_PATH_STYLE", "false").lower() == "true",
        verbose=os.environ.get("VERBOSE", "true").lower() == "true",
    )

    run_id = str(uuid.uuid4())
    started_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    log(cfg, "=== Phase 1: S3 -> PostgreSQL Staging ===")
    log(cfg, f"Run ID: {run_id}")
    log(cfg, f"Batch date: {cfg.batch_date}")
    log(cfg, f"S3 bucket: {cfg.s3_bucket}")
    log(cfg, f"S3 prefix: {cfg.s3_prefix}")
    log(cfg, f"S3 endpoint: {cfg.s3_endpoint_url or 'AWS default'}")
    log(cfg, f"AWS profile: {cfg.aws_profile or 'default credential chain'}")
    log(cfg, f"Force path style: {cfg.s3_force_path_style}")
    log(cfg, "")

    s3 = build_s3_client(cfg)
    inc_prefix = incoming_prefix(cfg)

    # 1) List files
    log(cfg, f"[1/5] Listing CSV files under: s3://{cfg.s3_bucket}/{inc_prefix}")
    keys = list_csv_keys(s3, cfg.s3_bucket, inc_prefix)

    if not keys:
        raise RuntimeError(f"No CSV files found under: s3://{cfg.s3_bucket}/{inc_prefix}")

    log(cfg, f"Found {len(keys)} CSV files:")
    for k in keys:
        rel = k.replace(inc_prefix, "", 1)
        if rel in FILE_MAP:
            table, mode = FILE_MAP[rel]
            log(cfg, f"  - {rel}  -> {table} ({mode})")
        else:
            log(cfg, f"  - {rel}  (ignored: not in FILE_MAP)")
    log(cfg, "")

    # 2) Connect Postgres
    log(cfg, "[2/5] Connecting to PostgreSQL...")
    conn = psycopg2.connect(cfg.pg_dsn)
    ensure_staging_schema(conn)
    log(cfg, "Connected and ensured schema `staging` exists.")
    log(cfg, "")

    manifest_files = []
    status = "success"

    try:
        # 3) Load to staging
        log(cfg, "[3/5] Loading files into staging tables...")
        loaded_count = 0

        for key in keys:
            rel = key.replace(inc_prefix, "", 1)
            if rel not in FILE_MAP:
                continue

            table, mode = FILE_MAP[rel]
            is_full = (mode == "FULL")

            log(cfg, f"  -> Downloading s3://{cfg.s3_bucket}/{key}")
            obj = s3.get_object(Bucket=cfg.s3_bucket, Key=key)
            data = obj["Body"].read()

            checksum = sha256_bytes(data)
            rows = count_csv_rows(data)

            log(cfg, f"     Loading into {table} ({'TRUNCATE+COPY' if is_full else 'APPEND COPY'}) | rows~{rows}")
            copy_csv_bytes_to_postgres(conn, table, data, truncate=is_full)

            manifest_files.append({
                "s3_key": key,
                "relative_path": rel,
                "target_table": table,
                "load_mode": mode,
                "rows_loaded_estimate": rows,
                "sha256": checksum,
            })
            loaded_count += 1

        if loaded_count == 0:
            raise RuntimeError("No recognized CSV files were loaded. Check FILE_MAP vs S3 layout.")

        log(cfg, f"Loaded {loaded_count} file(s) into staging.")
        log(cfg, "")

        finished_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        # 4) Create manifest
        log(cfg, "[4/5] Writing manifest to S3...")
        manifest = {
            "run_id": run_id,
            "pipeline": "phase1_s3_to_staging",
            "batch_date": cfg.batch_date,
            "started_at": started_at,
            "finished_at": finished_at,
            "bucket": cfg.s3_bucket,
            "incoming_prefix": inc_prefix,
            "status": status,
            "files": manifest_files,
        }

        m_key = manifest_key(cfg, run_id)
        s3.put_object(
            Bucket=cfg.s3_bucket,
            Key=m_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        log(cfg, f"Manifest written: s3://{cfg.s3_bucket}/{m_key}")
        log(cfg, "")

        # 5) Move to archive
        log(cfg, "[5/5] Archiving processed files (copy+delete)...")
        arch_pref = archive_prefix(cfg, run_id)
        for f in manifest_files:
            src_key = f["s3_key"]
            rel = f["relative_path"]
            dst_key = arch_pref + rel
            log(cfg, f"  - Moving {src_key} -> {dst_key}")
            move_s3_object(s3, cfg.s3_bucket, src_key, dst_key)

        log(cfg, "")
        log(cfg, "✅ Done.")
        log(cfg, f"Archived under: s3://{cfg.s3_bucket}/{arch_pref}")
        log(cfg, "Next step: Staging -> Data Warehouse (ELT + Data Quality).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()