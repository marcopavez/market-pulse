# ingestion/loaders/r2.py
from __future__ import annotations

import io
import logging
import os
from functools import lru_cache

import boto3
import pandas as pd
from botocore.config import Config

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def upload_parquet(
    df: pd.DataFrame,
    bucket: str,
    blob_path: str,
) -> str:
    """
    Serialize DataFrame to Parquet (snappy) and upload to Cloudflare R2.
    Returns the s3:// path for lineage tracking.
    Idempotent — R2 PutObject overwrites by default.
    """
    if df.empty:
        raise ValueError(f"Refusing to upload empty DataFrame to {bucket}/{blob_path}")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy", engine="pyarrow")
    buffer.seek(0)

    _get_client().put_object(
        Bucket=bucket,
        Key=blob_path,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    full_path = f"s3://{bucket}/{blob_path}"
    logger.info("r2 | uploaded rows=%d path=%s", len(df), full_path)

    return full_path


def download_parquet(bucket: str, blob_path: str) -> pd.DataFrame:
    """Download a Parquet object and return as DataFrame."""
    obj = _get_client().get_object(Bucket=bucket, Key=blob_path)
    buffer = io.BytesIO(obj["Body"].read())
    return pd.read_parquet(buffer, engine="pyarrow")


def list_blobs(bucket: str, prefix: str) -> list[str]:
    """List object keys under a given prefix."""
    paginator = _get_client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
