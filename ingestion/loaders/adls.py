# ingestion/loaders/adls.py
from __future__ import annotations

import io
import logging
import os

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings, StorageStreamDownloader

logger = logging.getLogger(__name__)



def _get_client() -> BlobServiceClient:
    conn_str = os.environ["AZURE_STORAGE_CONN_STR"]
    return BlobServiceClient.from_connection_string(conn_str)


def upload_parquet(
    df: pd.DataFrame,
    container: str,
    blob_path: str,
    overwrite: bool = True,
) -> str:
    """
    Serialize DataFrame to Parquet (snappy) and upload to ADLS Gen2.
    Returns the full blob path for lineage tracking.
    Idempotent by default (overwrite=True).
    """
    if df.empty:
        raise ValueError(f"Refusing to upload empty DataFrame to {container}/{blob_path}")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy", engine="pyarrow")
    buffer.seek(0)

    client = _get_client()
    blob_client = client.get_blob_client(container=container, blob=blob_path)

    blob_client.upload_blob( # type: ignore
        buffer,
        overwrite=overwrite,
        content_settings=ContentSettings(content_type="application/octet-stream"),
    )

    full_path = f"abfss://{container}@{_get_account_name()}.dfs.core.windows.net/{blob_path}"
    logger.info("adls | uploaded rows=%d path=%s", len(df), full_path)

    return full_path


def download_parquet(container: str, blob_path: str) -> pd.DataFrame:
    """Download a Parquet blob and return as DataFrame."""
    client = _get_client()
    blob_client = client.get_blob_client(container=container, blob=blob_path)

    stream : StorageStreamDownloader[bytes] = blob_client.download_blob() # type: ignore
    buffer = io.BytesIO(stream.readall())

    return pd.read_parquet(buffer, engine="pyarrow")


def list_blobs(container: str, prefix: str) -> list[str]:
    """List blob paths under a given prefix."""
    client = _get_client()
    container_client = client.get_container_client(container)

    return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]


def _get_account_name() -> str:
    """Extract storage account name from connection string."""
    conn_str = os.environ["AZURE_STORAGE_CONN_STR"]
    for part in conn_str.split(";"):
        if part.startswith("AccountName="):
            return part.split("=", 1)[1]
    raise ValueError("AccountName not found in AZURE_STORAGE_CONN_STR")