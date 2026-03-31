# ingestion/extractors/fred.py
from __future__ import annotations

import logging
import os
from typing import Any

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Series más relevantes para contexto macro
MACRO_SERIES: dict[str, str] = {
    "FEDFUNDS":  "fed_funds_rate",        # tasa de referencia Fed
    "CPIAUCSL":  "cpi_all_items",         # inflación CPI
    "T10Y2Y":    "yield_curve_10y2y",     # spread curva de rendimiento
    "UNRATE":    "unemployment_rate",      # desempleo
    "GDP":       "gdp_quarterly",          # PIB (quarterly)
    "DGS10":    "treasury_10y_yield",     # bono 10 años
}


def fetch_series(
    series_id: str,
    observation_start: str = "2020-01-01",
    observation_end: str | None = None,
    api_key: str | None = None,
) -> pd.DataFrame:
    """
    Fetch a single FRED series.
    FRED API is free with no meaningful rate limit (no key needed for public series,
    but key gives higher limits — set FRED_API_KEY env var).
    """
    _api_key = api_key or os.environ.get("FRED_API_KEY", "")

    params: dict[str, Any] = {
        "series_id": series_id,
        "observation_start": observation_start,
        "file_type": "json",
        "sort_order": "asc",
    }
    if observation_end:
        params["observation_end"] = observation_end
    if _api_key:
        params["api_key"] = _api_key

    with httpx.Client(timeout=30) as client:
        response = client.get(FRED_BASE_URL, params=params)
        response.raise_for_status()

    data = response.json()

    if "observations" not in data:
        raise ValueError(f"Unexpected FRED response for series '{series_id}': {data}")

    df = pd.DataFrame(data["observations"])[["date", "value"]]

    # FRED returns "." for missing observations
    df = df[df["value"] != "."].copy()
    df["value"] = df["value"].astype(float)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["series_id"] = series_id
    df["extracted_at"] = pd.Timestamp.utcnow()

    logger.info("fred | series=%s rows=%d start=%s", series_id, len(df), observation_start)
    return df


def fetch_macro_bundle(
    observation_start: str = "2020-01-01",
    api_key: str | None = None,
) -> pd.DataFrame:
    """
    Fetch all series in MACRO_SERIES and return a single long-format DataFrame.
    Continues on individual series failure (logs error, doesn't abort).
    """
    frames: list[pd.DataFrame] = []

    for series_id, friendly_name in MACRO_SERIES.items():
        try:
            df = fetch_series(series_id, observation_start=observation_start, api_key=api_key)
            df["metric_name"] = friendly_name
            frames.append(df)
        except Exception as exc:
            logger.error("fred | series=%s error=%s", series_id, exc)

    if not frames:
        raise RuntimeError("All FRED series failed — check connectivity or API key")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[["date", "series_id", "metric_name", "value", "extracted_at"]]

    return combined