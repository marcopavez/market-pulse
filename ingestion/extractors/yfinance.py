# ingestion/extractors/yahoo_finance.py
from __future__ import annotations

import logging
from typing import Literal

import pandas as pd
import yfinance as yf
from curl_cffi import requests

logger = logging.getLogger(__name__)

Period = Literal["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]

OHLCV_COLUMNS = {"open", "high", "low", "close", "volume"}

# Column rename map: yfinance history() returns title-case
_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "Dividends": "dividends",
    "Stock Splits": "stock_splits",
}


def _make_session() -> requests.Session:
    """Chrome-impersonating session — bypasses Yahoo 429 throttling."""
    return requests.Session(impersonate="chrome")


def _clean_history(df: pd.DataFrame, ticker: str, period: Period) -> pd.DataFrame:
    """Normalize raw yfinance history DataFrame to a consistent schema."""
    df = df.rename(columns=_RENAME)

    # Date index → column, strip timezone, cast to date
    df = df.reset_index().rename(columns={"Date": "date", "Datetime": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date

    df["ticker"] = ticker
    df["extracted_at"] = pd.Timestamp.utcnow()

    # Enforce column order, drop anything extra
    base_cols = ["date", "ticker", "open", "high", "low", "close", "volume",
                 "dividends", "stock_splits", "extracted_at"]
    return df[[c for c in base_cols if c in df.columns]]


def _validate_ohlcv(df: pd.DataFrame, ticker: str) -> None:
    missing = OHLCV_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"ticker={ticker} missing columns: {missing}")
    if (df["high"] < df["low"]).any():
        raise ValueError(f"ticker={ticker} has rows where high < low")
    if (df["close"] <= 0).any():
        raise ValueError(f"ticker={ticker} has non-positive close prices")


def fetch_prices(
    ticker: str,
    period: Period = "1mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Fetch OHLCV data for a single ticker via yfinance.
    Uses curl_cffi Chrome impersonation to avoid Yahoo 429s.
    Raises ValueError on empty response or failed validation.
    """
    session = _make_session()
    t = yf.Ticker(ticker, session=session)
    raw = t.history(period=period, interval=interval, auto_adjust=True)

    if raw.empty:
        raise ValueError(f"No data returned for ticker '{ticker}' — delisted or invalid")

    df = _clean_history(raw, ticker, period)
    _validate_ohlcv(df, ticker)

    logger.info("yahoo_finance | ticker=%s rows=%d period=%s", ticker, len(df), period)
    return df


def fetch_prices_bulk(
    tickers: list[str],
    period: Period = "1mo",
    interval: str = "1d",
) -> dict[str, pd.DataFrame]:
    """
    Fetch multiple tickers sharing a single session.
    Skips tickers that fail — logs error but doesn't abort the batch.
    """
    session = _make_session()
    results: dict[str, pd.DataFrame] = {}

    for ticker in tickers:
        try:
            t = yf.Ticker(ticker, session=session)
            raw = t.history(period=period, interval=interval, auto_adjust=True)

            if raw.empty:
                logger.warning("yahoo_finance | ticker=%s returned empty — skipping", ticker)
                continue

            df = _clean_history(raw, ticker, period)
            _validate_ohlcv(df, ticker)
            results[ticker] = df

            logger.info("yahoo_finance | ticker=%s rows=%d", ticker, len(df))

        except Exception as exc:
            logger.error("yahoo_finance | ticker=%s error=%s", ticker, exc)

    return results


def fetch_dividends(ticker: str) -> pd.DataFrame:
    """Returns dividend history. Empty DataFrame if none (e.g. BTC-USD)."""
    session = _make_session()
    t = yf.Ticker(ticker, session=session)
    divs = t.dividends

    if divs.empty:
        return pd.DataFrame(columns=["date", "dividend", "ticker", "extracted_at"])

    df = divs.reset_index()
    df.columns = pd.Index(["date", "dividend"])
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date
    df["ticker"] = ticker
    df["extracted_at"] = pd.Timestamp.utcnow()

    return df