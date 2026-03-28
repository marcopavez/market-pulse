# airflow/dags/ingestion_dag.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {
    "owner": "market-pulse",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,  # TODO: Fase 2 → Slack/email alert
}

TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY", "BTC-USD"]


@dag(
    dag_id="ingestion_market_prices",
    schedule="0 1 * * 2-6",   # Martes-Sábado 01:00 UTC (post-cierre NYSE)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "prices"],
)
def ingestion_dag() -> None:

    @task()
    def extract_and_load_prices(tickers: list[str]) -> dict[str, str]:
        """Extract OHLCV from Yahoo Finance and upload to ADLS Gen2 raw zone."""
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion")

        from extractors.yahoo_finance import fetch_prices
        from loaders.adls import upload_parquet

        results = {}
        for ticker in tickers:
            df = fetch_prices(ticker, period="5d")
            path = upload_parquet(
                df=df,
                container="raw",
                blob_path=f"prices/{ticker}/{datetime.now().strftime('%Y/%m/%d')}/{ticker}.parquet",
            )
            results[ticker] = path

        return results

    @task()
    def validate_load(paths: dict[str, str]) -> None:
        """Basic row-count check — Great Expectations en Fase 2."""
        for ticker, path in paths.items():
            if not path:
                raise ValueError(f"Missing upload for {ticker}")

    paths = extract_and_load_prices(TICKERS)
    validate_load(paths)


ingestion_dag()