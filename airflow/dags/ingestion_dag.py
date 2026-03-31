# airflow/dags/ingestion_dag.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "market-pulse",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY", "BTC-USD"]


@dag(
    dag_id="ingestion_market_prices",
    schedule="0 1 * * 2-6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "prices"],
)
def ingestion_dag() -> None:

    @task()
    def extract_and_load_prices(tickers: list[str]) -> dict[str, str]:
        import sys
        ingestion_path = "/opt/airflow/ingestion"
        if ingestion_path not in sys.path:
            sys.path.insert(0, ingestion_path)

        from extractors.yfinance import fetch_prices
        from loaders.adls import upload_parquet

        results = {}
        for ticker in tickers:
            df = fetch_prices(ticker, period="5d")
            # blob_path relativo 
            blob_path = f"prices/{ticker}/{datetime.utcnow().strftime('%Y/%m/%d')}/{ticker}.parquet"
            upload_parquet(
                df=df,
                container="raw",
                blob_path=blob_path,
            )
            results[ticker] = blob_path  

        return results

    @task()
    def load_to_postgres(paths: dict[str, str]) -> None:
        import sys
        ingestion_path = "/opt/airflow/ingestion"
        if ingestion_path not in sys.path:
            sys.path.insert(0, ingestion_path)
    
        import os
        import psycopg2
        from loaders.adls import download_parquet
    
        conn_str = os.environ["AZURE_SQL_CONN_STR"]
    
        # parse postgresql://user:pass@host/db?sslmode=require
        import urllib.parse as urlparse
        url = urlparse.urlparse(conn_str)
    
        conn = psycopg2.connect(
            host=url.hostname,
            port=url.port or 5432,
            dbname=url.path.lstrip("/").split("?")[0],
            user=url.username,
            password=url.password,
            sslmode="require",
        )
    
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw.raw_prices (
                        date        DATE,
                        ticker      VARCHAR(20),
                        open        DOUBLE PRECISION,
                        high        DOUBLE PRECISION,
                        low         DOUBLE PRECISION,
                        close       DOUBLE PRECISION,
                        volume      BIGINT,
                        dividends   DOUBLE PRECISION,
                        stock_splits DOUBLE PRECISION,
                        extracted_at TIMESTAMP,
                        PRIMARY KEY (date, ticker)
                    )
                """)
    
            for ticker, blob_path in paths.items():
                df = download_parquet(container="raw", blob_path=blob_path)
    
                with conn.cursor() as cur:
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO raw.raw_prices
                                (date, ticker, open, high, low, close, volume,
                                 dividends, stock_splits, extracted_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (date, ticker) DO UPDATE SET
                                open         = EXCLUDED.open,
                                high         = EXCLUDED.high,
                                low          = EXCLUDED.low,
                                close        = EXCLUDED.close,
                                volume       = EXCLUDED.volume,
                                extracted_at = EXCLUDED.extracted_at
                        """, (
                            row.get("date"), row.get("ticker"),
                            row.get("open"), row.get("high"),
                            row.get("low"), row.get("close"),
                            row.get("volume"), row.get("dividends", 0),
                            row.get("stock_splits", 0), row.get("extracted_at"),
                        ))
    
        conn.close()

    @task()
    def validate_load(paths: dict[str, str]) -> None:
        for ticker, path in paths.items():
            if not path:
                raise ValueError(f"Missing upload for {ticker}")

    paths = extract_and_load_prices(TICKERS)
    validate_load(paths)
    load_to_postgres(paths)


ingestion_dag()