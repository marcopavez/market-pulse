# airflow/dags/ingestion_dag.py
from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from callbacks import on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    "owner": "market-pulse",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY", "BTC-USD"]
ANOMALY_THRESHOLD = 0.10          # 10% daily move triggers an alert
FRED_OBSERVATION_START = "2020-01-01"

DBT_PROJECT_DIR = "/opt/airflow/dbt/market_pulse"


@dag(
    dag_id="ingestion_market_prices",
    schedule="0 1 * * 2-6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "prices", "macro", "dbt"],
)
def ingestion_dag() -> None:

    # ------------------------------------------------------------------ #
    #  PRICES                                                              #
    # ------------------------------------------------------------------ #

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
            blob_path = (
                f"prices/{ticker}/{datetime.utcnow().strftime('%Y/%m/%d')}/{ticker}.parquet"
            )
            upload_parquet(df=df, container="raw", blob_path=blob_path)
            results[ticker] = blob_path

        return results

    @task()
    def validate_load(paths: dict[str, str]) -> None:
        for ticker, path in paths.items():
            if not path:
                raise ValueError(f"Missing upload for {ticker}")

    @task()
    def load_to_postgres(paths: dict[str, str]) -> bool:
        import sys
        ingestion_path = "/opt/airflow/ingestion"
        if ingestion_path not in sys.path:
            sys.path.insert(0, ingestion_path)

        from loaders.adls import download_parquet

        import os
        import psycopg2
        import urllib.parse as urlparse

        conn_str = os.environ["AZURE_SQL_CONN_STR"]
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
                        date         DATE,
                        ticker       VARCHAR(20),
                        open         DOUBLE PRECISION,
                        high         DOUBLE PRECISION,
                        low          DOUBLE PRECISION,
                        close        DOUBLE PRECISION,
                        volume       BIGINT,
                        dividends    DOUBLE PRECISION,
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
        return True

    @task()
    def validate_raw_prices(_loaded: bool) -> None:
        """Run Great Expectations suite against raw.raw_prices."""
        import sys
        if "/opt/airflow/validation" not in sys.path:
            sys.path.insert(0, "/opt/airflow/validation")

        import os
        from raw_prices import validate_raw_prices as gx_validate

        gx_validate(os.environ["AZURE_SQL_CONN_STR"])

    @task()
    def detect_anomalies(_loaded: bool) -> None:
        """Alert if any ticker moved more than ANOMALY_THRESHOLD in the latest session."""
        import os
        import psycopg2
        import urllib.parse as urlparse

        from callbacks import send_alert

        conn_str = os.environ["AZURE_SQL_CONN_STR"]
        url = urlparse.urlparse(conn_str)
        conn = psycopg2.connect(
            host=url.hostname,
            port=url.port or 5432,
            dbname=url.path.lstrip("/").split("?")[0],
            user=url.username,
            password=url.password,
            sslmode="require",
        )

        with conn.cursor() as cur:
            cur.execute("""
                WITH returns AS (
                    SELECT
                        ticker,
                        date,
                        close,
                        LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
                    FROM raw.raw_prices
                    WHERE date >= CURRENT_DATE - INTERVAL '3 days'
                )
                SELECT
                    ticker,
                    date,
                    close,
                    prev_close,
                    ROUND(
                        ((close - prev_close) / NULLIF(prev_close, 0) * 100)::numeric, 2
                    ) AS return_pct
                FROM returns
                WHERE prev_close IS NOT NULL
                  AND ABS((close - prev_close) / NULLIF(prev_close, 0)) > %s
                ORDER BY date DESC
            """, (ANOMALY_THRESHOLD,))
            anomalies = cur.fetchall()

        conn.close()

        if anomalies:
            lines = "\n".join(
                f"  {ticker} on {date}: {ret}% (close={close:.2f}, prev={prev:.2f})"
                for ticker, date, close, prev, ret in anomalies
            )
            msg = f"Price anomaly detected ({len(anomalies)} event(s)):\n{lines}"
            logger.warning(msg)
            send_alert(msg)
        else:
            logger.info("No anomalies detected.")

    # ------------------------------------------------------------------ #
    #  MACRO (FRED)                                                         #
    # ------------------------------------------------------------------ #

    @task()
    def extract_and_load_macro() -> str:
        """Fetch all FRED macro series and upload as a single Parquet bundle to ADLS."""
        import sys
        ingestion_path = "/opt/airflow/ingestion"
        if ingestion_path not in sys.path:
            sys.path.insert(0, ingestion_path)

        from extractors.fred import fetch_macro_bundle
        from loaders.adls import upload_parquet

        df = fetch_macro_bundle(observation_start=FRED_OBSERVATION_START)
        date_str = datetime.utcnow().strftime("%Y/%m/%d")
        blob_path = f"macro/{date_str}/macro_bundle.parquet"
        upload_parquet(df=df, container="raw", blob_path=blob_path)
        logger.info("Macro bundle uploaded: %s (%d rows)", blob_path, len(df))
        return blob_path

    @task()
    def load_macro_to_postgres(macro_path: str) -> bool:
        """Download the macro Parquet from ADLS and upsert into raw.raw_macro."""
        import sys
        ingestion_path = "/opt/airflow/ingestion"
        if ingestion_path not in sys.path:
            sys.path.insert(0, ingestion_path)

        from loaders.adls import download_parquet

        import os
        import psycopg2
        import urllib.parse as urlparse

        df = download_parquet(container="raw", blob_path=macro_path)

        conn_str = os.environ["AZURE_SQL_CONN_STR"]
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
                    CREATE TABLE IF NOT EXISTS raw.raw_macro (
                        date         DATE,
                        series_id    VARCHAR(20),
                        metric_name  VARCHAR(100),
                        value        DOUBLE PRECISION,
                        extracted_at TIMESTAMP,
                        PRIMARY KEY (date, series_id)
                    )
                """)

            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO raw.raw_macro
                            (date, series_id, metric_name, value, extracted_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (date, series_id) DO UPDATE SET
                            metric_name  = EXCLUDED.metric_name,
                            value        = EXCLUDED.value,
                            extracted_at = EXCLUDED.extracted_at
                    """, (
                        row.get("date"), row.get("series_id"),
                        row.get("metric_name"), row.get("value"),
                        row.get("extracted_at"),
                    ))

        conn.close()
        logger.info("Macro rows loaded to raw.raw_macro: %d", len(df))
        return True

    # ------------------------------------------------------------------ #
    #  DBT                                                                 #
    # ------------------------------------------------------------------ #

    @task()
    def run_dbt(_prices_loaded: bool, _macro_loaded: bool) -> None:
        """
        Run dbt after both raw tables are populated.
        Parses AZURE_SQL_CONN_STR into individual env vars that profiles.yml reads.
        """
        import os
        import urllib.parse as urlparse

        conn_str = os.environ["AZURE_SQL_CONN_STR"]
        url = urlparse.urlparse(conn_str)

        dbt_env = {
            **os.environ,
            "DBT_HOST":     url.hostname or "",
            "DBT_PORT":     str(url.port or 5432),
            "DBT_USER":     url.username or "",
            "DBT_PASSWORD": url.password or "",
            "DBT_DBNAME":   url.path.lstrip("/").split("?")[0],
        }

        def _run(cmd: list[str]) -> None:
            result = subprocess.run(cmd, env=dbt_env, capture_output=True, text=True)
            logger.info("dbt stdout:\n%s", result.stdout)
            if result.returncode != 0:
                logger.error("dbt stderr:\n%s", result.stderr)
                raise RuntimeError(f"dbt command failed (exit {result.returncode}): {' '.join(cmd)}")

        base = ["--profiles-dir", DBT_PROJECT_DIR, "--project-dir", DBT_PROJECT_DIR]
        _run(["dbt", "deps"] + base)
        _run(["dbt", "run"] + base)

    # ------------------------------------------------------------------ #
    #  MARTS VALIDATION                                                    #
    # ------------------------------------------------------------------ #

    @task()
    def validate_marts(_dbt_done: None) -> None:
        """Run Great Expectations suites against dbt mart tables."""
        import sys
        if "/opt/airflow/validation" not in sys.path:
            sys.path.insert(0, "/opt/airflow/validation")

        import os
        from marts import validate_fct_daily_returns, validate_fct_volatility

        conn_str = os.environ["AZURE_SQL_CONN_STR"]
        validate_fct_daily_returns(conn_str)
        validate_fct_volatility(conn_str)

    # ------------------------------------------------------------------ #
    #  DAG WIRING                                                          #
    # ------------------------------------------------------------------ #

    # Prices branch
    prices = extract_and_load_prices(TICKERS)
    validate_load(prices)
    loaded_prices = load_to_postgres(prices)
    validate_raw_prices(loaded_prices)
    detect_anomalies(loaded_prices)

    # Macro branch (runs in parallel with prices)
    macro_path = extract_and_load_macro()
    loaded_macro = load_macro_to_postgres(macro_path)

    # dbt runs once both raw tables are ready, then marts are validated
    dbt_done = run_dbt(loaded_prices, loaded_macro)
    validate_marts(dbt_done)


ingestion_dag()
