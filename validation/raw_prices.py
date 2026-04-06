# validation/raw_prices.py
from __future__ import annotations

import logging

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
    UnexpectedRowsExpectation,
)

logger = logging.getLogger(__name__)

EXPECTED_TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY", "BTC-USD"]


def _gx_conn_str(raw_conn_str: str) -> str:
    """Ensure the connection string uses the psycopg2 dialect for SQLAlchemy."""
    if raw_conn_str.startswith("postgresql://"):
        return raw_conn_str.replace("postgresql://", "postgresql+psycopg2://", 1)
    return raw_conn_str


def validate_raw_prices(conn_str: str) -> bool:
    """
    Run Great Expectations validation suite against raw.raw_prices.
    Raises RuntimeError if any expectation fails so Airflow marks the task failed.
    Returns True on full pass.
    """
    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_postgres(
        name="market_pulse_pg",
        connection_string=_gx_conn_str(conn_str),
    )

    asset = datasource.add_table_asset(
        name="raw_prices",
        table_name="raw_prices",
        schema_name="raw",
    )

    batch_def = asset.add_batch_definition_whole_table("all_rows")

    suite = context.suites.add(gx.ExpectationSuite(name="raw_prices_suite"))

    # --- Nullability ---
    for col in ["date", "ticker", "open", "high", "low", "close", "volume"]:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))

    # --- Expected tickers ---
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(column="ticker", value_set=EXPECTED_TICKERS)
    )

    # --- Price sanity ---
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="close", min_value=0, strict_min=True)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="open", min_value=0, strict_min=True)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="volume", min_value=0)
    )

    # --- high >= low ---
    suite.add_expectation(
        UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE high < low"
        )
    )

    # --- No future dates ---
    suite.add_expectation(
        UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE date > CURRENT_DATE"
        )
    )

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="raw_prices_validation",
            data=batch_def,
            suite=suite,
        )
    )

    result = validation_def.run()

    if not result.success:
        failed = [
            str(r.expectation_config.type)
            for r in result.results
            if not r.success
        ]
        raise RuntimeError(
            f"raw_prices validation failed — {len(failed)} expectation(s): {failed}"
        )

    logger.info("raw_prices validation passed — all expectations met.")
    return True
