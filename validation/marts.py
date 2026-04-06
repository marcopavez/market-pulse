# validation/marts.py
from __future__ import annotations

import logging

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
    ExpectTableRowCountToBeGreaterThan,
    UnexpectedRowsExpectation,
)

logger = logging.getLogger(__name__)

EXPECTED_TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY", "BTC-USD"]


def _gx_conn_str(raw_conn_str: str) -> str:
    if raw_conn_str.startswith("postgresql://"):
        return raw_conn_str.replace("postgresql://", "postgresql+psycopg2://", 1)
    return raw_conn_str


def _add_datasource(context: gx.DataContext, conn_str: str) -> gx.datasource.fluent.PostgresDatasource:
    return context.data_sources.add_postgres(
        name="market_pulse_pg",
        connection_string=_gx_conn_str(conn_str),
    )


def validate_fct_daily_returns(conn_str: str, marts_schema: str = "marts") -> bool:
    """
    Validate fct_daily_returns:
      - no nulls on key columns
      - expected tickers only
      - daily returns within plausible range [-1, 1] (i.e. -100% to +100%)
      - close price > 0
      - table is not empty
    """
    context = gx.get_context(mode="ephemeral")
    datasource = _add_datasource(context, conn_str)

    asset = datasource.add_table_asset(
        name="fct_daily_returns",
        table_name="fct_daily_returns",
        schema_name=marts_schema,
    )
    batch_def = asset.add_batch_definition_whole_table("all_rows")
    suite = context.suites.add(gx.ExpectationSuite(name="fct_daily_returns_suite"))

    for col in ["date", "ticker", "close", "prev_close", "daily_return", "daily_return_pct", "volume"]:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))

    suite.add_expectation(
        ExpectColumnValuesToBeInSet(column="ticker", value_set=EXPECTED_TICKERS)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="close", min_value=0, strict_min=True)
    )
    # Returns outside [-1, 1] are theoretically possible (e.g. BTC) but flag if extreme
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="daily_return", min_value=-1.0, max_value=1.0)
    )
    suite.add_expectation(ExpectTableRowCountToBeGreaterThan(value=0))

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="fct_daily_returns_validation",
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
            f"fct_daily_returns validation failed — {len(failed)} expectation(s): {failed}"
        )

    logger.info("fct_daily_returns validation passed.")
    return True


def validate_fct_volatility(conn_str: str, marts_schema: str = "marts") -> bool:
    """
    Validate fct_volatility:
      - no nulls on key columns
      - annualized volatility > 0
      - obs_count always 30
    """
    context = gx.get_context(mode="ephemeral")
    datasource = _add_datasource(context, conn_str)

    asset = datasource.add_table_asset(
        name="fct_volatility",
        table_name="fct_volatility",
        schema_name=marts_schema,
    )
    batch_def = asset.add_batch_definition_whole_table("all_rows")
    suite = context.suites.add(gx.ExpectationSuite(name="fct_volatility_suite"))

    for col in ["date", "ticker", "volatility_30d_annualized_pct", "obs_count"]:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))

    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="volatility_30d_annualized_pct", min_value=0, strict_min=True
        )
    )
    suite.add_expectation(
        UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE obs_count != 30"
        )
    )

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="fct_volatility_validation",
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
            f"fct_volatility validation failed — {len(failed)} expectation(s): {failed}"
        )

    logger.info("fct_volatility validation passed.")
    return True
