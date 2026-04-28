# Market Pulse — Project Guide

Comprehensive reference. Originally lived in `CLAUDE.md`; moved here so `CLAUDE.md` can stay focused on the conventions Claude needs to operate. For current phase status and code map see `CONTEXT.md`.

---

## Project Overview

Market Pulse is a financial data platform that ingests equity prices and macro indicators, transforms them in PostgreSQL via dbt, and exposes analytics through Grafana and Power BI. The pipeline runs on Apache Airflow (Docker) and infrastructure is provisioned with Terraform on Azure.

---

## Architecture

```
yfinance / FRED API
       │
  ingestion/           ← Python extractors + ADLS loader
       │
  ADLS Gen2 (raw)      ← Parquet files partitioned by date
       │
  PostgreSQL (raw.*)   ← Raw tables loaded by Airflow
       │
  dbt (staging → marts) ← Typed views + analytical fact tables
       │
  Grafana / Power BI   ← Dashboards (Phase 3)
```

---

## Phases

| Phase | Status | Scope |
|-------|--------|-------|
| 1 | ✅ Done | Ingestion: extractors, ADLS loader, Airflow DAG, Terraform |
| 2 | ✅ Done | dbt models, Great Expectations validation, anomaly alerts |
| 3 | ⏳ Pending | Grafana dashboards, Power BI |
| 4 | ⏳ Pending | README, unit tests, CI/CD demo |

---

## Repo Structure

```
market-pulse/
├── airflow/
│   ├── dags/
│   │   ├── ingestion_dag.py     # Main DAG: extract → load → validate → alerts → dbt → mart validation
│   │   └── callbacks.py         # on_failure_callback + send_alert (Slack)
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt         # Python deps for the Airflow image
├── ingestion/
│   ├── extractors/
│   │   ├── yfinance.py          # curl_cffi-backed Yahoo Finance fetcher
│   │   └── fred.py              # FRED API extractor
│   └── loaders/
│       └── adls.py              # Upload/download Parquet to ADLS Gen2
├── dbt/market_pulse/
│   ├── models/
│   │   ├── staging/             # Typed + cleaned views over raw tables
│   │   ├── intermediate/        # (reserved for future join models)
│   │   └── marts/               # Analytical fact tables materialized as tables
│   ├── macros/
│   ├── tests/
│   ├── seeds/
│   └── dbt_project.yml
├── validation/
│   ├── raw_prices.py            # GX suite for raw.raw_prices
│   └── marts.py                 # GX suites for marts (fct_daily_returns, fct_volatility)
├── infra/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── CONTEXT.md                   # Project state snapshot (update as phases complete)
├── CLAUDE.md                    # Lean operating guide for Claude
├── docs/project-guide.md        # This file — full project guide
└── README.md
```

---

## Key Constants

| Name | Value | Location |
|------|-------|----------|
| `TICKERS` | `AAPL, MSFT, GOOGL, SPY, BTC-USD` | `ingestion_dag.py` |
| `ANOMALY_THRESHOLD` | `0.10` (10% daily move) | `ingestion_dag.py` |
| Volatility window | 30 trading days | `fct_volatility.sql` |
| Correlation window | 30 trading days | `fct_correlations.sql` |
| DAG schedule | `0 1 * * 2-6` (Tue–Sat 01:00 UTC) | `ingestion_dag.py` |

---

## Environment Variables

All secrets are injected via `.env` (never committed). The Airflow Docker Compose reads them at startup.

| Variable | Used by | Purpose |
|----------|---------|---------|
| `AZURE_STORAGE_CONN_STR` | `loaders/adls.py` | ADLS Gen2 connection |
| `AZURE_SQL_CONN_STR` | `ingestion_dag.py`, `validation/` | PostgreSQL Flexible Server |
| `FERNET_KEY` | Airflow | Airflow encryption key |
| `ALPHA_VANTAGE_API_KEY` | (reserved) | Future data source |
| `SLACK_WEBHOOK_URL` | `callbacks.py` | Optional Slack alerting |

---

## Azure Infrastructure

| Resource | Name | Location |
|----------|------|----------|
| Resource Group | `rg-market-pulse` | eastus |
| ADLS Gen2 | `stmarketpulse001` | containers: `raw`, `processed` |
| PostgreSQL Flexible Server | `psql-market-pulse-001` | eastus2 |
| Database | `market_pulse_dw` | — |

---

## Database Schemas

| Schema | Populated by | Contents |
|--------|-------------|----------|
| `raw` | Airflow DAG | `raw_prices`, `raw_macro` |
| `staging` | dbt | `stg_prices`, `stg_macro` |
| `intermediate` | dbt | (reserved) |
| `marts` | dbt | `fct_daily_returns`, `fct_volatility`, `fct_correlations`, `fct_macro_overlay` |

---

## dbt Conventions

- **staging** models: views, named `stg_<source>`, cast all types explicitly, filter nulls and bad data.
- **intermediate** models: views, named `int_<description>`, for multi-source joins.
- **marts** models: tables, named `fct_<metric>` (facts) or `dim_<entity>` (dimensions).
- All `numeric` columns use `numeric(18, 6)` precision.
- Window functions always use explicit `rows between N preceding and current row`.
- Rows with fewer than `N` observations in a rolling window are excluded (not zero-filled).

---

## Validation Conventions

- Great Expectations runs in **ephemeral mode** (no file store) — no `great_expectations.yml` needed.
- Each validation function raises `RuntimeError` on failure so Airflow marks the task as failed.
- Validation functions live in `validation/` and are called directly from Airflow tasks via `sys.path` injection.
- `raw_prices.py` validates ingestion output; `marts.py` validates transformed output.

---

## Coding Conventions

- Python: `from __future__ import annotations`, type hints on all function signatures.
- SQL: lowercase keywords, snake_case identifiers, CTEs for all non-trivial logic.
- No ORM — raw `psycopg2` for the loader; dbt handles all transformation SQL.
- Airflow tasks use the `@task` decorator (TaskFlow API). No classic operators.
- Secrets: never hardcode — always read from `os.environ`.
- Logging: use `logging.getLogger(__name__)`, not `print`.

---

## Running Locally

```bash
# Start Airflow stack
cd airflow
docker compose up airflow-init   # first time only
docker compose up -d

# Run dbt
cd dbt/market_pulse
dbt deps
dbt run
dbt test
```
