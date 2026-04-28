# CLAUDE.md — Market Pulse

Financial data platform: ingest equity prices + macro indicators → ADLS → PostgreSQL → dbt → dashboards. Pipeline runs on Airflow (Docker); infra is Terraform on Azure.

- Current phase status, infrastructure details, and code map: `CONTEXT.md`.
- Full project guide (architecture diagram, repo tree, Azure resources, schemas): `docs/project-guide.md`.

---

## Repo Structure

```
airflow/dags/         ingestion_dag.py, callbacks.py
airflow/              Dockerfile, docker-compose.yml, requirements.txt
ingestion/extractors/ yfinance.py, fred.py
ingestion/loaders/    adls.py
dbt/market_pulse/     models/{staging,marts}, profiles.yml, dbt_project.yml
validation/           raw_prices.py (raw GX), marts.py (mart GX)
infra/                Terraform (main.tf, variables.tf, outputs.tf)
```

Schemas: `raw` (Airflow) → `staging` (dbt) → `intermediate` (reserved) → `marts` (dbt).

---

## Environment Variables

Read from `.env` at Docker Compose startup. Never hardcode.

`AZURE_STORAGE_CONN_STR`, `AZURE_SQL_CONN_STR`, `FERNET_KEY`, `SLACK_WEBHOOK_URL`, `ALPHA_VANTAGE_API_KEY` (reserved).

---

## Coding Conventions

**Python**
- `from __future__ import annotations`, type hints on all signatures.
- `logging.getLogger(__name__)`, never `print`.
- Raw `psycopg2` for the loader (no ORM); dbt handles all transformation SQL.
- Airflow tasks: `@task` decorator (TaskFlow API). No classic operators.
- Secrets: `os.environ` only.

**SQL**
- Lowercase keywords, snake_case identifiers.
- CTEs for all non-trivial logic.
- `numeric(18, 6)` for all numeric columns.
- Window functions: explicit `rows between N preceding and current row`.
- Rows with fewer than N observations in a rolling window are excluded (not zero-filled).

---

## dbt Conventions

- **staging** — views, named `stg_<source>`. Cast types explicitly, filter nulls and bad data.
- **intermediate** — views, named `int_<description>`, for multi-source joins.
- **marts** — tables, named `fct_<metric>` (facts) or `dim_<entity>` (dimensions).

---

## Validation Conventions

- Great Expectations runs in **ephemeral mode** — no `great_expectations.yml` needed.
- Each validation function raises `RuntimeError` on failure so Airflow marks the task failed.
- Validation modules live in `validation/` and are called from Airflow tasks via `sys.path` injection.
- `raw_prices.py` validates ingestion output; `marts.py` validates transformed output.

---

## Key Constants

| Name | Value | Location |
|------|-------|----------|
| `TICKERS` | `AAPL, MSFT, GOOGL, SPY, BTC-USD` | `ingestion_dag.py` |
| `ANOMALY_THRESHOLD` | `0.10` | `ingestion_dag.py` |
| Volatility / correlation window | 30 trading days | `fct_volatility.sql`, `fct_correlations.sql` |
| DAG schedule | `0 1 * * 2-6` (Tue–Sat 01:00 UTC) | `ingestion_dag.py` |

---

## Running Locally

```bash
# Airflow stack
cd airflow
docker compose up airflow-init   # first time only
docker compose up -d

# dbt
cd dbt/market_pulse
dbt deps && dbt run && dbt test
```
