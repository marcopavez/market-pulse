# Market Pulse

A financial data platform that collects equity prices and macroeconomic indicators, transforms them into analytical datasets, and surfaces insights through dashboards.

---

## Objectives

- **Automate data ingestion** from Yahoo Finance (equity prices) and FRED (macro indicators) on a daily schedule.
- **Store raw data** in Cloudflare R2 as Parquet files and load it into Neon Postgres.
- **Transform and model** raw data into reusable analytical fact tables using dbt (returns, volatility, correlations, macro overlay).
- **Validate data quality** at every layer using Great Expectations, with Airflow tasks failing loudly on bad data.
- **Alert on anomalies** — any ticker moving more than 10% in a single session triggers a Slack notification.
- **Visualize** the results via Grafana (operational) and Power BI (reporting).

---

## Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2 (Docker, LocalExecutor) |
| Ingestion | Python · `yfinance` · `curl_cffi` · `httpx` |
| Object Storage | Cloudflare R2 (S3-compatible, free tier) |
| Database | Neon Postgres (serverless, free tier) |
| Transformation | dbt Core |
| Validation | Great Expectations ≥ 1.0 |
| Infrastructure | Terraform (Neon + Cloudflare providers) |
| Dashboards | Grafana · Power BI _(Phase 3)_ |
| Alerting | Slack webhook |

---

## Folder Structure

```
market-pulse/
├── airflow/
│   ├── dags/
│   │   ├── ingestion_dag.py     # Main pipeline DAG
│   │   └── callbacks.py         # Failure callbacks and Slack alerts
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
├── ingestion/
│   ├── extractors/
│   │   ├── yfinance.py          # Yahoo Finance price fetcher (curl_cffi session)
│   │   └── fred.py              # FRED macroeconomic data extractor
│   └── loaders/
│       └── r2.py                # Parquet upload/download to Cloudflare R2 (boto3)
├── dbt/market_pulse/
│   ├── models/
│   │   ├── staging/             # Cleaned, typed views over raw tables
│   │   │   ├── stg_prices.sql
│   │   │   └── stg_macro.sql
│   │   ├── intermediate/        # Multi-source join models (reserved)
│   │   └── marts/               # Analytical fact tables
│   │       ├── fct_daily_returns.sql
│   │       ├── fct_volatility.sql
│   │       ├── fct_correlations.sql
│   │       └── fct_macro_overlay.sql
│   ├── macros/
│   ├── tests/
│   ├── seeds/
│   └── dbt_project.yml
├── validation/
│   ├── raw_prices.py            # GX suite: raw.raw_prices
│   └── marts.py                 # GX suites: fct_daily_returns, fct_volatility
├── infra/
│   ├── main.tf                  # Neon project + Cloudflare R2 bucket
│   ├── variables.tf
│   └── outputs.tf
├── CLAUDE.md                    # AI assistant context and conventions
├── CONTEXT.md                   # Project state snapshot
└── README.md
```

---

## Data Pipeline

```
Yahoo Finance ──┐
                ├──► Cloudflare R2 (raw Parquet)
FRED API ───────┘         │
                          ▼
                  Neon Postgres raw.*
                          │
                        dbt
                          │
              ┌───────────┴────────────┐
          staging.*               marts.*
         (typed views)        (fact tables)
                          │
              ┌───────────┴────────────┐
           Grafana               Power BI
```

**DAG schedule:** `0 1 * * 2-6` — runs at 01:00 UTC Tuesday through Saturday (covering Mon–Fri market sessions).

---

## Analytical Models

| Model | Description |
|-------|-------------|
| `stg_prices` | Typed + filtered view over `raw.raw_prices` |
| `stg_macro` | Typed + filtered view over `raw.raw_macro` |
| `fct_daily_returns` | Daily log returns and percent returns per ticker |
| `fct_volatility` | Rolling 30-day annualized volatility (√252 factor) |
| `fct_correlations` | Rolling 30-day pairwise return correlations (C(5,2) = 10 pairs) |
| `fct_macro_overlay` | Price + returns joined with forward-filled macro indicators |

---

## Tracked Assets

| Ticker | Description |
|--------|-------------|
| `AAPL` | Apple Inc. |
| `MSFT` | Microsoft Corp. |
| `GOOGL` | Alphabet Inc. |
| `SPY` | S&P 500 ETF |
| `BTC-USD` | Bitcoin / USD |

**Macro series (FRED):** `FEDFUNDS`, `CPIAUCSL`, `T10Y2Y`, `UNRATE`, `GDP`, `DGS10`

---

## Conventions

### Python
- `from __future__ import annotations` in every module.
- Type hints on all function signatures.
- `logging.getLogger(__name__)` — no `print` statements.
- Secrets always from `os.environ`, never hardcoded.

### SQL (dbt)
- Lowercase keywords, `snake_case` identifiers.
- All non-trivial logic in CTEs.
- Explicit casts: `numeric(18, 6)` for prices, `bigint` for volume.
- Rolling windows: `rows between N preceding and current row`; rows with fewer than N observations are excluded.

### Airflow
- TaskFlow API (`@task` decorator) — no classic operators.
- `on_failure_callback` on all tasks via `default_args`.
- Validation tasks raise `RuntimeError` to surface failures as task failures.

### Validation (Great Expectations)
- Ephemeral mode — no file-based GX project.
- One validation function per table, returning `bool` or raising `RuntimeError`.

---

## Local Setup

### Prerequisites

- Docker Desktop
- Python 3.11+
- Terraform CLI
- A Neon account (https://neon.tech) and a Cloudflare account (https://dash.cloudflare.com)

### Running Airflow

```bash
cp .env.example airflow/.env   # fill in Neon + R2 credentials
cd airflow
docker compose up airflow-init   # first time only
docker compose up -d
# Airflow UI → http://localhost:8080  (admin / admin)
```

### Running dbt

```bash
cd dbt/market_pulse
dbt deps
dbt run
dbt test
```

### Provisioning Infrastructure

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # fill in API tokens
terraform init
terraform plan
terraform apply
```

---

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `POSTGRES_URL` | Neon Postgres connection string (`postgresql://user:pass@host.neon.tech/db?sslmode=require`) |
| `R2_ACCOUNT_ID` | Cloudflare account ID (used to build the R2 endpoint URL) |
| `R2_ACCESS_KEY_ID` | R2 S3-compatible access key |
| `R2_SECRET_ACCESS_KEY` | R2 S3-compatible secret |
| `R2_BUCKET_RAW` | R2 bucket for raw Parquet landing (default `market-pulse-raw`) |
| `FERNET_KEY` | Airflow encryption key |
| `FRED_API_KEY` | FRED API key (optional; required for macro extraction) |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook (optional — alerts suppressed if unset) |

---

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| 1 — Ingestion | ✅ Done | Extractors, R2 loader, Airflow DAG, Terraform infra |
| 2 — Transform & Validate | ✅ Done | dbt models, Great Expectations, anomaly alerts, FRED DAG task |
| 3 — Dashboards | ⏳ Pending | Grafana + Power BI |
| 4 — Quality & Demo | ⏳ Pending | Unit tests, CI/CD, full README demo |
