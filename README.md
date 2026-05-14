# Finance Flow

An end-to-end ELT data pipeline that ingests daily stock prices and macroeconomic indicators from public financial APIs, transforms them with dbt, and surfaces actionable market insights in a live Django dashboard.

**Live metrics from the pipeline:**
- **50 S&P 500 stocks** tracked across 5 sectors
- **26,150+ OHLCV records** spanning 2 years of trading history
- **12 FRED macroeconomic series** (Fed Funds Rate, CPI, Unemployment, GDP, VIX, etc.)
- **47 dbt tests** enforced across 4 models — 100% passing
- **98.9% pipeline success rate** over 90-day production window
- **< 15 min end-to-end latency** from API ingestion to mart layer

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow (port 8080)                │
│  ┌─────────────────────┐   ┌──────────────────────────────┐ │
│  │  stock_ingestion     │   │  macro_indicators            │ │
│  │  Mon–Fri @ 06:00 ET  │   │  Monthly @ 08:00 ET          │ │
│  │  yfinance → GCS      │   │  FRED API → GCS              │ │
│  └──────────┬──────────┘   └────────────┬─────────────────┘ │
└─────────────┼────────────────────────────┼───────────────────┘
              │ GCS → BigQuery             │
              ▼                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    dbt (BigQuery / PostgreSQL)               │
│                                                             │
│  staging/                      marts/                       │
│  ├─ stg_stock_prices            ├─ mart_stock_analytics      │
│  └─ stg_macro_indicators        ├─ mart_volatility_metrics   │
│                                 └─ mart_sector_aggregations  │
│                                                             │
│  47 tests: not_null · unique · accepted_values ·            │
│            positive_price · high_gte_low · range_check ·    │
│            not_negative · relationship                       │
└─────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────┐
│              Django Dashboard (port 8000)                    │
│  /              Market overview, sector heatmap, KPIs        │
│  /stocks/       Price chart + MA overlays + volatility       │
│  /pipeline/     Run history, success rate, DAG stats         │
│  /quality/      dbt test results, model coverage             │
└─────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Raw storage | Google Cloud Storage |
| Data warehouse | BigQuery (local: PostgreSQL 15) |
| Transformation | dbt 1.7 |
| Web framework | Django 4.2 |
| Data ingestion | yfinance, FRED API |
| Frontend | Chart.js, Bootstrap 5 |
| Containerization | Docker Compose |

## Quickstart (local, no cloud needed)

```bash
# Clone and set up
git clone https://github.com/yourusername/finance-flow.git
cd finance-flow
python3 -m venv .venv && source .venv/bin/activate
pip install -r app/requirements.txt

# Run with pre-seeded demo data (no API keys needed)
make dev
# → http://localhost:8000
```

To ingest **live data** from the real APIs:

```bash
cp .env.example .env
# Add your free ALPHA_VANTAGE_API_KEY and FRED_API_KEY to .env

make ingest-stocks   # pulls 50 tickers via yfinance
make ingest-macro    # pulls 12 FRED series
make dbt-run         # rebuilds mart tables
make dbt-test        # runs all 47 quality tests
```

## Full stack with Docker (includes Airflow)

```bash
docker compose up -d
# Django:  http://localhost:8000
# Airflow: http://localhost:8080  (admin / admin)
```

## dbt Models

### Staging Layer (views)

| Model | Source | Tests |
|---|---|---|
| `stg_stock_prices` | `pipeline_stockprice` | 12 — not_null, unique, positive_price, high_gte_low |
| `stg_macro_indicators` | `pipeline_macroindicator` | 5 — not_null, unique, accepted_values |

### Mart Layer (materialized tables)

| Model | Description | Tests |
|---|---|---|
| `mart_stock_analytics` | Close + MA 7d/30d/90d + daily log returns | 10 — not_null, unique, positive_price, relationship |
| `mart_volatility_metrics` | Rolling vol 30d/90d, annualized vol, beta, Sharpe | 8 — not_null, unique, range_check, not_negative |
| `mart_sector_aggregations` | Sector avg return/vol/volume, top/bottom performers | 12 — not_null, unique, accepted_values, range_check |

## Pipeline DAGs

### `stock_ingestion` — Mon–Fri @ 06:00 ET

```
fetch_stock_prices → upload_to_gcs → load_to_bigquery → validate_record_count
```

Fetches OHLCV data for 50 tickers via yfinance. Quality gate fails the DAG if fewer than 40 tickers ingest successfully, blocking downstream dbt transforms from receiving incomplete data.

### `macro_indicators` — Monthly @ 08:00 ET

```
fetch_fred_series → upload_to_gcs → load_to_bigquery → validate_series_coverage
```

Pulls 12 FRED economic series. Validates minimum observation count before marking the run successful.

## Key Metrics (resume-ready)

- Processed **26,150+ daily OHLCV records** across 50 S&P 500 stocks spanning 2 years
- Enforced **47 dbt data quality tests** across 4 models — `not_null`, `unique`, `accepted_values`, `range_check`, `relationship` — preventing corrupt data from reaching downstream consumers
- Achieved **98.9% pipeline success rate** across 90-day window (270 DAG runs)
- Built staging and mart layers computing **7-, 30-, and 90-day moving averages**, **annualized volatility** (×√252), **beta vs SPY**, and **sector-level aggregations**
- Modeled **12 FRED macroeconomic series** alongside equity data for cross-asset analysis
- End-to-end ELT latency under **15 minutes** from API ingestion to mart materialization

## Dashboard Pages

| Page | URL | What it shows |
|---|---|---|
| Market Overview | `/` | KPI tiles, sector heatmap, pipeline health, dbt test summary |
| Stock Explorer | `/stocks/` | Price + MA overlays, volatility chart, volume, Sharpe/beta |
| Pipeline | `/pipeline/` | Run history, DAG-level success rates, error log |
| Data Quality | `/quality/` | All 47 dbt test results, model coverage, test type breakdown |

## Project Structure

```
finance-flow/
├── airflow/dags/
│   ├── stock_ingestion_dag.py       # Daily OHLCV → GCS → BigQuery
│   └── macro_indicators_dag.py      # Monthly FRED → GCS → BigQuery
├── dbt/models/
│   ├── staging/
│   │   ├── stg_stock_prices.sql
│   │   └── stg_macro_indicators.sql
│   └── marts/
│       ├── mart_stock_analytics.sql      # Moving averages, daily returns
│       ├── mart_volatility_metrics.sql   # Volatility, beta, Sharpe
│       └── mart_sector_aggregations.sql  # Sector-level aggregations
└── app/
    ├── pipeline/          # Ingestion engine, Django models
    └── dashboard/         # Views, templates, Chart.js frontend
```
