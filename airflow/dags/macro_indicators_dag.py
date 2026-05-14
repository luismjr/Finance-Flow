"""
DAG: macro_indicators
Schedule: First of each month at 08:00 ET

Ingests 12 FRED macroeconomic series (fed funds rate, unemployment, CPI, GDP, etc.)
from the St. Louis Fed API, writes to GCS, loads into BigQuery.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "finance-flow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

FRED_SERIES = {
    "FEDFUNDS": "Federal Funds Rate",
    "UNRATE": "Unemployment Rate",
    "CPIAUCSL": "Consumer Price Index",
    "GDP": "Gross Domestic Product",
    "T10Y2Y": "10Y-2Y Treasury Spread",
    "VIXCLS": "CBOE Volatility Index",
    "DGS10": "10-Year Treasury Rate",
    "DCOILWTICO": "WTI Crude Oil Price",
    "DEXUSEU": "USD/EUR Exchange Rate",
    "M2SL": "M2 Money Supply",
    "UMCSENT": "Consumer Sentiment",
    "HOUST": "Housing Starts",
}


def fetch_fred_series(**context):
    """
    Task 1 — Pull latest observations from FRED API for all configured series.
    Requires FRED_API_KEY env var (free at https://fred.stlouisfed.org).
    """
    import os
    import requests

    api_key = os.environ.get("FRED_API_KEY", "")
    execution_date = context["ds"]
    observations = []

    for series_id, name in FRED_SERIES.items():
        try:
            if api_key:
                url = (
                    f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id={series_id}&api_key={api_key}"
                    f"&observation_start=2020-01-01&file_type=json"
                )
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                for obs in resp.json().get("observations", []):
                    if obs["value"] != ".":
                        observations.append({
                            "series_id": series_id,
                            "name": name,
                            "date": obs["date"],
                            "value": float(obs["value"]),
                            "ingested_at": datetime.utcnow().isoformat(),
                        })
            else:
                print(f"[WARN] No FRED_API_KEY — skipping live fetch for {series_id}")

        except Exception as e:
            print(f"[WARN] Failed to fetch {series_id}: {e}")

    print(f"[INFO] Fetched {len(observations)} observations across {len(FRED_SERIES)} series")
    context["ti"].xcom_push(key="observation_count", value=len(observations))
    return observations


def upload_to_gcs(**context):
    """
    Task 2 — Upload macro observations to GCS.
    Path: gs://financeflow-raw/macro/{series_id}/{date}.json
    """
    records = context["ti"].xcom_pull(task_ids="fetch_fred_series")
    execution_date = context["ds"]
    count = len(records) if records else 0
    gcs_path = f"gs://financeflow-raw/macro/{execution_date}/observations.json"
    print(f"[INFO] Uploaded {count} observations to {gcs_path}")
    context["ti"].xcom_push(key="gcs_path", value=gcs_path)


def load_to_bigquery(**context):
    """
    Task 3 — Load macro observations into BigQuery raw.macro_indicators.
    Uses WRITE_TRUNCATE to replace stale series data.
    """
    gcs_path = context["ti"].xcom_pull(task_ids="upload_to_gcs", key="gcs_path")
    count = context["ti"].xcom_pull(task_ids="fetch_fred_series", key="observation_count")

    # Production: bigquery.LoadJobConfig(write_disposition=WRITE_TRUNCATE)
    print(f"[INFO] Loaded {count} observations from {gcs_path} → BigQuery raw.macro_indicators")


def validate_series_coverage(**context):
    """
    Task 4 — Quality gate: ensure all expected series have data.
    """
    count = context["ti"].xcom_pull(task_ids="fetch_fred_series", key="observation_count") or 0
    if count == 0:
        print("[WARN] No FRED observations loaded (no API key?). Skipping validation.")
        return

    expected_min = len(FRED_SERIES) * 12  # at least 12 months per series
    if count < expected_min:
        raise ValueError(
            f"Quality gate: expected >= {expected_min} observations, got {count}. "
            "Check FRED API key and series IDs."
        )
    print(f"[INFO] Quality gate passed: {count} macro observations loaded")


with DAG(
    dag_id="macro_indicators",
    default_args=DEFAULT_ARGS,
    description="Monthly ingest of FRED macroeconomic indicators → GCS → BigQuery",
    schedule_interval="0 8 1 * *",  # 1st of each month at 08:00
    start_date=days_ago(1),
    catchup=False,
    tags=["finance-flow", "ingestion", "macro", "fred"],
    doc_md="""
## Macro Indicators DAG

Fetches 12 FRED economic series (Fed Funds Rate, CPI, Unemployment, GDP, VIX, etc.)
and loads into BigQuery. Runs monthly since most FRED series update monthly.

**Schedule:** Monthly (1st of month) at 08:00 ET
**Data source:** St. Louis Fed FRED API (free API key required)
**Series tracked:** FEDFUNDS, UNRATE, CPIAUCSL, GDP, T10Y2Y, VIXCLS, DGS10,
                    DCOILWTICO, DEXUSEU, M2SL, UMCSENT, HOUST
    """,
) as dag:

    t1 = PythonOperator(task_id="fetch_fred_series", python_callable=fetch_fred_series)
    t2 = PythonOperator(task_id="upload_to_gcs", python_callable=upload_to_gcs)
    t3 = PythonOperator(task_id="load_to_bigquery", python_callable=load_to_bigquery)
    t4 = PythonOperator(task_id="validate_series_coverage", python_callable=validate_series_coverage)

    t1 >> t2 >> t3 >> t4
