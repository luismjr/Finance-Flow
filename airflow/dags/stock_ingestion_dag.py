"""
DAG: stock_ingestion
Schedule: Daily at 06:00 ET (after market open data is available)

Ingests OHLCV data for 50 S&P 500 stocks from yfinance, writes raw JSON
to Google Cloud Storage, then loads into BigQuery raw layer.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "finance-flow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "ADBE",
    "JPM", "BAC", "WFC", "GS", "MS", "BLK", "SCHW", "AXP", "USB", "PNC",
    "UNH", "JNJ", "LLY", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY", "AMGN",
    "MCD", "NKE", "SBUX", "TGT", "HD", "LOW", "BKNG", "CMG", "ABNB", "ETSY",
    "XOM", "CVX", "COP", "EOG", "SLB", "PSX", "VLO", "MPC", "OXY", "HES",
]


def fetch_stock_prices(**context):
    """
    Task 1 — Fetch OHLCV data from yfinance for the previous trading day.
    In production: writes Parquet to GCS bucket gs://financeflow-raw/stocks/{date}/
    """
    import json
    import os
    from datetime import date

    import yfinance as yf

    execution_date = context["ds"]
    start = execution_date
    end = (datetime.strptime(execution_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    records = []
    failed = []

    for ticker in TICKERS:
        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if df.empty:
                failed.append(ticker)
                continue
            for idx, row in df.iterrows():
                records.append({
                    "ticker": ticker,
                    "date": str(idx.date()),
                    "open": round(float(row["Open"]), 4),
                    "high": round(float(row["High"]), 4),
                    "low": round(float(row["Low"]), 4),
                    "close": round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]),
                    "ingested_at": datetime.utcnow().isoformat(),
                })
        except Exception as e:
            failed.append(ticker)
            print(f"[WARN] Failed {ticker}: {e}")

    print(f"[INFO] Fetched {len(records)} records for {len(TICKERS) - len(failed)} tickers")
    if failed:
        print(f"[WARN] Failed tickers: {failed}")

    # Push record count to XCom for downstream tasks
    context["ti"].xcom_push(key="record_count", value=len(records))
    context["ti"].xcom_push(key="failed_tickers", value=failed)
    return records


def upload_to_gcs(**context):
    """
    Task 2 — Upload raw JSON to Google Cloud Storage.
    Path: gs://financeflow-raw/stocks/year=YYYY/month=MM/day=DD/prices.json
    """
    import json
    import os

    records = context["ti"].xcom_pull(task_ids="fetch_stock_prices")
    execution_date = context["ds"]

    # Production: use google.cloud.storage
    # bucket = storage.Client().bucket(os.environ["GCS_BUCKET_NAME"])
    # blob = bucket.blob(f"stocks/year={...}/month={...}/day={...}/prices.json")
    # blob.upload_from_string(json.dumps(records))

    # Local demo: log the upload
    count = len(records) if records else 0
    gcs_path = f"gs://financeflow-raw/stocks/{execution_date}/prices.json"
    print(f"[INFO] Uploaded {count} records to {gcs_path}")
    context["ti"].xcom_push(key="gcs_path", value=gcs_path)


def load_to_bigquery(**context):
    """
    Task 3 — Load from GCS into BigQuery raw.stock_prices table.
    Uses WRITE_APPEND to preserve full history.
    """
    gcs_path = context["ti"].xcom_pull(task_ids="upload_to_gcs", key="gcs_path")
    record_count = context["ti"].xcom_pull(task_ids="fetch_stock_prices", key="record_count")

    # Production: use google.cloud.bigquery
    # job_config = bigquery.LoadJobConfig(
    #     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    #     schema=[...],
    # )
    # client.load_table_from_uri(gcs_path, "financeflow.raw.stock_prices", job_config=job_config)

    print(f"[INFO] Loaded {record_count} rows from {gcs_path} → BigQuery raw.stock_prices")


def validate_record_count(**context):
    """
    Task 4 — Data quality gate: fail if < 40 tickers ingested successfully.
    Prevents partial data from propagating to dbt transformations.
    """
    failed = context["ti"].xcom_pull(task_ids="fetch_stock_prices", key="failed_tickers") or []
    success_count = len(TICKERS) - len(failed)

    if success_count < 40:
        raise ValueError(
            f"Quality gate failed: only {success_count}/{len(TICKERS)} tickers ingested. "
            f"Failed: {failed}. Downstream dbt transforms will NOT run."
        )

    print(f"[INFO] Quality gate passed: {success_count}/{len(TICKERS)} tickers ingested")


with DAG(
    dag_id="stock_ingestion",
    default_args=DEFAULT_ARGS,
    description="Daily ingest of S&P 500 OHLCV data → GCS → BigQuery",
    schedule_interval="0 6 * * 1-5",  # Mon–Fri at 06:00
    start_date=days_ago(1),
    catchup=False,
    tags=["finance-flow", "ingestion", "stocks"],
    doc_md="""
## Stock Ingestion DAG

Fetches daily OHLCV (Open, High, Low, Close, Volume) for 50 S&P 500 stocks
via yfinance and loads into BigQuery via GCS.

**Schedule:** Mon–Fri 06:00 ET
**SLA:** Data available by 06:30 ET
**Upstream:** yfinance (Yahoo Finance)
**Downstream:** `dbt_transform` DAG (triggers on success)
    """,
) as dag:

    t1 = PythonOperator(
        task_id="fetch_stock_prices",
        python_callable=fetch_stock_prices,
    )

    t2 = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    t3 = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    t4 = PythonOperator(
        task_id="validate_record_count",
        python_callable=validate_record_count,
    )

    t1 >> t2 >> t3 >> t4
