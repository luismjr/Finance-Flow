"""
ELT ingestion layer — pulls raw data from public financial APIs.

Equivalent to the Airflow-orchestrated ingestion that writes to GCS in production;
here we write directly to PostgreSQL for the local/demo environment.
"""
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import requests
import yfinance as yf
from django.conf import settings
from django.utils import timezone

from pipeline.models import CompanyInfo, MacroIndicator, PipelineRun, StockPrice

logger = logging.getLogger(__name__)

COMPANY_METADATA = {
    "AAPL": ("Apple Inc.", "Technology", "Consumer Electronics"),
    "MSFT": ("Microsoft Corporation", "Technology", "Software—Infrastructure"),
    "GOOGL": ("Alphabet Inc.", "Technology", "Internet Content & Information"),
    "AMZN": ("Amazon.com Inc.", "Consumer Discretionary", "Internet Retail"),
    "NVDA": ("NVIDIA Corporation", "Technology", "Semiconductors"),
    "META": ("Meta Platforms Inc.", "Technology", "Internet Content & Information"),
    "TSLA": ("Tesla Inc.", "Consumer Discretionary", "Auto Manufacturers"),
    "AVGO": ("Broadcom Inc.", "Technology", "Semiconductors"),
    "ORCL": ("Oracle Corporation", "Technology", "Software—Infrastructure"),
    "ADBE": ("Adobe Inc.", "Technology", "Software—Application"),
    "JPM": ("JPMorgan Chase & Co.", "Financials", "Banks—Diversified"),
    "BAC": ("Bank of America Corp.", "Financials", "Banks—Diversified"),
    "WFC": ("Wells Fargo & Company", "Financials", "Banks—Diversified"),
    "GS": ("Goldman Sachs Group Inc.", "Financials", "Capital Markets"),
    "MS": ("Morgan Stanley", "Financials", "Capital Markets"),
    "BLK": ("BlackRock Inc.", "Financials", "Asset Management"),
    "SCHW": ("Charles Schwab Corp.", "Financials", "Capital Markets"),
    "AXP": ("American Express Company", "Financials", "Credit Services"),
    "USB": ("U.S. Bancorp", "Financials", "Banks—Diversified"),
    "PNC": ("PNC Financial Services", "Financials", "Banks—Diversified"),
    "UNH": ("UnitedHealth Group Inc.", "Healthcare", "Healthcare Plans"),
    "JNJ": ("Johnson & Johnson", "Healthcare", "Drug Manufacturers—General"),
    "LLY": ("Eli Lilly and Company", "Healthcare", "Drug Manufacturers—General"),
    "ABBV": ("AbbVie Inc.", "Healthcare", "Drug Manufacturers—General"),
    "MRK": ("Merck & Co. Inc.", "Healthcare", "Drug Manufacturers—General"),
    "TMO": ("Thermo Fisher Scientific", "Healthcare", "Diagnostics & Research"),
    "ABT": ("Abbott Laboratories", "Healthcare", "Medical Devices"),
    "DHR": ("Danaher Corporation", "Healthcare", "Diagnostics & Research"),
    "BMY": ("Bristol-Myers Squibb", "Healthcare", "Drug Manufacturers—General"),
    "AMGN": ("Amgen Inc.", "Healthcare", "Drug Manufacturers—General"),
    "MCD": ("McDonald's Corporation", "Consumer Discretionary", "Restaurants"),
    "NKE": ("NIKE Inc.", "Consumer Discretionary", "Footwear & Accessories"),
    "SBUX": ("Starbucks Corporation", "Consumer Discretionary", "Restaurants"),
    "TGT": ("Target Corporation", "Consumer Discretionary", "Discount Stores"),
    "HD": ("Home Depot Inc.", "Consumer Discretionary", "Home Improvement Retail"),
    "LOW": ("Lowe's Companies Inc.", "Consumer Discretionary", "Home Improvement Retail"),
    "BKNG": ("Booking Holdings Inc.", "Consumer Discretionary", "Travel Services"),
    "CMG": ("Chipotle Mexican Grill", "Consumer Discretionary", "Restaurants"),
    "ABNB": ("Airbnb Inc.", "Consumer Discretionary", "Travel Services"),
    "ETSY": ("Etsy Inc.", "Consumer Discretionary", "Internet Retail"),
    "XOM": ("Exxon Mobil Corporation", "Energy", "Oil & Gas Integrated"),
    "CVX": ("Chevron Corporation", "Energy", "Oil & Gas Integrated"),
    "COP": ("ConocoPhillips", "Energy", "Oil & Gas E&P"),
    "EOG": ("EOG Resources Inc.", "Energy", "Oil & Gas E&P"),
    "SLB": ("SLB (Schlumberger)", "Energy", "Oil & Gas Equipment & Services"),
    "PSX": ("Phillips 66", "Energy", "Oil & Gas Refining & Marketing"),
    "VLO": ("Valero Energy Corporation", "Energy", "Oil & Gas Refining & Marketing"),
    "MPC": ("Marathon Petroleum Corp.", "Energy", "Oil & Gas Refining & Marketing"),
    "OXY": ("Occidental Petroleum", "Energy", "Oil & Gas E&P"),
    "HES": ("Hess Corporation", "Energy", "Oil & Gas E&P"),
}


def ensure_company_info():
    for ticker, (name, sector, industry) in COMPANY_METADATA.items():
        CompanyInfo.objects.update_or_create(
            ticker=ticker,
            defaults={"name": name, "sector": sector, "industry": industry},
        )


def ingest_stock_prices(tickers: list[str], start_date: date, end_date: date) -> dict:
    """Ingest OHLCV data from yfinance. Returns ingestion stats."""
    ensure_company_info()

    records_created = 0
    records_skipped = 0
    tickers_ok = 0
    tickers_failed = 0

    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
            if df.empty:
                logger.warning("No data for %s", ticker)
                tickers_failed += 1
                continue

            df = df.reset_index()
            batch = []
            for _, row in df.iterrows():
                batch.append(
                    StockPrice(
                        ticker=ticker,
                        date=row["Date"].date() if hasattr(row["Date"], "date") else row["Date"],
                        open_price=Decimal(str(round(float(row["Open"]), 4))),
                        high_price=Decimal(str(round(float(row["High"]), 4))),
                        low_price=Decimal(str(round(float(row["Low"]), 4))),
                        close_price=Decimal(str(round(float(row["Close"]), 4))),
                        adj_close=Decimal(str(round(float(row["Close"]), 4))),
                        volume=int(row["Volume"]),
                    )
                )

            created = StockPrice.objects.bulk_create(batch, ignore_conflicts=True)
            records_created += len(created)
            tickers_ok += 1
            logger.info("Ingested %d rows for %s", len(batch), ticker)

        except Exception as exc:
            logger.error("Failed to ingest %s: %s", ticker, exc)
            tickers_failed += 1

    return {
        "records_created": records_created,
        "records_skipped": records_skipped,
        "tickers_ok": tickers_ok,
        "tickers_failed": tickers_failed,
    }


def ingest_macro_indicators(series_map: dict[str, str], start_date: date) -> dict:
    """Ingest macroeconomic indicators from FRED API."""
    api_key = settings.FRED_API_KEY
    records_created = 0
    series_ok = 0
    series_failed = 0

    for series_id, name in series_map.items():
        try:
            if api_key:
                url = (
                    f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id={series_id}&api_key={api_key}"
                    f"&observation_start={start_date}&file_type=json"
                )
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                observations = resp.json().get("observations", [])
            else:
                # No API key — use synthetic demo data
                observations = _generate_mock_macro(series_id, start_date)

            batch = []
            for obs in observations:
                val = obs.get("value", ".")
                if val == "." or val is None:
                    continue
                batch.append(
                    MacroIndicator(
                        series_id=series_id,
                        name=name,
                        date=obs["date"],
                        value=Decimal(str(round(float(val), 6))),
                    )
                )

            created = MacroIndicator.objects.bulk_create(batch, ignore_conflicts=True)
            records_created += len(created)
            series_ok += 1

        except Exception as exc:
            logger.error("Failed to ingest %s: %s", series_id, exc)
            series_failed += 1

    return {
        "records_created": records_created,
        "series_ok": series_ok,
        "series_failed": series_failed,
    }


def _generate_mock_macro(series_id: str, start_date: date) -> list[dict]:
    """Generate plausible mock macro data when no FRED API key is provided."""
    import random
    import math

    base_values = {
        "FEDFUNDS": 5.25, "UNRATE": 3.9, "CPIAUCSL": 308.0, "GDP": 27000.0,
        "T10Y2Y": -0.3, "VIXCLS": 18.0, "DGS10": 4.2, "DCOILWTICO": 75.0,
        "DEXUSEU": 1.08, "M2SL": 20500.0, "UMCSENT": 72.0, "HOUST": 1400.0,
    }
    base = base_values.get(series_id, 100.0)
    observations = []
    current = start_date
    today = date.today()
    random.seed(hash(series_id) % 10000)

    while current <= today:
        noise = random.gauss(0, base * 0.005)
        drift = math.sin((current - start_date).days / 180) * base * 0.02
        val = round(base + noise + drift, 4)
        observations.append({"date": str(current), "value": str(val)})
        current += timedelta(days=30)

    return observations
