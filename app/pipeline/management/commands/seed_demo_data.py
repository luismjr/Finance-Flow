"""
Seed Finance Flow with realistic synthetic data — no API keys required.
Generates 2 years of OHLCV data for 50 stocks and 12 macro series.
"""
import math
import random
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
from django.core.management.base import BaseCommand
from django.utils import timezone

from pipeline.ingestion import COMPANY_METADATA
from pipeline.models import (
    CompanyInfo, DbtTestResult, MacroIndicator, PipelineRun,
    SectorAggregation, StockAnalytics, StockPrice, VolatilityMetric,
)
from pipeline.transforms import (
    run_sector_aggregation_mart,
    run_stock_analytics_mart,
    run_volatility_mart,
)


MACRO_SERIES = {
    "FEDFUNDS": ("Federal Funds Rate", "%", 5.25),
    "UNRATE": ("Unemployment Rate", "%", 3.9),
    "CPIAUCSL": ("Consumer Price Index", "Index", 308.0),
    "T10Y2Y": ("10Y-2Y Treasury Spread", "%", -0.3),
    "VIXCLS": ("CBOE Volatility Index", "Index", 18.0),
    "DGS10": ("10-Year Treasury Rate", "%", 4.2),
    "DCOILWTICO": ("WTI Crude Oil Price", "USD/bbl", 75.0),
    "DEXUSEU": ("USD/EUR Exchange Rate", "USD", 1.08),
    "M2SL": ("M2 Money Supply", "Billions USD", 20500.0),
    "UMCSENT": ("Consumer Sentiment", "Index", 72.0),
    "HOUST": ("Housing Starts", "Thousands", 1400.0),
    "GDP": ("Gross Domestic Product", "Billions USD", 27000.0),
}

BASE_PRICES = {
    "AAPL": 182.0, "MSFT": 374.0, "GOOGL": 140.0, "AMZN": 178.0, "NVDA": 495.0,
    "META": 353.0, "TSLA": 248.0, "AVGO": 620.0, "ORCL": 118.0, "ADBE": 601.0,
    "JPM": 197.0, "BAC": 35.0, "WFC": 56.0, "GS": 387.0, "MS": 92.0,
    "BLK": 800.0, "SCHW": 64.0, "AXP": 196.0, "USB": 43.0, "PNC": 157.0,
    "UNH": 525.0, "JNJ": 162.0, "LLY": 582.0, "ABBV": 170.0, "MRK": 127.0,
    "TMO": 560.0, "ABT": 108.0, "DHR": 251.0, "BMY": 52.0, "AMGN": 285.0,
    "MCD": 294.0, "NKE": 103.0, "SBUX": 94.0, "TGT": 138.0, "HD": 350.0,
    "LOW": 232.0, "BKNG": 3700.0, "CMG": 2242.0, "ABNB": 134.0, "ETSY": 71.0,
    "XOM": 109.0, "CVX": 158.0, "COP": 122.0, "EOG": 126.0, "SLB": 53.0,
    "PSX": 159.0, "VLO": 161.0, "MPC": 185.0, "OXY": 58.0, "HES": 149.0,
}

DBT_TESTS = [
    ("stg_stock_prices", "not_null", "ticker"),
    ("stg_stock_prices", "not_null", "date"),
    ("stg_stock_prices", "not_null", "close_price"),
    ("stg_stock_prices", "not_null", "volume"),
    ("stg_stock_prices", "accepted_values", "ticker"),
    ("stg_stock_prices", "unique", "ticker_date"),
    ("stg_stock_prices", "positive_price", "close_price"),
    ("stg_stock_prices", "positive_price", "open_price"),
    ("stg_stock_prices", "positive_price", "high_price"),
    ("stg_stock_prices", "positive_price", "low_price"),
    ("stg_stock_prices", "not_negative", "volume"),
    ("stg_stock_prices", "high_gte_low", "high_price"),
    ("stg_macro_indicators", "not_null", "series_id"),
    ("stg_macro_indicators", "not_null", "date"),
    ("stg_macro_indicators", "not_null", "value"),
    ("stg_macro_indicators", "unique", "series_id_date"),
    ("stg_macro_indicators", "accepted_values", "series_id"),
    ("mart_stock_analytics", "not_null", "ticker"),
    ("mart_stock_analytics", "not_null", "date"),
    ("mart_stock_analytics", "not_null", "close_price"),
    ("mart_stock_analytics", "not_null", "ma_7d"),
    ("mart_stock_analytics", "not_null", "ma_30d"),
    ("mart_stock_analytics", "not_null", "ma_90d"),
    ("mart_stock_analytics", "unique", "ticker_date"),
    ("mart_stock_analytics", "positive_price", "close_price"),
    ("mart_stock_analytics", "positive_price", "ma_7d"),
    ("mart_stock_analytics", "positive_price", "ma_30d"),
    ("mart_stock_analytics", "positive_price", "ma_90d"),
    ("mart_stock_analytics", "relationship", "ticker"),
    ("mart_volatility_metrics", "not_null", "ticker"),
    ("mart_volatility_metrics", "not_null", "date"),
    ("mart_volatility_metrics", "unique", "ticker_date"),
    ("mart_volatility_metrics", "not_negative", "rolling_vol_30d"),
    ("mart_volatility_metrics", "not_negative", "annualized_vol"),
    ("mart_volatility_metrics", "relationship", "ticker"),
    ("mart_volatility_metrics", "range_check", "beta"),
    ("mart_sector_aggregations", "not_null", "sector"),
    ("mart_sector_aggregations", "not_null", "date"),
    ("mart_sector_aggregations", "unique", "sector_date"),
    ("mart_sector_aggregations", "accepted_values", "sector"),
    ("mart_sector_aggregations", "not_null", "num_stocks"),
    ("mart_sector_aggregations", "positive_count", "num_stocks"),
    ("mart_sector_aggregations", "not_null", "avg_return"),
    ("mart_sector_aggregations", "range_check", "avg_return"),
    ("mart_sector_aggregations", "not_null", "avg_volatility"),
    ("mart_sector_aggregations", "not_negative", "total_volume"),
    ("mart_sector_aggregations", "relationship", "top_performer"),
]


class Command(BaseCommand):
    help = "Seed Finance Flow with 2 years of realistic synthetic financial data"

    def add_arguments(self, parser):
        parser.add_argument("--years", type=int, default=2)
        parser.add_argument("--clear", action="store_true", help="Clear existing data first")
        parser.add_argument("--skip-transforms", action="store_true")

    def handle(self, *args, **options):
        years = options["years"]
        self.stdout.write(self.style.MIGRATE_HEADING(
            f"\nSeeding Finance Flow — {years} years × {len(BASE_PRICES)} stocks × 12 macro series\n"
        ))

        if options["clear"]:
            self._clear_data()

        if StockPrice.objects.exists():
            self.stdout.write(self.style.WARNING("Data already seeded. Use --clear to reseed."))
        else:
            self._seed_companies()
            self._seed_stock_prices(years)
            self._seed_macro_indicators(years)

        if not options["skip_transforms"]:
            self._run_transforms()

        self._seed_pipeline_runs(years)
        self._seed_dbt_tests()

        self._print_summary()

    def _clear_data(self):
        self.stdout.write("Clearing existing data...")
        for model in [DbtTestResult, PipelineRun, SectorAggregation, VolatilityMetric,
                      StockAnalytics, MacroIndicator, StockPrice, CompanyInfo]:
            model.objects.all().delete()

    def _seed_companies(self):
        self.stdout.write("  Seeding company metadata...")
        for ticker, (name, sector, industry) in COMPANY_METADATA.items():
            CompanyInfo.objects.update_or_create(
                ticker=ticker,
                defaults={"name": name, "sector": sector, "industry": industry},
            )

    def _seed_stock_prices(self, years: int):
        self.stdout.write("  Generating stock price time series...")
        end_date = date.today()
        start_date = end_date.replace(year=end_date.year - years)

        trading_days = self._business_days(start_date, end_date)
        total = len(BASE_PRICES) * len(trading_days)
        self.stdout.write(f"    → {len(BASE_PRICES)} stocks × {len(trading_days)} trading days = {total:,} records")

        batch = []
        for ticker, base_price in BASE_PRICES.items():
            random.seed(hash(ticker) % 99999)
            np.random.seed(hash(ticker) % 99999)

            prices = self._gbm_prices(base_price, len(trading_days))

            for i, day in enumerate(trading_days):
                close = prices[i]
                daily_range = close * random.uniform(0.005, 0.025)
                open_p = close * random.uniform(0.995, 1.005)
                high = close + daily_range * random.uniform(0.3, 1.0)
                low = close - daily_range * random.uniform(0.3, 1.0)
                vol = int(random.gauss(15_000_000, 5_000_000) * (base_price / 100) ** -0.3)
                vol = max(vol, 100_000)

                batch.append(StockPrice(
                    ticker=ticker,
                    date=day,
                    open_price=Decimal(str(round(max(open_p, 0.01), 4))),
                    high_price=Decimal(str(round(max(high, close), 4))),
                    low_price=Decimal(str(round(max(min(low, close), 0.01), 4))),
                    close_price=Decimal(str(round(close, 4))),
                    adj_close=Decimal(str(round(close, 4))),
                    volume=vol,
                ))

            if len(batch) >= 5000:
                StockPrice.objects.bulk_create(batch, ignore_conflicts=True)
                batch = []

        if batch:
            StockPrice.objects.bulk_create(batch, ignore_conflicts=True)

        self.stdout.write(self.style.SUCCESS(f"    ✓ {StockPrice.objects.count():,} stock price records"))

    def _seed_macro_indicators(self, years: int):
        self.stdout.write("  Generating macroeconomic indicator series...")
        end_date = date.today()
        start_date = end_date.replace(year=end_date.year - years)

        batch = []
        for series_id, (name, unit, base_val) in MACRO_SERIES.items():
            random.seed(hash(series_id) % 9999)
            current = start_date.replace(day=1)

            while current <= end_date:
                t = (current - start_date).days / 365.0
                noise = random.gauss(0, base_val * 0.008)
                cycle = math.sin(t * math.pi * 2) * base_val * 0.03
                trend = t * base_val * 0.01
                val = base_val + noise + cycle + trend

                batch.append(MacroIndicator(
                    series_id=series_id,
                    name=name,
                    date=current,
                    value=Decimal(str(round(val, 4))),
                    unit=unit,
                ))
                # Next month
                month = current.month + 1
                year = current.year + (month > 12)
                current = current.replace(year=year, month=((month - 1) % 12) + 1, day=1)

        MacroIndicator.objects.bulk_create(batch, ignore_conflicts=True)
        self.stdout.write(self.style.SUCCESS(f"    ✓ {MacroIndicator.objects.count():,} macro indicator records"))

    def _run_transforms(self):
        self.stdout.write("  Running dbt-equivalent transformations...")
        tickers = list(BASE_PRICES.keys())

        n = run_stock_analytics_mart(tickers)
        self.stdout.write(f"    ✓ mart_stock_analytics: {n:,} rows")

        n = run_volatility_mart(tickers)
        self.stdout.write(f"    ✓ mart_volatility_metrics: {n:,} rows")

        n = run_sector_aggregation_mart()
        self.stdout.write(f"    ✓ mart_sector_aggregations: {n:,} rows")

    def _seed_pipeline_runs(self, years: int):
        self.stdout.write("  Seeding pipeline run history (90 days)...")
        if PipelineRun.objects.exists():
            return

        from datetime import datetime
        import uuid

        runs = []
        today = date.today()
        # 90 days of daily runs — simulate 98.7% success rate
        fail_days = set(random.sample(range(90), 2))

        for i in range(90):
            run_date = today - timedelta(days=90 - i)
            failed = i in fail_days

            for dag_id, records, duration in [
                (PipelineRun.DAG_STOCK_INGESTION, random.randint(2400, 2600), random.uniform(45, 120)),
                (PipelineRun.DAG_MACRO_INGESTION, random.randint(50, 80), random.uniform(8, 25)),
                (PipelineRun.DAG_DBT_TRANSFORM, random.randint(125000, 135000), random.uniform(60, 180)),
            ]:
                started = timezone.make_aware(datetime.combine(run_date, datetime.min.time()).replace(hour=6))
                completed = started + timedelta(seconds=duration) if not (failed and dag_id == PipelineRun.DAG_STOCK_INGESTION) else None
                status = PipelineRun.STATUS_FAILED if (failed and dag_id == PipelineRun.DAG_STOCK_INGESTION) else PipelineRun.STATUS_SUCCESS

                runs.append(PipelineRun(
                    dag_id=dag_id,
                    run_id=f"scheduled__{run_date.isoformat()}T06:00:00+00:00_{dag_id[:4]}",
                    status=status,
                    started_at=started,
                    completed_at=completed,
                    records_ingested=records if dag_id != PipelineRun.DAG_DBT_TRANSFORM else 0,
                    records_transformed=records if dag_id == PipelineRun.DAG_DBT_TRANSFORM else 0,
                    tickers_processed=50 if dag_id == PipelineRun.DAG_STOCK_INGESTION else 0,
                    duration_seconds=duration if completed else None,
                    error_message="APIError: yfinance rate limit exceeded (429)" if status == PipelineRun.STATUS_FAILED else "",
                ))

        PipelineRun.objects.bulk_create(runs)
        self.stdout.write(self.style.SUCCESS(f"    ✓ {PipelineRun.objects.count()} pipeline run records"))

    def _seed_dbt_tests(self):
        self.stdout.write("  Seeding dbt test results...")
        if DbtTestResult.objects.exists():
            return

        latest_run = PipelineRun.objects.filter(
            dag_id=PipelineRun.DAG_DBT_TRANSFORM, status=PipelineRun.STATUS_SUCCESS
        ).first()

        batch = []
        for model, test_name, column in DBT_TESTS:
            batch.append(DbtTestResult(
                pipeline_run=latest_run,
                run_at=latest_run.started_at if latest_run else timezone.now(),
                model_name=model,
                test_name=test_name,
                column_name=column,
                test_type="generic" if test_name in ("not_null", "unique", "accepted_values") else "singular",
                status=DbtTestResult.STATUS_PASS,
                failures=0,
                execution_time_ms=random.randint(12, 340),
            ))

        DbtTestResult.objects.bulk_create(batch)
        self.stdout.write(self.style.SUCCESS(f"    ✓ {DbtTestResult.objects.count()} dbt test records"))

    def _print_summary(self):
        total_stock = StockPrice.objects.count()
        total_macro = MacroIndicator.objects.count()
        total_analytics = StockAnalytics.objects.count()
        total_vol = VolatilityMetric.objects.count()
        total_sector = SectorAggregation.objects.count()
        runs = PipelineRun.objects.count()
        tests = DbtTestResult.objects.count()
        success_runs = PipelineRun.objects.filter(status=PipelineRun.STATUS_SUCCESS).count()
        rate = round(success_runs / runs * 100, 1) if runs else 0

        self.stdout.write("\n" + "─" * 58)
        self.stdout.write(self.style.SUCCESS("  Finance Flow seeded successfully!\n"))
        self.stdout.write(f"  {'Stock price records:':<35} {total_stock:>10,}")
        self.stdout.write(f"  {'Macro indicator records:':<35} {total_macro:>10,}")
        self.stdout.write(f"  {'mart_stock_analytics rows:':<35} {total_analytics:>10,}")
        self.stdout.write(f"  {'mart_volatility_metrics rows:':<35} {total_vol:>10,}")
        self.stdout.write(f"  {'mart_sector_aggregations rows:':<35} {total_sector:>10,}")
        self.stdout.write(f"  {'Pipeline runs (90 days):':<35} {runs:>10,}")
        self.stdout.write(f"  {'Pipeline success rate:':<35} {rate:>9.1f}%")
        self.stdout.write(f"  {'dbt tests passing:':<35} {tests:>10,}")
        self.stdout.write("─" * 58 + "\n")
        self.stdout.write("  Run: python manage.py runserver\n")

    @staticmethod
    def _business_days(start: date, end: date) -> list[date]:
        days = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                days.append(current)
            current += timedelta(days=1)
        return days

    @staticmethod
    def _gbm_prices(s0: float, n: int) -> list[float]:
        """Geometric Brownian Motion price simulation."""
        mu = 0.0003
        sigma = 0.018
        dt = 1
        prices = [s0]
        for _ in range(n - 1):
            shock = np.random.normal(mu * dt, sigma * math.sqrt(dt))
            prices.append(max(prices[-1] * math.exp(shock), 0.01))
        return prices
