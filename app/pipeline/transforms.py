"""
dbt-equivalent transformation layer.

In production, these transformations run as dbt SQL models against BigQuery.
Locally, this module applies the same logic in pandas and writes to the mart tables.
"""
import logging
from decimal import Decimal

import numpy as np
import pandas as pd
from django.db import connection
from django.utils import timezone

from pipeline.models import SectorAggregation, StockAnalytics, StockPrice, VolatilityMetric
from pipeline.ingestion import COMPANY_METADATA

logger = logging.getLogger(__name__)


def _sector_for(ticker: str) -> str:
    return COMPANY_METADATA.get(ticker, (None, "Unknown", None))[1]


def run_stock_analytics_mart(tickers: list[str] | None = None) -> int:
    """Build mart_stock_analytics — moving averages and daily returns."""
    if tickers is None:
        tickers = StockPrice.objects.values_list("ticker", flat=True).distinct()

    total_rows = 0
    for ticker in tickers:
        qs = StockPrice.objects.filter(ticker=ticker).order_by("date").values(
            "date", "close_price", "volume"
        )
        if not qs:
            continue

        df = pd.DataFrame(qs)
        df["close_price"] = df["close_price"].astype(float)
        df = df.sort_values("date").reset_index(drop=True)

        df["daily_return"] = df["close_price"].pct_change()
        df["ma_7d"] = df["close_price"].rolling(7, min_periods=1).mean()
        df["ma_30d"] = df["close_price"].rolling(30, min_periods=1).mean()
        df["ma_90d"] = df["close_price"].rolling(90, min_periods=1).mean()

        rows = []
        for _, row in df.iterrows():
            rows.append(
                StockAnalytics(
                    ticker=ticker,
                    date=row["date"],
                    close_price=Decimal(str(round(row["close_price"], 4))),
                    volume=int(row["volume"]) if row["volume"] else None,
                    daily_return=_safe_decimal(row["daily_return"], 6),
                    ma_7d=_safe_decimal(row["ma_7d"], 4),
                    ma_30d=_safe_decimal(row["ma_30d"], 4),
                    ma_90d=_safe_decimal(row["ma_90d"], 4),
                )
            )

        StockAnalytics.objects.filter(ticker=ticker).delete()
        StockAnalytics.objects.bulk_create(rows)
        total_rows += len(rows)

    return total_rows


def run_volatility_mart(tickers: list[str] | None = None) -> int:
    """Build mart_volatility_metrics — rolling volatility and beta."""
    if tickers is None:
        tickers = list(StockAnalytics.objects.values_list("ticker", flat=True).distinct())

    # Use SPY as market benchmark for beta (if available, else skip beta)
    spy_returns = _get_returns("SPY")

    total_rows = 0
    for ticker in tickers:
        qs = StockAnalytics.objects.filter(ticker=ticker).order_by("date").values(
            "date", "daily_return"
        )
        if not qs:
            continue

        df = pd.DataFrame(qs)
        df["daily_return"] = df["daily_return"].astype(float)
        df = df.sort_values("date").reset_index(drop=True)

        df["rolling_vol_30d"] = df["daily_return"].rolling(30, min_periods=5).std()
        df["rolling_vol_90d"] = df["daily_return"].rolling(90, min_periods=20).std()
        df["annualized_vol"] = df["rolling_vol_30d"] * np.sqrt(252)
        df["sharpe_30d"] = (
            df["daily_return"].rolling(30, min_periods=5).mean() /
            df["rolling_vol_30d"].replace(0, np.nan)
        ) * np.sqrt(252)

        # Beta calculation against market (SPY)
        df["beta"] = np.nan
        if spy_returns is not None and len(spy_returns) > 0:
            merged = df.merge(spy_returns.rename("spy_return"), left_on="date", right_index=True, how="left")
            rolling_cov = merged["daily_return"].rolling(90, min_periods=20).cov(merged["spy_return"])
            rolling_var = merged["spy_return"].rolling(90, min_periods=20).var()
            df["beta"] = (rolling_cov / rolling_var.replace(0, np.nan)).values

        rows = []
        for _, row in df.iterrows():
            rows.append(
                VolatilityMetric(
                    ticker=ticker,
                    date=row["date"],
                    rolling_vol_30d=_safe_decimal(row["rolling_vol_30d"], 6),
                    rolling_vol_90d=_safe_decimal(row["rolling_vol_90d"], 6),
                    annualized_vol=_safe_decimal(row["annualized_vol"], 6),
                    beta=_safe_decimal(row.get("beta"), 4),
                    sharpe_30d=_safe_decimal(row["sharpe_30d"], 4),
                )
            )

        VolatilityMetric.objects.filter(ticker=ticker).delete()
        VolatilityMetric.objects.bulk_create(rows)
        total_rows += len(rows)

    return total_rows


def run_sector_aggregation_mart() -> int:
    """Build mart_sector_aggregations — sector-level daily stats."""
    all_tickers = list(StockAnalytics.objects.values_list("ticker", flat=True).distinct())
    ticker_sector = {t: _sector_for(t) for t in all_tickers}

    qs = StockAnalytics.objects.all().values("ticker", "date", "daily_return", "volume")
    if not qs:
        return 0

    df = pd.DataFrame(qs)
    df["daily_return"] = df["daily_return"].astype(float)
    df["sector"] = df["ticker"].map(ticker_sector)
    df = df.dropna(subset=["sector"])

    vol_qs = VolatilityMetric.objects.all().values("ticker", "date", "annualized_vol")
    if vol_qs:
        vol_df = pd.DataFrame(vol_qs)
        vol_df["annualized_vol"] = vol_df["annualized_vol"].astype(float)
        df = df.merge(vol_df, on=["ticker", "date"], how="left")
    else:
        df["annualized_vol"] = np.nan

    rows = []
    for (sector, date_val), group in df.groupby(["sector", "date"]):
        if len(group) == 0:
            continue
        top = group.nlargest(1, "daily_return")["ticker"].values
        bottom = group.nsmallest(1, "daily_return")["ticker"].values
        rows.append(
            SectorAggregation(
                sector=sector,
                date=date_val,
                avg_return=_safe_decimal(group["daily_return"].mean(), 6),
                avg_volatility=_safe_decimal(group["annualized_vol"].mean(), 6) if "annualized_vol" in group else None,
                total_volume=int(group["volume"].sum()) if "volume" in group else None,
                num_stocks=len(group),
                top_performer=top[0] if len(top) > 0 else "",
                bottom_performer=bottom[0] if len(bottom) > 0 else "",
            )
        )

    SectorAggregation.objects.all().delete()
    SectorAggregation.objects.bulk_create(rows)
    return len(rows)


def _get_returns(ticker: str) -> pd.Series | None:
    qs = StockAnalytics.objects.filter(ticker=ticker).order_by("date").values("date", "daily_return")
    if not qs:
        return None
    df = pd.DataFrame(qs)
    df["daily_return"] = df["daily_return"].astype(float)
    return df.set_index("date")["daily_return"]


def _safe_decimal(val, places: int) -> Decimal | None:
    try:
        if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
            return None
        return Decimal(str(round(float(val), places)))
    except Exception:
        return None
