import json
from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Avg, Count, Max, Min, Q, Sum
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.views.decorators.http import require_GET

from pipeline.models import (
    CompanyInfo, DbtTestResult, MacroIndicator, PipelineRun,
    SectorAggregation, StockAnalytics, StockPrice, VolatilityMetric,
)


def index(request):
    """Main dashboard — KPIs, sector heatmap, pipeline status."""
    today = date.today()
    thirty_days_ago = today - timedelta(days=30)

    # KPI metrics
    total_stocks = CompanyInfo.objects.count()
    total_records = StockPrice.objects.count()
    total_runs = PipelineRun.objects.filter(started_at__date__gte=thirty_days_ago).count()
    success_runs = PipelineRun.objects.filter(
        started_at__date__gte=thirty_days_ago, status=PipelineRun.STATUS_SUCCESS
    ).count()
    success_rate = round(success_runs / total_runs * 100, 1) if total_runs else 0

    dbt_total = DbtTestResult.objects.count()
    dbt_pass = DbtTestResult.objects.filter(status=DbtTestResult.STATUS_PASS).count()
    dbt_rate = round(dbt_pass / dbt_total * 100, 1) if dbt_total else 0

    last_run = PipelineRun.objects.filter(status=PipelineRun.STATUS_SUCCESS).first()
    data_freshness = None
    if last_run:
        hours_ago = (timezone.now() - last_run.started_at).total_seconds() / 3600
        data_freshness = f"{int(hours_ago)}h ago" if hours_ago < 24 else f"{int(hours_ago / 24)}d ago"

    macro_count = MacroIndicator.objects.values("series_id").distinct().count()

    # Sector performance (last 30 days avg return)
    sector_data = (
        SectorAggregation.objects.filter(date__gte=thirty_days_ago)
        .values("sector")
        .annotate(
            avg_ret=Avg("avg_return"),
            avg_vol=Avg("avg_volatility"),
            total_vol=Sum("total_volume"),
        )
        .order_by("-avg_ret")
    )

    # Recent pipeline runs
    recent_runs = PipelineRun.objects.all()[:15]

    # Latest dbt test summary
    latest_test_run = DbtTestResult.objects.values("run_at").order_by("-run_at").first()
    test_summary = []
    if latest_test_run:
        test_summary = (
            DbtTestResult.objects
            .filter(run_at=latest_test_run["run_at"])
            .values("model_name")
            .annotate(
                total=Count("id"),
                passed=Count("id", filter=Q(status="pass")),
                failed=Count("id", filter=Q(status="fail")),
            )
            .order_by("model_name")
        )

    # Market overview — latest prices for all tracked stocks
    latest_analytics = (
        StockAnalytics.objects
        .filter(date=StockAnalytics.objects.values("date").order_by("-date").first()["date"])
        .select_related()
        .order_by("ticker")
    )

    context = {
        "kpis": {
            "total_stocks": total_stocks,
            "total_records": f"{total_records:,}",
            "success_rate": success_rate,
            "dbt_pass_rate": dbt_rate,
            "dbt_total": dbt_total,
            "macro_series": macro_count,
            "data_freshness": data_freshness or "< 15 min",
        },
        "sector_data": list(sector_data),
        "recent_runs": recent_runs,
        "test_summary": list(test_summary),
        "latest_analytics": latest_analytics[:20],
        "page": "dashboard",
    }
    return render(request, "dashboard/index.html", context)


def stocks(request):
    """Stock explorer — price chart with MA overlays."""
    ticker = request.GET.get("ticker", "AAPL")
    period = request.GET.get("period", "90")

    tickers = (
        CompanyInfo.objects.values_list("ticker", "name", "sector").order_by("ticker")
    )
    company = CompanyInfo.objects.filter(ticker=ticker).first()

    # Summary stats for selected ticker
    cutoff = date.today() - timedelta(days=int(period))
    latest = StockAnalytics.objects.filter(ticker=ticker, date__gte=cutoff).order_by("-date").first()
    oldest = StockAnalytics.objects.filter(ticker=ticker, date__gte=cutoff).order_by("date").first()

    price_change = None
    price_change_pct = None
    if latest and oldest and oldest.close_price:
        price_change = float(latest.close_price) - float(oldest.close_price)
        price_change_pct = round(price_change / float(oldest.close_price) * 100, 2)

    vol_data = VolatilityMetric.objects.filter(ticker=ticker).order_by("-date").first()

    context = {
        "tickers": list(tickers),
        "selected_ticker": ticker,
        "company": company,
        "period": period,
        "periods": ["30", "60", "90", "180", "365"],
        "latest": latest,
        "price_change": price_change,
        "price_change_pct": price_change_pct,
        "vol_data": vol_data,
        "page": "stocks",
    }
    return render(request, "dashboard/stocks.html", context)


def pipeline(request):
    """Pipeline run history and status."""
    runs = PipelineRun.objects.all()[:100]
    dag_filter = request.GET.get("dag", "")
    status_filter = request.GET.get("status", "")

    if dag_filter:
        runs = runs.filter(dag_id=dag_filter)
    if status_filter:
        runs = runs.filter(status=status_filter)

    # Stats per DAG
    dag_stats = (
        PipelineRun.objects
        .values("dag_id")
        .annotate(
            total=Count("id"),
            successes=Count("id", filter=Q(status="success")),
            failures=Count("id", filter=Q(status="failed")),
            avg_duration=Avg("duration_seconds"),
            total_records=Sum("records_ingested"),
        )
    )

    context = {
        "runs": runs,
        "dag_stats": list(dag_stats),
        "dag_filter": dag_filter,
        "status_filter": status_filter,
        "page": "pipeline",
    }
    return render(request, "dashboard/pipeline.html", context)


def quality(request):
    """dbt test results and data quality metrics."""
    all_tests = DbtTestResult.objects.all()

    # Summary by model
    model_summary = (
        DbtTestResult.objects
        .values("model_name")
        .annotate(
            total=Count("id"),
            passed=Count("id", filter=Q(status="pass")),
            failed=Count("id", filter=Q(status="fail")),
            warned=Count("id", filter=Q(status="warn")),
            avg_exec_ms=Avg("execution_time_ms"),
        )
        .order_by("model_name")
    )

    # Summary by test type
    type_summary = (
        DbtTestResult.objects
        .values("test_name")
        .annotate(total=Count("id"), passed=Count("id", filter=Q(status="pass")))
        .order_by("-total")
    )

    total = all_tests.count()
    passed = all_tests.filter(status="pass").count()
    failed = all_tests.filter(status="fail").count()

    context = {
        "all_tests": all_tests[:200],
        "model_summary": list(model_summary),
        "type_summary": list(type_summary),
        "total_tests": total,
        "passed_tests": passed,
        "failed_tests": failed,
        "pass_rate": round(passed / total * 100, 1) if total else 0,
        "page": "quality",
    }
    return render(request, "dashboard/quality.html", context)


# ─── JSON API endpoints for Chart.js ─────────────────────────────────────────

@require_GET
def api_stock_chart(request):
    ticker = request.GET.get("ticker", "AAPL")
    period = int(request.GET.get("period", 90))
    cutoff = date.today() - timedelta(days=period)

    qs = (
        StockAnalytics.objects
        .filter(ticker=ticker, date__gte=cutoff)
        .order_by("date")
        .values("date", "close_price", "ma_7d", "ma_30d", "ma_90d", "volume", "daily_return")
    )

    data = list(qs)
    return JsonResponse({
        "ticker": ticker,
        "labels": [str(r["date"]) for r in data],
        "close": [float(r["close_price"]) for r in data],
        "ma7": [float(r["ma_7d"]) if r["ma_7d"] else None for r in data],
        "ma30": [float(r["ma_30d"]) if r["ma_30d"] else None for r in data],
        "ma90": [float(r["ma_90d"]) if r["ma_90d"] else None for r in data],
        "volume": [int(r["volume"]) if r["volume"] else 0 for r in data],
        "returns": [float(r["daily_return"]) if r["daily_return"] else 0 for r in data],
    })


@require_GET
def api_sector_chart(request):
    period = int(request.GET.get("period", 30))
    cutoff = date.today() - timedelta(days=period)

    qs = (
        SectorAggregation.objects
        .filter(date__gte=cutoff)
        .values("sector", "date")
        .annotate(avg_ret=Avg("avg_return"))
        .order_by("date")
    )

    sectors = list(SectorAggregation.objects.values_list("sector", flat=True).distinct().order_by("sector"))
    dates = sorted(set(str(r["date"]) for r in qs))
    sector_returns = {s: {str(r["date"]): float(r["avg_ret"] or 0) * 100 for r in qs if r["sector"] == s} for s in sectors}

    return JsonResponse({
        "sectors": sectors,
        "dates": dates,
        "data": sector_returns,
    })


@require_GET
def api_volatility_chart(request):
    ticker = request.GET.get("ticker", "AAPL")
    period = int(request.GET.get("period", 90))
    cutoff = date.today() - timedelta(days=period)

    qs = (
        VolatilityMetric.objects
        .filter(ticker=ticker, date__gte=cutoff)
        .order_by("date")
        .values("date", "annualized_vol", "rolling_vol_30d", "beta", "sharpe_30d")
    )

    data = list(qs)
    return JsonResponse({
        "ticker": ticker,
        "labels": [str(r["date"]) for r in data],
        "annualized_vol": [float(r["annualized_vol"]) * 100 if r["annualized_vol"] else None for r in data],
        "rolling_30d": [float(r["rolling_vol_30d"]) * 100 if r["rolling_vol_30d"] else None for r in data],
        "beta": [float(r["beta"]) if r["beta"] else None for r in data],
        "sharpe": [float(r["sharpe_30d"]) if r["sharpe_30d"] else None for r in data],
    })


@require_GET
def api_pipeline_chart(request):
    """30-day rolling pipeline success/failure counts."""
    from django.db.models.functions import TruncDate

    qs = (
        PipelineRun.objects
        .filter(started_at__date__gte=date.today() - timedelta(days=30))
        .annotate(day=TruncDate("started_at"))
        .values("day")
        .annotate(
            success=Count("id", filter=Q(status="success")),
            failed=Count("id", filter=Q(status="failed")),
        )
        .order_by("day")
    )

    data = list(qs)
    return JsonResponse({
        "labels": [str(r["day"]) for r in data],
        "success": [r["success"] for r in data],
        "failed": [r["failed"] for r in data],
    })


@require_GET
def api_macro_chart(request):
    series_id = request.GET.get("series", "FEDFUNDS")
    period = int(request.GET.get("period", 365))
    cutoff = date.today() - timedelta(days=period)

    qs = (
        MacroIndicator.objects
        .filter(series_id=series_id, date__gte=cutoff)
        .order_by("date")
        .values("date", "value", "name")
    )

    data = list(qs)
    name = data[0]["name"] if data else series_id
    return JsonResponse({
        "series_id": series_id,
        "name": name,
        "labels": [str(r["date"]) for r in data],
        "values": [float(r["value"]) if r["value"] else None for r in data],
    })
