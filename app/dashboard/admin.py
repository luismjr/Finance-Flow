from django.contrib import admin
from pipeline.models import (
    CompanyInfo, StockPrice, MacroIndicator,
    PipelineRun, DbtTestResult, StockAnalytics,
    VolatilityMetric, SectorAggregation,
)

@admin.register(CompanyInfo)
class CompanyInfoAdmin(admin.ModelAdmin):
    list_display = ("ticker", "name", "sector", "industry")
    search_fields = ("ticker", "name", "sector")

@admin.register(StockPrice)
class StockPriceAdmin(admin.ModelAdmin):
    list_display = ("ticker", "date", "close_price", "volume", "ingested_at")
    list_filter = ("ticker",)
    search_fields = ("ticker",)
    ordering = ("-date",)

@admin.register(MacroIndicator)
class MacroIndicatorAdmin(admin.ModelAdmin):
    list_display = ("series_id", "name", "date", "value")
    list_filter = ("series_id",)
    ordering = ("-date",)

@admin.register(PipelineRun)
class PipelineRunAdmin(admin.ModelAdmin):
    list_display = ("dag_id", "run_id", "status", "started_at", "duration_seconds", "records_ingested")
    list_filter = ("dag_id", "status")
    ordering = ("-started_at",)

@admin.register(DbtTestResult)
class DbtTestResultAdmin(admin.ModelAdmin):
    list_display = ("model_name", "test_name", "column_name", "status", "failures", "execution_time_ms")
    list_filter = ("model_name", "status")
