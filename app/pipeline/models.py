from django.db import models
from django.utils import timezone


class CompanyInfo(models.Model):
    ticker = models.CharField(max_length=10, unique=True, db_index=True)
    name = models.CharField(max_length=200)
    sector = models.CharField(max_length=100)
    industry = models.CharField(max_length=150)
    market_cap = models.BigIntegerField(null=True)
    exchange = models.CharField(max_length=20, default="NASDAQ")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["ticker"]

    def __str__(self):
        return f"{self.ticker} — {self.name}"


class StockPrice(models.Model):
    ticker = models.CharField(max_length=10, db_index=True)
    date = models.DateField(db_index=True)
    open_price = models.DecimalField(max_digits=12, decimal_places=4)
    high_price = models.DecimalField(max_digits=12, decimal_places=4)
    low_price = models.DecimalField(max_digits=12, decimal_places=4)
    close_price = models.DecimalField(max_digits=12, decimal_places=4)
    adj_close = models.DecimalField(max_digits=12, decimal_places=4)
    volume = models.BigIntegerField()
    ingested_at = models.DateTimeField(default=timezone.now)
    source = models.CharField(max_length=50, default="yfinance")

    class Meta:
        unique_together = ("ticker", "date")
        ordering = ["-date", "ticker"]
        indexes = [
            models.Index(fields=["ticker", "date"]),
        ]

    def __str__(self):
        return f"{self.ticker} {self.date} close={self.close_price}"


class MacroIndicator(models.Model):
    series_id = models.CharField(max_length=50, db_index=True)
    name = models.CharField(max_length=200)
    date = models.DateField(db_index=True)
    value = models.DecimalField(max_digits=20, decimal_places=6, null=True)
    unit = models.CharField(max_length=50, blank=True)
    frequency = models.CharField(max_length=20, default="monthly")
    ingested_at = models.DateTimeField(default=timezone.now)
    source = models.CharField(max_length=50, default="FRED")

    class Meta:
        unique_together = ("series_id", "date")
        ordering = ["-date", "series_id"]
        indexes = [
            models.Index(fields=["series_id", "date"]),
        ]

    def __str__(self):
        return f"{self.series_id} {self.date} = {self.value}"


class PipelineRun(models.Model):
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_CHOICES = [
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
    ]
    DAG_STOCK_INGESTION = "stock_ingestion"
    DAG_MACRO_INGESTION = "macro_indicators"
    DAG_DBT_TRANSFORM = "dbt_transform"
    DAG_CHOICES = [
        (DAG_STOCK_INGESTION, "Stock Price Ingestion"),
        (DAG_MACRO_INGESTION, "Macro Indicators Ingestion"),
        (DAG_DBT_TRANSFORM, "dbt Transformation"),
    ]

    dag_id = models.CharField(max_length=100, choices=DAG_CHOICES)
    run_id = models.CharField(max_length=200, unique=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_RUNNING)
    started_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)
    records_ingested = models.IntegerField(default=0)
    records_transformed = models.IntegerField(default=0)
    tickers_processed = models.IntegerField(default=0)
    error_message = models.TextField(blank=True)
    duration_seconds = models.FloatField(null=True)

    class Meta:
        ordering = ["-started_at"]

    def __str__(self):
        return f"{self.dag_id} {self.run_id} [{self.status}]"

    def save(self, *args, **kwargs):
        if self.completed_at and self.started_at:
            delta = self.completed_at - self.started_at
            self.duration_seconds = delta.total_seconds()
        super().save(*args, **kwargs)

    @property
    def duration_display(self):
        if not self.duration_seconds:
            return "—"
        mins = int(self.duration_seconds // 60)
        secs = int(self.duration_seconds % 60)
        return f"{mins}m {secs}s" if mins else f"{secs}s"


class DbtTestResult(models.Model):
    STATUS_PASS = "pass"
    STATUS_FAIL = "fail"
    STATUS_WARN = "warn"
    STATUS_CHOICES = [
        (STATUS_PASS, "Pass"),
        (STATUS_FAIL, "Fail"),
        (STATUS_WARN, "Warn"),
    ]

    pipeline_run = models.ForeignKey(
        PipelineRun, on_delete=models.CASCADE, null=True, related_name="dbt_tests"
    )
    run_at = models.DateTimeField(default=timezone.now)
    model_name = models.CharField(max_length=200)
    test_name = models.CharField(max_length=200)
    test_type = models.CharField(max_length=50, default="generic")
    column_name = models.CharField(max_length=100, blank=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES)
    failures = models.IntegerField(default=0)
    execution_time_ms = models.IntegerField(default=0)

    class Meta:
        ordering = ["-run_at", "model_name"]
        indexes = [
            models.Index(fields=["run_at", "status"]),
            models.Index(fields=["model_name"]),
        ]

    def __str__(self):
        return f"{self.model_name}.{self.test_name} [{self.status}]"


class StockAnalytics(models.Model):
    """dbt mart layer — moving averages and daily returns."""
    ticker = models.CharField(max_length=10, db_index=True)
    date = models.DateField(db_index=True)
    close_price = models.DecimalField(max_digits=12, decimal_places=4)
    volume = models.BigIntegerField(null=True)
    daily_return = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    ma_7d = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    ma_30d = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    ma_90d = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    computed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        unique_together = ("ticker", "date")
        ordering = ["-date", "ticker"]
        indexes = [
            models.Index(fields=["ticker", "date"]),
        ]


class VolatilityMetric(models.Model):
    """dbt mart layer — rolling volatility metrics."""
    ticker = models.CharField(max_length=10, db_index=True)
    date = models.DateField(db_index=True)
    rolling_vol_30d = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    rolling_vol_90d = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    annualized_vol = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    beta = models.DecimalField(max_digits=8, decimal_places=4, null=True)
    sharpe_30d = models.DecimalField(max_digits=8, decimal_places=4, null=True)
    computed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        unique_together = ("ticker", "date")
        ordering = ["-date", "ticker"]


class SectorAggregation(models.Model):
    """dbt mart layer — sector-level daily aggregations."""
    sector = models.CharField(max_length=100, db_index=True)
    date = models.DateField(db_index=True)
    avg_return = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    avg_volatility = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    total_volume = models.BigIntegerField(null=True)
    num_stocks = models.IntegerField(default=0)
    top_performer = models.CharField(max_length=10, blank=True)
    bottom_performer = models.CharField(max_length=10, blank=True)
    computed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        unique_together = ("sector", "date")
        ordering = ["-date", "sector"]
