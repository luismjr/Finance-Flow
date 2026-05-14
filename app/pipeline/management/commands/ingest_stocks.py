from datetime import date, timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from pipeline.ingestion import ingest_stock_prices
from pipeline.models import PipelineRun
from pipeline.transforms import run_sector_aggregation_mart, run_stock_analytics_mart, run_volatility_mart


class Command(BaseCommand):
    help = "Ingest daily stock prices from yfinance (no API key needed)"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=5, help="Days of history to ingest")
        parser.add_argument("--full", action="store_true", help="Ingest 2 years of history")
        parser.add_argument("--no-transforms", action="store_true")

    def handle(self, *args, **options):
        tickers = settings.TRACKED_TICKERS
        end_date = date.today()
        start_date = end_date - timedelta(days=730 if options["full"] else options["days"])

        run = PipelineRun.objects.create(
            dag_id=PipelineRun.DAG_STOCK_INGESTION,
            run_id=f"manual__{timezone.now().isoformat()}",
            status=PipelineRun.STATUS_RUNNING,
        )

        self.stdout.write(f"Ingesting {len(tickers)} tickers from {start_date} to {end_date}...")

        try:
            stats = ingest_stock_prices(tickers, start_date, end_date)
            run.status = PipelineRun.STATUS_SUCCESS
            run.records_ingested = stats["records_created"]
            run.tickers_processed = stats["tickers_ok"]
            run.completed_at = timezone.now()
            run.save()

            self.stdout.write(self.style.SUCCESS(
                f"Ingested {stats['records_created']:,} records for {stats['tickers_ok']} tickers"
                f" ({stats['tickers_failed']} failed)"
            ))

            if not options["no_transforms"]:
                self.stdout.write("Running transformations...")
                run_stock_analytics_mart()
                run_volatility_mart()
                run_sector_aggregation_mart()
                self.stdout.write(self.style.SUCCESS("Transformations complete."))

        except Exception as exc:
            run.status = PipelineRun.STATUS_FAILED
            run.error_message = str(exc)
            run.completed_at = timezone.now()
            run.save()
            raise
