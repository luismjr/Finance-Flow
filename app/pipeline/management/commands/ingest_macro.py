from datetime import date, timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from pipeline.ingestion import ingest_macro_indicators
from pipeline.models import PipelineRun


class Command(BaseCommand):
    help = "Ingest macroeconomic indicators from FRED API"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=90)

    def handle(self, *args, **options):
        start_date = date.today() - timedelta(days=options["days"])
        run = PipelineRun.objects.create(
            dag_id=PipelineRun.DAG_MACRO_INGESTION,
            run_id=f"manual__{timezone.now().isoformat()}",
            status=PipelineRun.STATUS_RUNNING,
        )

        self.stdout.write(f"Ingesting {len(settings.FRED_SERIES)} FRED series since {start_date}...")

        try:
            stats = ingest_macro_indicators(settings.FRED_SERIES, start_date)
            run.status = PipelineRun.STATUS_SUCCESS
            run.records_ingested = stats["records_created"]
            run.completed_at = timezone.now()
            run.save()
            self.stdout.write(self.style.SUCCESS(
                f"Ingested {stats['records_created']} observations across {stats['series_ok']} series"
            ))
        except Exception as exc:
            run.status = PipelineRun.STATUS_FAILED
            run.error_message = str(exc)
            run.completed_at = timezone.now()
            run.save()
            raise
