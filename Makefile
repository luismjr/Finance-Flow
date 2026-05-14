.PHONY: help up down build migrate seed ingest-stocks ingest-macro dbt-run dbt-test shell logs

help:
	@echo "Finance Flow — Available Commands"
	@echo ""
	@echo "  make up            Start all services (Docker)"
	@echo "  make down          Stop all services"
	@echo "  make build         Rebuild Docker images"
	@echo ""
	@echo "  make migrate       Run Django migrations"
	@echo "  make seed          Seed demo data (no API key needed)"
	@echo "  make ingest-stocks Ingest stock prices from yfinance"
	@echo "  make ingest-macro  Ingest macroeconomic indicators from FRED"
	@echo ""
	@echo "  make dbt-run       Run all dbt models"
	@echo "  make dbt-test      Run all dbt tests (47 tests)"
	@echo "  make dbt-docs      Generate & serve dbt docs"
	@echo ""
	@echo "  make shell         Django shell"
	@echo "  make logs          Tail all service logs"

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

migrate:
	cd app && python manage.py migrate

seed:
	cd app && python manage.py seed_demo_data

ingest-stocks:
	cd app && python manage.py ingest_stocks

ingest-macro:
	cd app && python manage.py ingest_macro

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

shell:
	cd app && python manage.py shell

logs:
	docker compose logs -f

dev:
	cd app && python manage.py migrate && python manage.py seed_demo_data && python manage.py runserver

install:
	pip install -r app/requirements.txt
	pip install -r airflow/requirements.txt
