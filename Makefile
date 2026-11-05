.PHONY: install lint test build up down deploy-dev poc
install: ; pip install -e ".[dev,ml,stream,local]"
lint: ; ruff check src tests scripts && ruff format --check src tests && mypy src/nmp --ignore-missing-imports
test: ; pytest --cov=nmp --cov-report=term-missing
build: ; python -m build --wheel
up: ; docker compose up -d --build
down: ; docker compose down -v
deploy-dev: ; databricks bundle deploy -t dev
poc: ; python scripts/poc_event.py --ticker AAPL --day $(DAY)   # semaine 1 : make poc DAY=2026-10-13 (mode local, sans Databricks)
