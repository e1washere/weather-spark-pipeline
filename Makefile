# Makefile for Weather Spark ETL Pipeline

.PHONY: test lint run clean profile install setup help

# Default target
help:
	@echo "Available commands:"
	@echo "  install    - Install dependencies"
	@echo "  setup      - Setup project directories"
	@echo "  test       - Run all tests"
	@echo "  lint       - Run flake8 linting"
	@echo "  run        - Run full ETL pipeline"
	@echo "  ingest     - Run only data ingestion"
	@echo "  transform  - Run only data transformation"
	@echo "  profile    - Generate data profile report"
	@echo "  clean      - Clean generated data and logs"

install:
	pip3 install -r requirements.txt

setup:
	python3 -c "from config import config; config.ensure_directories()"

test:
	pytest tests/ -v

lint:
	flake8 . --max-line-length=100

run:
	python3 main.py --mode full

ingest:
	python3 main.py --mode ingest

transform:
	python3 main.py --mode transform --input-path data/landing/weather_data_*.csv

profile:
	python3 main.py --mode transform --input-path data/landing/weather_data_*.csv
	@echo "Profile report generated:"
	@cat data/profile_report.md

clean:
	rm -rf data/landing/* data/processed/* data/output/* data/profile_report.md pipeline.log
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
 