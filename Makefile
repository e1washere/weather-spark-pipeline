# Weather ETL Pipeline Makefile

.PHONY: help install setup test lint run ingest transform profile clean validate monitor performance-report

help:
	@echo "Weather ETL Pipeline - Available Commands:"
	@echo ""
	@echo "Setup:"
	@echo "  install          Install Python dependencies"
	@echo "  setup            Setup project directories and configuration"
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests"
	@echo "  lint             Run code linting with flake8"
	@echo "  type-check       Run static type checking with mypy"
	@echo ""
	@echo "Pipeline Execution:"
	@echo "  run              Run full ETL pipeline"
	@echo "  ingest           Run data ingestion only"
	@echo "  transform        Run data transformation only"
	@echo "  profile          Generate data profile report"
	@echo ""
	@echo "Data Quality:"
	@echo "  validate         Run data validation tests"
	@echo "  monitor          Run performance monitoring"
	@echo "  performance-report Generate performance report"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean            Clean all generated files and logs"

install:
	pip install -r requirements.txt

setup:
	mkdir -p data/{landing,processed,output,logs}
	mkdir -p tests
	@echo "Project setup completed"

test:
	pytest tests/ -v

lint:
	flake8 src/ --max-line-length=100

type-check:
	mypy src/

run:
	python3 -m src.bin.main --mode full

ingest:
	python3 -m src.bin.main --mode ingest

transform:
	python3 -m src.bin.main --mode transform --input-path data/landing/weather_data_*.csv

profile:
	python3 -m src.bin.main --mode transform --input-path data/landing/weather_data_*.csv
	@echo "Profile report generated:"
	@cat data/profile_report.md

validate:
	pytest tests/test_validation.py -v

monitor:
	python3 -m src.bin.main --mode full --performance-report

performance-report:
	@echo "Latest performance reports:"
	@ls -la logs/performance_report_*.md 2>/dev/null || echo "No performance reports found"

clean:
	rm -rf data/{landing,processed,output,logs}/*
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf src/__pycache__/
	rm -rf src/*/__pycache__/
	rm -rf tests/__pycache__/
	rm -rf tests/*/__pycache__/
	rm -f *.log
	rm -f data/profile_report.md
	@echo "Cleanup completed"
 