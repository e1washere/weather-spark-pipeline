# Makefile for Weather Spark ETL Pipeline

.PHONY: test lint run clean profile

test:
	pytest tests/ -v

lint:
	flake8 .

run:
	python3 main.py --mode full

profile:
	python3 main.py --mode transform && cat data/profile_report.md

clean:
	rm -rf data/landing/* data/processed/* data/profile_report.md pipeline.log
