lint:
	flake8 .

typecheck:
	mypy .

test:
	pytest tests/

run:
	python3 main.py --mode full --station-id GHCND:USW00014735 