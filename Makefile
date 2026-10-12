install:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check src tests

sample:
	python -m lakehouse.ingestion.generate_data --out data/landing --rows 20000 --date 2025-01-15 --with-dims

build:
	python -m build --wheel

deploy: build
	databricks bundle deploy -t dev
