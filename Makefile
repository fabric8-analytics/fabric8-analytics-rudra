install:
	pip install -e .

test:
	pytest -W ignore tests -v

.PHONY: install test
