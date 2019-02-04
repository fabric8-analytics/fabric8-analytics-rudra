install:
	pip install -e .

test:
	pip install -e . && pytest -W ignore tests -v

.PHONY: install test
