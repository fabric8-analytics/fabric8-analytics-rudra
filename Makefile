install:
	pip install -e .

venv:
	virtualenv -p python3 --no-site-packages venv-test

clean:
	rm -rf venv-test rudra.egg-info && find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

run_pytest:
	pytest -W ignore tests -v

test: venv install run_pytest clean

.PHONY: install test venv clean run_pytest
