.PHONY: all clean build test cover docs

PYMODULE=giraffez

all: clean install

build:
	@echo "Building giraffez extension..."
	@python setup.py build --force

install:
	@echo "Installing giraffez..."
	@python setup.py install

wheel:
	@echo "Building giraffez wheel..."
	@python setup.py bdist_wheel

clean:
	@echo "Cleaning up existing build files..."
	@rm -rf build MANIFEST *.egg-info htmlcov tests/tmp .cache .benchmarks .coverage .eggs
	@rm -f $(PYMODULE)/*.so $(PYMODULE)/*.pyd $(PYMODULE)/*.pyc

test: wheel
	@echo "Running unit tests..."
	@py.test tests/

bench:
	@echo "Running benchmark tests..."
	@py.test tests/benchmarks.py

cover: wheel
	@echo "Analyzing coverage (html)"
	@py.test --cov $(PYMODULE) --cov-report html

upload: test
	@python setup.py bdist_wheel upload

docs:
	@cd docs && sphinx-build -b html src .
