.PHONY: all clean build test cover cover-html docs

PYMODULE=giraffez
GH_PAGES_SOURCES=docs/source $(PYMODULE) docs/Makefile
CURRENT_BRANCH:=$(shell git rev-parse --abbrev-ref HEAD)
NEW_COMMIT_MESSAGE:=$(shell git log $(CURRENT_BRANCH) -1 --pretty=short --abbrev-commit)

all: clean install

build:
	@echo "Building giraffez extension..."
	@python setup.py build --force

build-wheel:
	@python setup.py bdist_wheel

install: build-wheel
	@echo "Installing giraffez..."
	@pip install --ignore-installed --upgrade --no-deps --user .

upload: test
	@python setup.py bdist_wheel upload

clean:
	@echo "Cleaning up existing build files..."
	@rm -rf build dist MANIFEST *.egg-info htmlcov tests/tmp .cache .benchmarks .coverage .eggs
	@rm -f $(PYMODULE)/*.so $(PYMODULE)/*.pyd
	@find . -name '__pycache__' -delete -o -name '*.pyc' -delete

test:
	@echo "Running unit tests..."
	@py.test tests/

test-deps:
	@echo "Installing test dependencies..."
	@pip install --user --force --upgrade mock pytest pytest-cov pytest-mock pytest-benchmark sphinx_rtd_theme wheel

bench:
	@echo "Running unit tests..."
	@py.test tests/benchmarks.py

cover: install-wheel
	@echo "Analyzing coverage (html)"
	@py.test --cov $(PYMODULE) --cov-report html

docs:
	$(MAKE) -C docs html

initdocs:
	@git branch gh-pages
	@git symbolic-ref HEAD refs/heads/gh-pages
	@rm .git/index
	@git clean -fdx
	@touch .nojekyll
	@git add .nojekyll
	@git commit -m 'Initial commit'
	@git push origin gh-pages

updatedocs:
	@git checkout gh-pages
	@rm -rf docs/build docs/_sources docs/_static *.html *.js
	@git checkout $(CURRENT_BRANCH) $(GH_PAGES_SOURCES)
	@git reset HEAD
	@$(MAKE) -C docs clean html
	@rm -rf _sources _static
	@mv -fv docs/build/html/* .
	@rm -rf $(GH_PAGES_SOURCES) docs/build/html
	@git add -A
	@git commit -m "Generage gh-pages for $(NEW_COMMIT_MESSAGE)"
	@git push origin gh-pages
	@git checkout $(CURRENT_BRANCH)
