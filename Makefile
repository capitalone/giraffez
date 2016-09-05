.PHONY: all clean build test cover docs

PYMODULE=giraffez
GH_PAGES_SOURCES=docs/source $(PYMODULE) docs/Makefile
CURRENT_BRANCH:=$(shell git rev-parse --abbrev-ref HEAD)
NEW_COMMIT_MESSAGE:=$(shell git log $(CURRENT_BRANCH) -1 --pretty=short --abbrev-commit)

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

test:
	@echo "Running unit tests..."
	@py.test tests/

bench:
	@echo "Running benchmark tests..."
	@py.test tests/benchmarks.py

cover: install-wheel
	@echo "Analyzing coverage (html)"
	@py.test --cov $(PYMODULE) --cov-report html

upload: test
	@python setup.py bdist_wheel upload

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
