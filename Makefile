# You can set these variables from the command line.
SPHINXBUILD   = sphinx-build
SOURCEDIR     = docs
BUILDDIR      = docs/_build


help:            ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

install:         ## Install development dependencies
	pip install -r requirements-dev.txt pre-commit -e . 
	pre-commit install

test:            ## Run tests
	tox

docs.build:      ## Build the documentation
	$(SPHINXBUILD) $(SOURCEDIR) $(BUILDDIR) -b html -E
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)."

docs.clean:      ## Remove the generated documentation
	rm -rf $(BUILDDIR)
