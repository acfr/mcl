.PHONY: clean clean-doc clean-pyc docs test

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "clean     - Clean all. Run clean-doc and clean-pyc"
	@echo "clean-doc - Remove Sphinx intermediary documentation files"
	@echo "clean-pyc - Remove Python file artifacts"
	@echo "docs      - Generate Sphinx HTML documentation"
	@echo "test      - Run unit-tests with nosetests"

clean: clean-pyc clean-doc

clean-doc:
	$(MAKE) -C doc clean

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

docs:
	$(MAKE) -C doc html
	$(MAKE) -C doc doctest

test:
	nosetests -v --with-timer --timer-top-n 10 --with-coverage --cover-package=mcl mcl/

