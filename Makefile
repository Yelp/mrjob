PYTHON=python
PYTHONPATH=$(CURDIR)

all: test

.PHONY: test
test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) setup.py test

.PHONY: tests
tests: test

install:
	$(PYTHON) setup.py install

.PHONY: clean
clean:
	$(PYTHON) setup.py clean
