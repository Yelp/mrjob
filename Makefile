PYTHON=python
PYTHONPATH=.

all: test install

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
