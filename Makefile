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
	rm -f *deb

.PHONY: dev-tools
dev-tools:
	apt-get update
	apt-get -y install ruby1.9.1-full git python-setuptools python-pip
	gem install fpm --conservative

.PHONY: package
package:
	fpm -s python -t deb .
