VERSION=$(shell git describe --tags --dirty)
REPO ?= planetlabs
REPO_PATH ?= datalake
IMAGE="$(REPO)/$(REPO_PATH):$(VERSION)"

.PHONY: docker # build the docker container
docker: version
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: dev  # Open a developer shell in the docker env
dev: docker
	docker run --rm -it -v $$PWD:/opt --entrypoint /bin/bash $(IMAGE)

test-client: docker
	docker run --rm -t --entrypoint tox $(IMAGE) -c /opt/client/tox.ini

test-ingester: docker
	docker run --rm -t --entrypoint pytest $(IMAGE) /opt/ingester -svvx

test-api: docker
	docker run --rm -t --entrypoint pytest $(IMAGE) /opt/api -svvx

testp-client: docker
	docker run --rm -t --entrypoint /bin/bash $(IMAGE) -c "cd /opt/client && pytest -svvx -n auto"

testp-ingester: docker
	docker run --rm -t --entrypoint pytest $(IMAGE) /opt/ingester -svvx -n auto

testp-api: docker
	docker run --rm -t --entrypoint pytest $(IMAGE) /opt/api -svvx -n auto

.PHONY: test  # Run the tests
test:
	echo VERSION=$(VERSION)
	$(MAKE) test-client
	$(MAKE) test-ingester
	$(MAKE) test-api

.PHONY: testp  # Run all tests in parallel with pytest-xdist
testp: docker
	docker run --rm -t --entrypoint /bin/bash $(IMAGE) -c "\
		set -e && \
		echo '==> Running client tests...' && \
		cd /opt/client && pytest -svvx -n auto && \
		echo '==> Running ingester tests...' && \
		cd /opt/ingester && pytest -svvx -n auto && \
		echo '==> Running API tests...' && \
		cd /opt/api && pytest -svvx -n auto && \
		echo '==> All tests passed!'"

.PHONY: push
push:
	docker push $(IMAGE)

clean:
	rm -rf version.txt

.PHONY: version
version:
	@test -f version.txt \
		|| echo $(VERSION) | tee version.txt

.PHONY: help  # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/  \1: \2/' | expand -t20
