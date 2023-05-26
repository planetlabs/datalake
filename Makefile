VERSION=$(shell git describe --tags --dirty)
REPO ?= planetlabs
REPO_PATH ?= datalake
IMAGE="$(REPO)/$(REPO_PATH):$(VERSION)"

.PHONY: docker # build the docker container
docker: version
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: devshell  # Open a developer shell in the docker env
devshell: docker
	docker run --rm -it -v $$PWD:/opt --entrypoint /bin/bash $(IMAGE)

test-client: docker
	docker run --rm --entrypoint tox $(IMAGE) -c /opt/client/tox.ini

test-ingester: docker
	docker run --rm --entrypoint py.test $(IMAGE) ingester

test-api: docker
	docker run --rm --entrypoint py.test $(IMAGE) api

.PHONY: test  # Run the tests
test:
	echo VERSION=$(VERSION)
	$(MAKE) test-client
	$(MAKE) test-ingester
	$(MAKE) test-api

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
