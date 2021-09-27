VERSION=$(shell git describe --tags --dirty)
REPO=planetlabs
IMAGE="$(REPO)/datalake:$(VERSION)"

.PHONY: docker # build the docker container
docker: version
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: devshell  # Open a developer shell in the docker env
devshell: docker
	docker run --rm -it -v $$PWD:/opt --entrypoint /bin/bash $(IMAGE)

test-client: docker
	docker run --rm --entrypoint py.test $(IMAGE) client

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
ifeq ($(DOCKER_USERNAME),)
	echo "You must set DOCKER_USERNAME"
	exit 1
endif
ifeq ($(DOCKER_PASSWORD),)
	echo "You must set DOCKER_PASSWORD"
	exit 1
endif
	echo "$(DOCKER_PASSWORD)" | docker login -u "$(DOCKER_USERNAME)" --password-stdin && \
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
