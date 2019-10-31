VERSION=$(shell git describe --tags --dirty)
REPO=planetlabs
IMAGE="$(REPO)/datalake:$(VERSION)"

.PHONY: docker # build the docker container
docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: devshell  # Open a developer shell in the docker env
devshell: docker
	docker run --rm -it -v $$PWD:/opt --entrypoint /bin/bash $(IMAGE)

.PHONY: test  # Run the tests
test: docker
	echo VERSION=$(VERSION)
	for p in common client ingester api; do \
		docker run --rm -it --entrypoint py.test $(IMAGE) $$p; \
	done

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

.PHONY: help  # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/  \1: \2/' | expand -t20
