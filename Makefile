VERSION=$(shell git describe --tags --dirty)

.PHONY: docker # build the docker container
docker:
	docker build --build-arg VERSION=$(VERSION) -t datalake:$(VERSION) .

.PHONY: devshell  # Open a developer shell in the docker env
devshell: docker
	docker run --rm -it -v $$PWD:/opt --entrypoint /bin/bash datalake:$(VERSION)

.PHONY: test  # Run the tests

test: docker
	echo VERSION=$(VERSION)
	for p in common client ingester api; do \
		docker run --rm -it --entrypoint py.test datalake:$(VERSION) $$p; \
	done

.PHONY: help  # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/  \1: \2/' | expand -t20
