.PHONY: docker # build the docker container
docker:
	docker build -t datalake:test .

.PHONY: devshell  # Open a developer shell in the docker env
devshell: docker
	docker run --rm -it -v $$PWD:/opt --entrypoint /bin/bash datalake:test

.PHONY: test  # Run the tests

test: docker
	for p in common client ingester api; do \
		docker run --rm -it --entrypoint py.test datalake:test $$p; \
	done

.PHONY: help  # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/  \1: \2/' | expand -t20
