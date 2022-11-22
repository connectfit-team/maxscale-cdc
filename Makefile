COVERAGE_OUTPUT	=	coverage.out

DOCKER_COMPOSE_FILE_TEST = docker-compose.yml

.PHONY: integration-test
integration-test: vendor
	@echo "> Starting services needed for integration tests..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE_TEST) up \
	--build \
	--abort-on-container-exit \
	--exit-code-from test-runner \
	--remove-orphans
	@echo "> Test finished. Removing services...."
	@docker-compose down

.PHONY: coverage-html
coverage-html:
	@echo "> Generating HTML representation of the coverage profile..."
	@go tool cover -html=$(COVERAGE_OUTPUT)

.PHONY: clean
clean:
	@echo "> Deleting coverage file $(COVERAGE_OUTPUT)..."
	@rm -f $(COVERAGE_OUTPUT)
	@echo "> Deleting vendor directory..."
	@rm -rf vendor

.PHONY: vendor
vendor:
	@echo "> Making vendored copy of dependencies..."
	@go mod vendor
