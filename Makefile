help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

lint: ## Run linter
	@go install -v github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@golangci-lint run -v --fix -c .golangci.yaml ./...

test: ## Run tests
	@go test ./... -race -v -count=1 -timeout=20m
