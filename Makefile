run:
	go run .

install-tools:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.58.0
lint:
	golangci-lint run --allow-parallel-runners --verbose --build-tags integration --timeout=30m
test:
	go test ./... ./...
