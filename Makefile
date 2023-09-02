all: build
build:
	go build -o timeq ./cmd

lint:
	golangci-lint run ./...

test:
	gotestsum ./...
