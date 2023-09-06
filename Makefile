all: build
build:
	go build -o timeq ./cmd

lint:
	golangci-lint run ./...

test:
	gotestsum ./...

bench:
	go test -v -bench=. -run=xxx -benchmem -cpuprofile=cpu.pprof -memprofile=mem.pprof
