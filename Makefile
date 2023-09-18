all: build
build:
	go build -o timeq ./cmd

lint:
	golangci-lint run ./...

test:
	gotestsum ./...

bench:
	sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
	go test -v -bench=None -run=xxx -benchmem -cpuprofile=cpu.pprof -memprofile=mem.pprof
