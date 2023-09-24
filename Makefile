all: build
build:
	go build -o timeq ./cmd

lint:
	golangci-lint run ./...

test:
	gotestsum -- -coverprofile=cover.out ./...

sloc:
	@cloc --json $$(find -iname '*.go' ! -iname '*_test.go') | jq '.Go.code' | xargs -n1 printf "Actual code:\t%d Lines\n"
	@cloc --json $$(find -iname '*_test.go') | jq '.Go.code' | xargs -n1 printf "Test cases:\t%d Lines\n"
	@cloc --json $$(find -iname '*.go') | jq '.Go.code' | xargs -n1 printf "Total code:\t%d Lines\n"

fuzz:
	go test -v -fuzztime 5m -fuzz ./

bench:
	sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
	go test -v -bench=. -run=xxx -benchmem -cpuprofile=cpu.pprof -memprofile=mem.pprof
