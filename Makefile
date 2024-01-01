all: build
build:
	go build -o timeq ./cmd

lint:
	golangci-lint run ./...

test:
	@# This incudes integration testing in the output.
	@# (e.g.: Shovel() has no tests in bucket/ but has one in ./
	@#  without this, Shovel() would be counted as 0% covered)
	@gotestsum -- -race -coverprofile=cover.out ./... -covermode=atomic -coverpkg $$(go list -f $$'{{range $$f := .Imports}}{{$$f}}\n{{end}}' ./... | grep timeq | sort | uniq | paste -sd ',')

test_all:
	@gotestsum -- --tags=slow -race -coverprofile=cover.out ./... -covermode=atomic -coverpkg $$(go list -f $$'{{range $$f := .Imports}}{{$$f}}\n{{end}}' ./... | grep timeq | sort | uniq | paste -sd ',')

cover: test
	go tool cover -html cover.out

sloc:
	@cloc --json $$(find -iname '*.go' ! -iname '*_test.go') | jq '.Go.code' | xargs -n1 printf "Actual code:\t%d Lines\n"
	@cloc --json $$(find -iname '*_test.go') | jq '.Go.code' | xargs -n1 printf "Test cases:\t%d Lines\n"
	@cloc --json $$(find -iname '*.go') | jq '.Go.code' | xargs -n1 printf "Total code:\t%d Lines\n"

fuzz:
	go test -v -fuzztime 5m -fuzz ./

bench:
	sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
	go test -v -bench=. -run=xxx -benchmem -cpuprofile=cpu.pprof -memprofile=mem.pprof
