# ``timeq``

[![GoDoc](https://godoc.org/github.com/sahib/timeq?status.svg)](https://godoc.org/github.com/sahib/timeq)
![Build status](https://github.com/sahib/timeq/actions/workflows/go.yml/badge.svg)

A time-based persistent priority queue in Go.

> [!WARNING]
> This is still in active development. Not every goal described below was reached yet.

## Features

- Clean and well test code base based on Go 1.21.
- Configurable durability behavior.
- High (enough) throughput.
- Small memory footprint.
- Simple interface with classic `Push()` and `Pop()` and only
  few other functions.

This implementation does currently not seek to be generally useful but seeks to
be high performant and understandable at the same time. We make the following
assumption for optimal performance:

- Your OS supports `mmap()` and `mremap()` (tested on Linux)
- Seeking is not too expensive (i.e. no rotating disks)
- The priority key is something that roughly increases over time (see [FAQ](#FAQ)).
- You only want to store one value per key (see [FAQ](#FAQ)).
- You push and pop your data in batches.
- File storage is not a primary concern (i.e. no compression implemented).
- You trust your data durability to some random duded on the internet (don't we all?).

If some of those assumptions do not fit your usecase and you still managed to make it work,
I would be happy for some feedback or even pull requests to improve the general usability.
## Usecase

My primary usecase was a embedded linux device that has different services that generate
a stream of data that needs to be send to the cloud. For this the data was required to be
in ascending order (sorted by time) and also needed to be buffered with tight memory boundaries.

A previous attempt with ``sqlite3`` did work well but was much slower than it had to be (also
due to the heavy cost of ``cgo``). This motivated to write this queue implementation.

## Usage

To download the library, just do this in your project:

```bash
$ go get github.com/sahib/timeq@latest
```

We also ship a very minimal command-line client that can be used for experiments.
You can install it like this:

```bash
$ go install github.com/sahib/timeq/cmd@latest
```

## Benchmarks

The included benchmark pushes 2000 items with a payload of 40 byte per operation.

```
$ go test -bench=. -run=xxx
goos: linux
goarch: amd64
pkg: github.com/sahib/timeq
cpu: 12th Gen Intel(R) Core(TM) i7-1270P
BenchmarkPopSyncNone-16      	  35924	    33738 ns/op	    240 B/op	      5 allocs/op
BenchmarkPopSyncData-16      	  35286	    33938 ns/op	    240 B/op	      5 allocs/op
BenchmarkPopSyncIndex-16     	  34030	    34003 ns/op	    240 B/op	      5 allocs/op
BenchmarkPopSyncFull-16      	  35170	    33592 ns/op	    240 B/op	      5 allocs/op
BenchmarkPushSyncNone-16     	  20336	    56867 ns/op	     72 B/op	      2 allocs/op
BenchmarkPushSyncData-16     	  20630	    58613 ns/op	     72 B/op	      2 allocs/op
BenchmarkPushSyncIndex-16    	  20684	    58782 ns/op	     72 B/op	      2 allocs/op
BenchmarkPushSyncFull-16     	  19994	    59491 ns/op	     72 B/op	      2 allocs/op
```

## Design

* All data is divided into buckets by a user-defined function.
* Each bucket is it's own priority queue, responsible for a part of the key space.
* A push to a bucket writes the batch of data to a memory-mapped
  Write-Ahead-Log (WAL) file on disk. The location of the batch is stored in an
  in-memory index and to a index WAL.
* On pop we select the bucket with the lowest key first and ask the index to give
  us the location of the lowest batch. Once done the index is updated to mark the
  items as popped. The data stays intact in the data WAL.
* Once a bucket was completely drained it is removed from disk to retain space.

Since the index is quite small (only one entry per batch) we can fit in memory.
Only the index of buckets is loaded that were pushed to or popped from.

## FAQ:

### Can timeq be also used with non-time based keys?

There are no notable place where the key of an item is actually assumed to be
timestamp, except for the default bucket func (which can be configured). If you
find a good way to sort your data into buckets you should be good to go. Keep
in mind that timestamps were the idea behind the original design, so your
mileage may vary - always benchmark your individual usecase. You can modify one
of the existing benchmarks to test your assumptions.

### Can I store more than one value per key?

Yes, you can. This slows pushing down a bit though, so you might want to avoid
it. If you want to use priority keys that are in a very narrow range (thus many
duplicates) then you can think about spreading the range a bit wider.

For example: You have priority keys from zero to ten for the tasks in your job
queue. Instead of using zero to ten as keys, you can add the job-id to the key
and shift the priority: ``(prio << 32) | jobID``.

### How failsafe is ``timeq``?

Time will tell. I use it on a big fleet of embedded devices in the field
without issue. Design wise, damaged index files can be regenerated from the
data log. There's no error correction code applied in the data log and no
checksums are currently written. If you need this, I'm happy if a PR comes in.

## License

Source code is available under the MIT [License](/LICENSE).

## Contact

Chris Pahl [@sahib](https://github.com/sahib)

## TODO List

- [ ] Improve test coverage / extend test suite
- [ ] Add fuzzing test for Push/Pop
- [ ] Add way to run test as separate process that can be crashed randomly
- [ ] Profile and optimize a bit more, if possible.
- [ ] Use a configurable logger for warnings
