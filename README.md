# ``timeq``

[![GoDoc](https://godoc.org/github.com/sahib/timeq?status.svg)](https://godoc.org/github.com/sahib/timeq)
![Build status](https://github.com/sahib/timeq/actions/workflows/go.yml/badge.svg)

A time-based persistent priority queue in Go.

> [!WARNING]
> This is still in active development. Not every goal described below was reached yet.

## Features

- Clean and well test code base.
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
$ go install github.com/sahib/timeq@latest
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

There's no place where the key of an item is actually assumed to be timestamp.
If you find a good way to sort your data into buckets, you should be good to go.

### Can I store more than one value per key?

Yes, you can. This slows down the index a bit, so you should avoid it. That's
also why this is called `timeq` as this was my primary usecase.

## License

Source code is available under the MIT [License](/LICENSE).

## Contact

Chris Pahl [@sahib](https://github.com/sahib)
