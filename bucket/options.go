package bucket

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sahib/timeq/item"
)

type SyncMode int

func (sm SyncMode) IsValid() bool {
	return sm >= 0 && sm <= SyncFull
}

// The available option are inspired by SQLite:
// https://www.sqlite.org/pragma.html#pragma_synchronous
const (
	// SyncNone does not sync on normal operation (only on close)
	SyncNone = 0
	// SyncData only synchronizes the data log
	SyncData = SyncMode(1 << iota)
	// SyncIndex only synchronizes the index log (does not make sense alone)
	SyncIndex
	// SyncFull syncs both the data and index log
	SyncFull = SyncData | SyncIndex
)

// Logger is a small interface to redirect logs to.
// The default logger outputs to stderr.
type Logger interface {
	Printf(fmt string, args ...any)
}

type writerLogger struct {
	w io.Writer
}

func (fl *writerLogger) Printf(fmtStr string, args ...any) {
	fmt.Fprintf(fl.w, "[timeq] "+fmtStr+"\n", args...)
}

type ErrorMode int

func (em ErrorMode) IsValid() bool {
	return em < errorModeMax && em >= 0
}

const (
	// ErrorModeAbort will immediately abort the current
	// operation if an error is encountered that might lead to data loss.
	ErrorModeAbort = ErrorMode(iota)

	// ErrorModeContinue tries to progress further in case of errors
	// by jumping over a faulty bucket or entry in a bucket.
	// If the error was recoverable, none is returned, but the
	// Logger in the Options will be called (if set) to log the error.
	ErrorModeContinue

	errorModeMax
)

func WriterLogger(w io.Writer) Logger {
	return &writerLogger{w: w}
}

// DefaultLogger produces a logger that writes to stderr.
func DefaultLogger() Logger {
	return &writerLogger{w: os.Stderr}
}

// NullLogger produces a logger that discards all messages.
func NullLogger() Logger {
	return &writerLogger{w: io.Discard}
}

// Options are fine-tuning knobs specific to individual buckets
type Options struct {
	// SyncMode controls how often we sync data to the disk. The more data we sync
	// the more durable is the queue at the cost of throughput.
	// Default is the safe SyncFull. Think twice before lowering this.
	SyncMode SyncMode

	// Logger is used to output some non-critical warnigns or errors that could
	// have been recovered. By default we print to stderr.
	// Only warnings or errors are logged, no debug or informal messages.
	Logger Logger

	// ErrorMode defines how non-critical errors are handled.
	// See the individual enum values for more info.
	ErrorMode ErrorMode

	// BucketFunc defines what key goes to what bucket.
	// The provided function should clamp the key value to
	// a common value. Each same value that was returned goes
	// into the same bucket. The returned value should be also
	// the minimum key of the bucket.
	//
	// Example: '(key / 10) * 10' would produce buckets with 10 items.
	//
	// What bucket size to choose? Please refer to the FAQ in the README.
	//
	// NOTE: This may not be changed after you opened a queue with it!
	//       Only way to change is to create a new queue and shovel the
	//       old data into it.
	BucketFunc func(item.Key) item.Key

	// MaxParallelOpenBuckets limits the number of buckets that can be opened
	// in parallel. Normally, operations like Push() will create more and more
	// buckets with time and old buckets do not get closed automatically, as
	// we don't know when they get accessed again. If there are more buckets
	// open than this number they get closed and will be re-opened if accessed
	// again. If this happens frequently, this comes with a performance penalty.
	// If you tend to access your data with rather random keys, you might want
	// to increase this number, depending on how much resources you have.
	//
	// This is currently only applies to write operations (i.e. Push() and so on),
	// not for read operations like Pop() (as memory needs to stay intact when
	// reading from several buckets). If you have situations where you do read-only
	// operations for some time you should throw in a CloseUnused() call from time
	// to time to make sure memory gets cleaned up.
	//
	// If this number is <= 0, then this feature is disabled, which is not
	// recommended.
	MaxParallelOpenBuckets int
}

func DefaultOptions() Options {
	return Options{
		SyncMode:               SyncFull,
		ErrorMode:              ErrorModeAbort,
		Logger:                 DefaultLogger(),
		BucketFunc:             DefaultBucketFunc,
		MaxParallelOpenBuckets: 4,
	}
}

var DefaultBucketFunc = ShiftBucketFunc(37)

// ShiftBucketFunc creates a fast BucketFunc that divides data into buckets
// by masking `shift` less significant bits of the key. With a shift
// of 37 you roughly get 2m buckets (if your key input are nanosecond-timestamps).
// If you want to calculate the size of a shift, use this formula:
// (2 ** shift) / (1e9 / 60) = minutes
func ShiftBucketFunc(shift int) func(key item.Key) item.Key {
	timeMask := ^item.Key(0) << shift
	return func(key item.Key) item.Key {
		return key & timeMask
	}
}

// FixedSizeBucketFunc returns a BucketFunc that divides buckets into
// equal sized buckets with `n` entries. This can also be used to create
// time-based keys, if you use nanosecond based keys and pass time.Minute
// to create a buckets with a size of one minute.
func FixedSizeBucketFunc(n uint64) func(key item.Key) item.Key {
	if n == 0 {
		// avoid zero division.
		n = 1
	}

	return func(key item.Key) item.Key {
		return (key / item.Key(n)) * item.Key(n)
	}
}

func (o *Options) Validate() error {
	if o.Logger == nil {
		// this allows us to leave out quite some null checks when
		// using the logger option, even when it's not set.
		o.Logger = NullLogger()
	}

	if !o.SyncMode.IsValid() {
		return errors.New("invalid sync mode")
	}

	if !o.ErrorMode.IsValid() {
		return errors.New("invalid error mode")
	}

	if o.BucketFunc == nil {
		return errors.New("bucket func is not allowed to be empty")
	}

	if o.MaxParallelOpenBuckets == 0 {
		// For the outside, that's the same thing, but closeUnused() internally
		// actually knows how to keep the number of buckets to zero, so be clear
		// that the user wants to disable this feature.
		o.MaxParallelOpenBuckets = -1
	}

	return nil
}
