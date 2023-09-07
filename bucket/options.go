package bucket

import (
	"fmt"
	"io"
	"os"
	"time"
)

type SyncMode int

// The available option are inspired by SQLite:
// https://www.sqlite.org/pragma.html#pragma_synchronous
const (
	// SyncNone does not sync on normal operation (only on close)
	SyncNone = SyncMode(1 << iota)
	// SyncData only synchronizes the data log
	SyncData
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

type fileLogger struct {
	w io.Writer
}

func (fl *fileLogger) Printf(fmtStr string, args ...any) {
	fmt.Fprintf(fl.w, "[timeq] "+fmtStr, args...)
}

func DefaultLogger() Logger {
	return &fileLogger{w: os.Stderr}
}

// Options are fine-tuning knobs specific to individual buckets
type Options struct {
	// MaxSkew defines how much a key may be shifted in case of duplicates.
	// This can lead to very small inaccuracies. Zero disables this.
	// Default is 1us is plenty time to shift a key
	MaxSkew time.Duration

	// SyncMode controls how often we sync data to the disk. The more data we sync
	// the more durable is the queue at the cost of throughput.
	// Default is the safe SyncFull. Think twice before lowering this.
	SyncMode SyncMode

	// Logger is used to output some non-critical warnigns or errors that could
	// have been recovered. By default we print to stderr.
	Logger Logger
}

// DefaultOptions returns sane options you can start with
func DefaultOptions() Options {
	return Options{
		MaxSkew:  time.Microsecond,
		SyncMode: SyncFull,
		Logger:   DefaultLogger(),
	}
}
