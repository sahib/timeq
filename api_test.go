package timeq

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestKeyTrunc(t *testing.T) {
	t.Parallel()

	stamp := time.Date(2023, 1, 1, 12, 13, 14, 15, time.UTC)
	trunc1 := DefaultBucketFunc(item.Key(stamp.UnixNano()))
	trunc2 := DefaultBucketFunc(item.Key(stamp.Add(time.Minute).UnixNano()))
	trunc3 := DefaultBucketFunc(item.Key(stamp.Add(time.Hour).UnixNano()))

	// First two stamps only differ by one minute. They should be truncated
	// to the same value. One hour further should yield a different value.
	require.Equal(t, trunc1, trunc2)
	require.NotEqual(t, trunc1, trunc3)
}

func TestAPIPushPopEmpty(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	queue, err := Open(dir, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, queue.Close())

	require.NoError(t, queue.Push(nil))
	_, err = queue.Pop(100, nil)
	require.NoError(t, err)
}

func TestAPIOptionsValidate(t *testing.T) {
	opts := Options{}
	require.Error(t, opts.Validate())

	opts.BucketFunc = func(k Key) Key { return k }
	require.NoError(t, opts.Validate())
	require.NotNil(t, opts.Logger)
}

func TestAPIPushPopSeveralBuckets(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Open queue with a bucket size of 10 items:
	opts := Options{
		Options: bucket.DefaultOptions(),
		BucketFunc: func(key Key) Key {
			return (key / 10) * 10
		},
	}

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	// Push two batches:
	push1 := Items(testutils.GenItems(10, 20, 1))
	push2 := Items(testutils.GenItems(30, 40, 1))
	require.NoError(t, queue.Push(push1))
	require.NoError(t, queue.Push(push2))
	require.Equal(t, 20, queue.Len())

	// Pop them in one go:
	items, err := queue.Pop(-1, nil)
	require.NoError(t, err)
	require.Equal(t, 0, queue.Len())
	require.Len(t, items, 20)
	require.Equal(t, append(push1, push2...), items)

	// Write the queue to disk:
	require.NoError(t, queue.Sync())
	require.NoError(t, queue.Close())

	// Re-open to see if the items were permanently deleted:
	reopened, err := Open(dir, opts)
	require.NoError(t, err)
	require.Equal(t, 0, reopened.Len())
	items, err = reopened.Pop(-1, nil)
	require.NoError(t, err)
	require.Len(t, items, 0)
}

func TestAPIBinsplit(t *testing.T) {
	t.Parallel()

	idFunc := func(k Key) Key { return k }

	items := Items{
		Item{Key: 0},
		Item{Key: 0},
		Item{Key: 0},
		Item{Key: 1},
		Item{Key: 1},
		Item{Key: 1},
	}

	require.Equal(t, 3, binsplit(items, 0, idFunc))
	require.Equal(t, 6, binsplit(items, 1, idFunc))
	require.Equal(t, 0, binsplit(Items{}, 0, idFunc))
}

func TestAPIBinsplitSeq(t *testing.T) {
	t.Parallel()

	idFunc := func(k Key) Key { return k }
	items := testutils.GenItems(0, 10, 1)
	for idx := 0; idx < len(items); idx++ {
		require.Equal(t, 1, binsplit(items[idx:], Key(idx), idFunc))
	}
}

func TestAPIShovelFastPath(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	q1Dir := filepath.Join(dir, "q1")
	q2Dir := filepath.Join(dir, "q2")

	q1, err := Open(q1Dir, DefaultOptions())
	require.NoError(t, err)

	q2, err := Open(q2Dir, DefaultOptions())
	require.NoError(t, err)

	exp := Items(testutils.GenItems(0, 1000, 1))
	require.NoError(t, q1.Push(exp))
	require.Equal(t, len(exp), q1.Len())
	require.Equal(t, 0, q2.Len())

	n, err := Shovel(q1, q2)
	require.NoError(t, err)
	require.Equal(t, len(exp), n)

	require.Equal(t, 0, q1.Len())
	require.Equal(t, len(exp), q2.Len())

	got, err := q2.Pop(len(exp), nil)
	require.NoError(t, err)
	require.Equal(t, exp, got)

	require.NoError(t, q1.Close())
	require.NoError(t, q2.Close())
}

func TestAPIShovelSlowPath(t *testing.T) {
	t.Run("reopen", func(t *testing.T) {
		testAPIShovelSlowPath(t, true)
	})

	t.Run("no-reopen", func(t *testing.T) {
		testAPIShovelSlowPath(t, false)
	})
}

func testAPIShovelSlowPath(t *testing.T, reopen bool) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	q1Dir := filepath.Join(dir, "q1")
	q2Dir := filepath.Join(dir, "q2")

	q1, err := Open(q1Dir, DefaultOptions())
	require.NoError(t, err)

	q2, err := Open(q2Dir, DefaultOptions())
	require.NoError(t, err)

	q1Push := Items(testutils.GenItems(0, 500, 1))
	require.NoError(t, q1.Push(q1Push))

	// If the bucket exists we have to append:
	q2Push := Items(testutils.GenItems(1000, 2500, 1))
	require.NoError(t, q2.Push(q2Push))

	require.Equal(t, len(q1Push), q1.Len())
	require.Equal(t, len(q2Push), q2.Len())

	n, err := Shovel(q1, q2)
	require.NoError(t, err)
	require.Equal(t, len(q1Push), n)

	require.Equal(t, 0, q1.Len())
	require.Equal(t, len(q1Push)+len(q2Push), q2.Len())

	if reopen {
		require.NoError(t, q1.Close())
		require.NoError(t, q2.Close())

		q1, err = Open(q1Dir, DefaultOptions())
		require.NoError(t, err)

		q2, err = Open(q2Dir, DefaultOptions())
		require.NoError(t, err)
	}

	exp := append(q1Push, q2Push...)
	got, err := q2.Pop(len(q1Push)+len(q2Push), nil)
	require.NoError(t, err)
	require.Equal(t, exp, got)

	require.NoError(t, q1.Close())
	require.NoError(t, q2.Close())
}

func TestAPIDeleteLowerThan(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.BucketFunc = func(k Key) Key {
		return (k / 100) * 100
	}

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	// Deleting the first half should work without issue:
	exp := testutils.GenItems(0, 1000, 1)
	require.NoError(t, queue.Push(exp))
	ndeleted, err := queue.DeleteLowerThan(500)
	require.NoError(t, err)
	require.Equal(t, 500, ndeleted)

	// Deleting the same should yield 0 now.
	ndeleted, err = queue.DeleteLowerThan(500)
	require.NoError(t, err)
	require.Equal(t, 0, ndeleted)

	// Do a partial delete of a bucket:
	ndeleted, err = queue.DeleteLowerThan(501)
	require.NoError(t, err)
	require.Equal(t, 1, ndeleted)

	// Delete more than what is left:
	ndeleted, err = queue.DeleteLowerThan(2000)
	require.NoError(t, err)
	require.Equal(t, 499, ndeleted)

	require.NoError(t, queue.Close())
}

func TestAPIPeek(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.BucketFunc = func(k Key) Key {
		return (k / 100) * 100
	}

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	exp := testutils.GenItems(0, 200, 1)
	require.NoError(t, queue.Push(exp))
	got, err := queue.Peek(len(exp), nil)
	require.NoError(t, err)
	require.Equal(t, exp, got)

	got, err = queue.Pop(len(exp), nil)
	require.NoError(t, err)
	require.Equal(t, exp, got)

	require.NoError(t, queue.Close())
}

func TestAPClear(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	queue, err := Open(dir, DefaultOptions())
	require.NoError(t, err)

	// Empty clear should still work fine:
	require.NoError(t, queue.Clear())
	require.NoError(t, queue.Push(testutils.GenItems(0, 100, 1)))
	require.NoError(t, queue.Clear())
	require.Equal(t, 0, queue.Len())

	require.NoError(t, queue.Close())
}

func TestAPIMove(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	srcDir := filepath.Join(dir, "src")
	dstDir := filepath.Join(dir, "dst")

	opts := DefaultOptions()
	opts.BucketFunc = func(k Key) Key {
		return (k / 100) * 100
	}

	srcQueue, err := Open(srcDir, opts)
	require.NoError(t, err)

	dstQueue, err := Open(dstDir, opts)
	require.NoError(t, err)

	exp := testutils.GenItems(0, 200, 1)
	require.NoError(t, srcQueue.Push(exp))
	require.Equal(t, len(exp), srcQueue.Len())
	require.Equal(t, 0, dstQueue.Len())

	got, err := srcQueue.Move(len(exp), nil, dstQueue)
	require.NoError(t, err)
	require.Equal(t, exp, got)

	require.Equal(t, 0, srcQueue.Len())
	require.Equal(t, len(exp), dstQueue.Len())

	gotMoved, err := dstQueue.Pop(len(exp), nil)
	require.NoError(t, err)
	require.Equal(t, exp, gotMoved)

	require.Equal(t, 0, srcQueue.Len())
	require.Equal(t, 0, dstQueue.Len())

	require.NoError(t, srcQueue.Close())
	require.NoError(t, dstQueue.Close())
}

type LogBuffer struct {
	buf bytes.Buffer
}

func (lb *LogBuffer) Printf(fmtSpec string, args ...any) {
	lb.buf.WriteString(fmt.Sprintf(fmtSpec, args...))
	lb.buf.WriteByte('\n')
}

func (lb *LogBuffer) String() string {
	return lb.buf.String()
}

func TestAPIErrorModePush(t *testing.T) {
	t.Run("abort", func(t *testing.T) {
		testAPIErrorModePush(t, bucket.ErrorModeAbort)
	})
	t.Run("continue", func(t *testing.T) {
		testAPIErrorModePush(t, bucket.ErrorModeContinue)
	})
}

func testAPIErrorModePush(t *testing.T, mode bucket.ErrorMode) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := &LogBuffer{}
	opts := Options{
		Options: bucket.Options{
			ErrorMode: mode,
			Logger:    logger,
		},
		BucketFunc: func(key Key) Key {
			return (key / 10) * 10
		},
	}

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	// make sure the whole directory cannot be accessed,
	// forcing an error during Push.
	require.NoError(t, os.Chmod(dir, 0100))

	pushErr := queue.Push(testutils.GenItems(0, 100, 1))
	if mode == bucket.ErrorModeContinue {
		require.NotEmpty(t, logger.String())
		require.NoError(t, pushErr)
	} else {
		require.Error(t, pushErr)
	}

	require.NoError(t, queue.Close())
}

func TestAPIErrorModePop(t *testing.T) {
	t.Run("abort", func(t *testing.T) {
		testAPIErrorModePop(t, bucket.ErrorModeAbort)
	})
	t.Run("continue", func(t *testing.T) {
		testAPIErrorModePop(t, bucket.ErrorModeContinue)
	})
}

func testAPIErrorModePop(t *testing.T, mode bucket.ErrorMode) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := &LogBuffer{}
	opts := Options{
		Options: bucket.Options{
			ErrorMode: mode,
			Logger:    logger,
		},
		BucketFunc: func(key Key) Key {
			return (key / 10) * 10
		},
	}

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	require.NoError(t, queue.Push(testutils.GenItems(0, 100, 1)))

	// truncate the data log of a single bucket.
	require.NoError(
		t,
		os.Truncate(filepath.Join(dir, Key(0).String(), bucket.DataLogName), 0),
	)

	items, popErr := queue.Pop(100, nil)
	if mode == bucket.ErrorModeContinue {
		require.NoError(t, popErr)
		require.NotEmpty(t, logger.String())
		require.NotEmpty(t, items)
	} else {
		require.Error(t, popErr)
		require.Empty(t, items)
	}

	require.NoError(t, queue.Close())
}

func TestAPIErrorModeDelete(t *testing.T) {
	t.Run("abort", func(t *testing.T) {
		testAPIErrorModeDeleteLowerThan(t, bucket.ErrorModeAbort)
	})
	t.Run("continue", func(t *testing.T) {
		testAPIErrorModeDeleteLowerThan(t, bucket.ErrorModeContinue)
	})
}

func testAPIErrorModeDeleteLowerThan(t *testing.T, mode bucket.ErrorMode) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := &LogBuffer{}
	opts := Options{
		Options: bucket.Options{
			ErrorMode: mode,
			Logger:    logger,
		},
		BucketFunc: func(key Key) Key {
			return (key / 10) * 10
		},
	}

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	require.NoError(t, queue.Push(testutils.GenItems(0, 100, 1)))

	// truncate the data log of a single bucket.
	require.NoError(
		t,
		os.Truncate(filepath.Join(dir, Key(0).String(), bucket.DataLogName), 0),
	)

	ndeleted, err := queue.DeleteLowerThan(100)
	if mode == bucket.ErrorModeContinue {
		require.NotEmpty(t, logger.String())
		require.NoError(t, err)
		require.Equal(t, 90, ndeleted)
	} else {
		require.Error(t, err)
		require.Equal(t, 0, ndeleted)
	}

	require.NoError(t, queue.Close())
}

func TestAPIBadOptions(t *testing.T) {
	t.Parallel()

	// Still create test dir to make sure it does not error out because of that:
	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.BucketFunc = nil
	_, err = Open(dir, opts)
	require.Error(t, err)
}

func TestAPIPushError(t *testing.T) {
	// Still create test dir to make sure it does not error out because of that:
	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	buf := &bytes.Buffer{}
	opts := DefaultOptions()
	opts.Logger = bucket.WriterLogger(buf)
	queue, err := Open(dir, opts)
	require.NoError(t, err)

	// First push creates the bucket.
	require.NoError(t, queue.Push(testutils.GenItems(0, 10, 1)))

	// Truncating the log should trigger an error on the second push (actually a panic)
	dataPath := filepath.Join(dir, item.Key(0).String(), bucket.DataLogName)
	require.NoError(t, os.Truncate(dataPath, 0))
	require.Error(t, queue.Push(testutils.GenItems(0, 10, 1)))

	require.NoError(t, queue.Close())
}

// helper to get the number of open file descriptors for current process:
func openfds(t *testing.T) int {
	ents, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(ents)
}

// helper to get the residual memory of the current process:
func rssBytes(t *testing.T) int64 {
	data, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		split := strings.SplitN(line, ":", 2)
		if strings.TrimSpace(split[0]) != "VmRSS" {
			continue
		}

		kbs := strings.TrimSpace(strings.TrimSuffix(split[1], "kB"))
		kb, err := strconv.ParseInt(kbs, 10, 64)
		require.NoError(t, err)
		return kb * 1024
	}

	require.Fail(t, "failed to find rss")
	return 0
}

// Check if old buckets get closed when pushing a lot of data.
// Old buckets would still claim the memory maps, causing more residual memory
// usage and also increasing number of file descriptors.
func TestAPIMaxParallelBuckets(t *testing.T) {
	t.Parallel()

	// Still create test dir to make sure it does not error out because of that:
	dir, err := os.MkdirTemp("", "timeq-apitest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const N = 1000 // bucket size
	opts := DefaultOptions()
	opts.BucketFunc = func(key item.Key) item.Key {
		return (key / N) * N
	}

	// This test should fail if this is set to 0!
	opts.MaxParallelOpenBuckets = 1

	queue, err := Open(dir, opts)
	require.NoError(t, err)

	var refFds int
	var refRss int64

	for idx := 0; idx < 100; idx++ {
		if idx == 10 {
			refFds = openfds(t)
			refRss = rssBytes(t)
		}

		if idx > 10 {
			// it takes a bit of time for the values to stabilize.
			fds := openfds(t)
			rss := rssBytes(t)

			if fac := float64(fds) / float64(refFds); fac > 1.5 {
				require.Failf(
					t,
					"fd increase",
					"number of fds increases: %v at bucket #%d",
					fac,
					idx,
				)
			}

			if fac := float64(rss) / float64(refRss); fac > 1.5 {
				require.Failf(
					t,
					"rss increase",
					"number of rss increases: %v times at bucket #%d",
					fac,
					idx,
				)
			}
		}

		require.NoError(t, queue.Push(testutils.GenItems(idx*N, idx*N+N, 1)))
	}

	require.NoError(t, queue.Close())
}

func TestBug(t *testing.T) {
	opts := DefaultOptions()
	opts.Logger = bucket.DefaultLogger()
	queue, err := Open("/home/chris/code/timeq/db", opts)
	require.NoError(t, err)

	queue.buckets.Iter(bucket.Load, func(key item.Key, b *bucket.Bucket) error {
		fmt.Println("LOADED", key)
		return nil
	})

	require.NoError(t, queue.Close())

}
