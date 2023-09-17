package timeq

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestKeyTrunc(t *testing.T) {
	stamp := time.Date(2023, 1, 1, 12, 13, 14, 15, time.UTC)
	trunc1 := DefaultBucketFunc(item.Key(stamp.UnixNano()))
	trunc2 := DefaultBucketFunc(item.Key(stamp.Add(time.Minute).UnixNano()))
	trunc3 := DefaultBucketFunc(item.Key(stamp.Add(time.Hour).UnixNano()))

	// First two stamps only differ by one minute. They should be truncated
	// to the same value. One hour further should yield a different value.
	require.Equal(t, trunc1, trunc2)
	require.NotEqual(t, trunc1, trunc3)
}

func TestAPIPushPopSeveralBuckets(t *testing.T) {
	dir, err := os.MkdirTemp("", "timeq-bucketstest")
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
	idFunc := func(k Key) Key { return k }
	items := testutils.GenItems(0, 10, 1)
	for idx := 0; idx < len(items); idx++ {
		require.Equal(t, 1, binsplit(items[idx:], Key(idx), idFunc))
	}
}

func TestShovelFastPath(t *testing.T) {
	dir, err := os.MkdirTemp("", "timeq-bucketstest")
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

func TestShovelSlowPath(t *testing.T) {
	dir, err := os.MkdirTemp("", "timeq-bucketstest")
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

	exp := append(q1Push, q2Push...)
	got, err := q2.Pop(len(q1Push)+len(q2Push), nil)
	require.NoError(t, err)
	require.Equal(t, exp, got)

	require.NoError(t, q1.Close())
	require.NoError(t, q2.Close())
}
