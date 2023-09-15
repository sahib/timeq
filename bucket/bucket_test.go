package bucket

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func withEmptyBucket(t *testing.T, fn func(b *Bucket)) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-buckettest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	bucketDir := filepath.Join(dir, item.Key(23).String())
	bucket, err := Open(bucketDir, DefaultOptions())
	require.NoError(t, err)

	fn(bucket)

	require.NoError(t, bucket.Close())
}

func TestBucketOpenEmpty(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		require.True(t, bucket.Empty())
		require.Equal(t, 0, bucket.Len())
	})
}

func TestBucketPushEmpty(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		require.NoError(t, bucket.Push(nil))
	})
}

func TestBucketPopZero(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		dst := testutils.GenItems(0, 10, 1)
		gotItems, nPopped, err := bucket.Pop(0, dst)
		require.NoError(t, err)
		require.Equal(t, dst, gotItems)
		require.Equal(t, 0, nPopped)
	})
}

func TestBucketPopEmpty(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		dst := testutils.GenItems(0, 10, 1)
		gotItems, nPopped, err := bucket.Pop(100, dst)
		require.NoError(t, err)
		require.Equal(t, dst, gotItems)
		require.Equal(t, 0, nPopped)
	})
}

func TestBucketPushPop(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		expItems := testutils.GenItems(0, 10, 1)
		require.NoError(t, bucket.Push(expItems))
		gotItems, nPopped, err := bucket.Pop(len(expItems), nil)
		require.NoError(t, err)
		require.Equal(t, expItems, gotItems)
		require.Equal(t, len(expItems), nPopped)
	})
}

func TestBucketPushPopReverse(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		expItems := testutils.GenItems(10, 0, -1)
		require.NoError(t, bucket.Push(expItems))
		gotItems, nPopped, err := bucket.Pop(len(expItems), nil)
		require.NoError(t, err)
		require.Equal(t, expItems, gotItems)
		require.Equal(t, len(expItems), nPopped)
	})
}

func TestBucketPushPopSorted(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		push1 := testutils.GenItems(0, 10, 1)
		push2 := testutils.GenItems(11, 20, 1)
		expItems := append(push1, push2...)
		require.NoError(t, bucket.Push(push2))
		require.NoError(t, bucket.Push(push1))
		gotItems, nPopped, err := bucket.Pop(len(push1)+len(push2), nil)
		require.NoError(t, err)
		require.Equal(t, len(push1)+len(push2), nPopped)
		require.Equal(t, expItems, gotItems)
	})
}

func TestBucketPushPopZip(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		push1 := testutils.GenItems(0, 20, 2)
		push2 := testutils.GenItems(1, 20, 2)
		require.NoError(t, bucket.Push(push2))
		require.NoError(t, bucket.Push(push1))
		gotItems, nPopped, err := bucket.Pop(len(push1)+len(push2), nil)
		require.NoError(t, err)

		for idx := 0; idx < 20; idx++ {
			require.Equal(t, testutils.ItemFromIndex(idx), gotItems[idx])
		}

		require.Equal(t, len(push1)+len(push2), nPopped)
	})
}

func TestBucketPopSeveral(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		expItems := testutils.GenItems(0, 10, 1)
		require.NoError(t, bucket.Push(expItems))
		gotItems1, nPopped1, err := bucket.Pop(5, nil)
		require.NoError(t, err)
		gotItems2, nPopped2, err := bucket.Pop(5, nil)
		require.NoError(t, err)

		require.Equal(t, 5, nPopped1)
		require.Equal(t, 5, nPopped2)
		require.Equal(t, expItems, append(gotItems1, gotItems2...))
	})
}

func TestBucketPushPopSeveral(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		push1 := testutils.GenItems(0, 20, 2)
		push2 := testutils.GenItems(1, 20, 2)
		require.NoError(t, bucket.Push(push2))
		require.NoError(t, bucket.Push(push1))
		gotItems1, nPopped1, err := bucket.Pop(10, nil)
		require.NoError(t, err)
		gotItems2, nPopped2, err := bucket.Pop(10, nil)
		require.NoError(t, err)

		require.Equal(t, 10, nPopped1)
		require.Equal(t, 10, nPopped2)

		gotItems := append(gotItems1, gotItems2...)
		for idx := 0; idx < 20; idx++ {
			require.Equal(t, testutils.ItemFromIndex(idx), gotItems[idx])
		}
	})
}

func TestBucketPopLarge(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		expItems := testutils.GenItems(0, 10, 1)
		require.NoError(t, bucket.Push(expItems))
		gotItems, nPopped, err := bucket.Pop(20, nil)
		require.NoError(t, err)
		require.Equal(t, len(expItems), nPopped)
		require.Equal(t, expItems, gotItems)

		gotItems, nPopped, err = bucket.Pop(20, nil)
		require.NoError(t, err)
		require.Equal(t, 0, nPopped)
		require.Len(t, gotItems, 0)
	})
}

func TestBucketLen(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		require.Equal(t, 0, bucket.Len())
		require.True(t, bucket.Empty())

		expItems := testutils.GenItems(0, 10, 1)
		require.NoError(t, bucket.Push(expItems))
		require.Equal(t, 10, bucket.Len())
		require.False(t, bucket.Empty())

		_, _, err := bucket.Pop(5, nil)
		require.NoError(t, err)
		require.Equal(t, 5, bucket.Len())
		require.False(t, bucket.Empty())

		_, _, err = bucket.Pop(5, nil)
		require.NoError(t, err)
		require.True(t, bucket.Empty())
		require.Equal(t, 0, bucket.Len())
	})
}

func TestBucketDeleteLowerThan(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		require.Equal(t, 0, bucket.Len())
		require.True(t, bucket.Empty())

		expItems := testutils.GenItems(0, 100, 1)
		require.NoError(t, bucket.Push(expItems))
		require.Equal(t, 100, bucket.Len())

		deleted, err := bucket.DeleteLowerThan(50)
		require.NoError(t, err)
		require.Equal(t, 49, deleted)
		require.False(t, bucket.Empty())

		deleted, err = bucket.DeleteLowerThan(100)
		require.NoError(t, err)
		require.Equal(t, 51, deleted)
		require.True(t, bucket.Empty())
	})
}

func TestPushDuplicates(t *testing.T) {
	withEmptyBucket(t, func(bucket *Bucket) {
		const pushes = 100
		expItems := testutils.GenItems(0, 10, 1)
		for idx := 0; idx < pushes; idx++ {
			require.NoError(t, bucket.Push(expItems))
			require.Equal(t, (idx+1)*len(expItems), bucket.Len())
		}

		buckLen := bucket.Len()
		gotItems, popped, err := bucket.Pop(buckLen, nil)
		require.NoError(t, err)
		require.Equal(t, buckLen, popped)
		require.Equal(t, buckLen, len(gotItems))
		require.True(t, slices.IsSortedFunc(gotItems, func(i, j item.Item) int {
			return int(i.Key - j.Key)
		}))

		for key := 0; key < len(expItems); key++ {
			for idx := 0; idx < pushes; idx++ {
				it := gotItems[key*pushes+idx]
				require.Equal(t, item.Key(key), it.Key)
			}
		}
	})
}

// TODO: Tests:
// - overlapping pushes.
// - key function (api)
// - re-open tests:
//   - bucket deleted?
//   - popped items really gone?
// - iter tests for buckets.
