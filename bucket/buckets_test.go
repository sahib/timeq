package bucket

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func writeDummyBucket(t *testing.T, dir string, key item.Key, items item.Items) {
	bucketDir := filepath.Join(dir, key.String())
	require.NoError(t, os.MkdirAll(bucketDir, 0700))

	bucket, err := Open(bucketDir, nil, DefaultOptions())
	require.NoError(t, err)

	require.NoError(t, bucket.Push(items))
	require.NoError(t, bucket.Sync(true))
	require.NoError(t, bucket.Close())
}

func TestBucketsOpenEmpty(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	bs, err := LoadAll(dir, opts)
	require.NoError(t, err)
	require.Equal(t, 0, bs.Len(""))
	require.NoError(t, bs.Sync())
	require.NoError(t, bs.Close())
}

func TestBucketsClearEmpty(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	bs, err := LoadAll(dir, opts)
	require.NoError(t, err)
	require.NoError(t, bs.Clear())
	require.NoError(t, bs.Close())
}

func TestBucketsIter(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	expected := []item.Key{10, 20, 40, 80}
	for _, key := range expected {
		writeDummyBucket(
			t,
			dir,
			key,
			testutils.GenItems(int(key), int(key)+10, 1),
		)
	}

	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	bs, err := LoadAll(dir, opts)
	require.NoError(t, err)

	// load bucket 80 early to check if iter can handle
	// already loaded buckets too.
	_, err = bs.forKey(80)
	require.NoError(t, err)

	got := []item.Key{}
	require.NoError(t, bs.iter(Load, func(key item.Key, b *Bucket) error {
		got = append(got, b.Key())
		require.Equal(t, key, b.Key())
		return nil
	}))

	require.Equal(t, expected, got)
	require.Equal(t, 40, bs.Len(""))
}

func TestBucketsForKey(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	writeDummyBucket(t, dir, 33, testutils.GenItems(0, 10, 1))

	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	bs, err := LoadAll(dir, opts)
	require.NoError(t, err)

	// open freshly:
	b1, err := bs.forKey(32)
	require.NoError(t, err)
	require.Equal(t, item.Key(32), b1.Key())

	// open again, must be the same memory:
	b2, err := bs.forKey(32)
	require.NoError(t, err)
	require.Equal(t, item.Key(32), b2.Key())
	require.Equal(t, b1, b2)

	// existing bucket should load fine:
	b3, err := bs.forKey(33)
	require.NoError(t, err)
	require.Equal(t, item.Key(33), b3.Key())
	require.Equal(t, 10, b3.Len(""))

	require.NoError(t, bs.Clear())

	// open again, must be the same memory:

	b3c, err := bs.forKey(33)
	require.NoError(t, err)
	require.Equal(t, item.Key(33), b3c.Key())
	require.Equal(t, 0, b3c.Len(""))
	require.NoError(t, bs.Close())
}

func TestBucketsValidateFunc(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	writeDummyBucket(t, dir, 30, testutils.GenItems(30, 40, 1))
	writeDummyBucket(t, dir, 50, testutils.GenItems(50, 60, 1))

	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	bs, err := LoadAll(dir, opts)
	require.NoError(t, err)

	require.NoError(t, bs.ValidateBucketKeys(func(key item.Key) item.Key {
		// id-func has to pass always.
		return key
	}))

	require.NoError(t, bs.ValidateBucketKeys(func(key item.Key) item.Key {
		// 30 -> 30 and 50 -> 50
		return (key * 10) / 10
	}))

	require.Error(t, bs.ValidateBucketKeys(func(key item.Key) item.Key {
		return (key / 3) * 3
	}))

	require.NoError(t, bs.Close())
}

func TestBucketsDelete(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	bs, err := LoadAll(dir, opts)
	require.NoError(t, err)

	// Delete non-existing yet.
	require.Error(t, bs.delete(50))

	// Create bucket and delete again:
	_, err = bs.forKey(50)
	require.NoError(t, err)
	require.NoError(t, bs.delete(50))
	require.Error(t, bs.delete(50))
}

func TestBucketsNotEmptyDir(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	writeDummyBucket(t, dir, 33, testutils.GenItems(0, 10, 1))

	subDir := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(subDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "file"), []byte("Hello World!"), 0700))

	// Loading such a dir should error out as it seems that we try to open a directory with other things
	// in it that are not buckets at all. The caller can prepare this by having a os.Remove() of the contents,
	// but we should not do this automatically.
	opts := DefaultOptions()
	opts.MaxParallelOpenBuckets = 1
	_, err = LoadAll(dir, opts)
	require.Error(t, err)
}

func TestAPIBinsplit(t *testing.T) {
	t.Parallel()

	idFunc := func(k item.Key) item.Key { return k }

	items := item.Items{
		item.Item{Key: 0},
		item.Item{Key: 0},
		item.Item{Key: 0},
		item.Item{Key: 1},
		item.Item{Key: 1},
		item.Item{Key: 1},
	}

	require.Equal(t, 3, binsplit(items, 0, idFunc))
	require.Equal(t, 6, binsplit(items, 1, idFunc))
	require.Equal(t, 0, binsplit(item.Items{}, 0, idFunc))
}

func TestAPIBinsplitSeq(t *testing.T) {
	t.Parallel()

	idFunc := func(k item.Key) item.Key { return k }
	items := testutils.GenItems(0, 10, 1)
	for idx := 0; idx < len(items); idx++ {
		require.Equal(t, 1, binsplit(items[idx:], item.Key(idx), idFunc))
	}
}
