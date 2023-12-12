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

	bucket, err := Open(bucketDir, DefaultOptions())
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

	bs, err := LoadAll(dir, 1, DefaultOptions())
	require.NoError(t, err)
	require.Equal(t, 0, bs.Len())
	require.NoError(t, bs.Sync())
	require.NoError(t, bs.Close())
}

func TestBucketsClearEmpty(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	bs, err := LoadAll(dir, 1, DefaultOptions())
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

	bs, err := LoadAll(dir, 1, DefaultOptions())
	require.NoError(t, err)

	// load bucket 80 early to check if iter can handle
	// already loaded buckets too.
	_, err = bs.ForKey(80)
	require.NoError(t, err)

	got := []item.Key{}
	require.NoError(t, bs.Iter(Load, func(key item.Key, b *Bucket) error {
		got = append(got, b.Key())
		require.Equal(t, key, b.Key())
		return nil
	}))

	require.Equal(t, expected, got)
	require.Equal(t, 40, bs.Len())
}

func TestBucketsForKey(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	writeDummyBucket(t, dir, 33, testutils.GenItems(0, 10, 1))
	bs, err := LoadAll(dir, 1, DefaultOptions())
	require.NoError(t, err)

	// open freshly:
	b1, err := bs.ForKey(32)
	require.NoError(t, err)
	require.Equal(t, item.Key(32), b1.Key())

	// open again, must be the same memory:
	b2, err := bs.ForKey(32)
	require.NoError(t, err)
	require.Equal(t, item.Key(32), b2.Key())
	require.Equal(t, b1, b2)

	// existing bucket should load fine:
	b3, err := bs.ForKey(33)
	require.NoError(t, err)
	require.Equal(t, item.Key(33), b3.Key())
	require.Equal(t, 10, b3.Len())

	require.NoError(t, bs.Clear())

	// open again, must be the same memory:

	b3c, err := bs.ForKey(33)
	require.NoError(t, err)
	require.Equal(t, item.Key(33), b3c.Key())
	require.Equal(t, 0, b3c.Len())
	require.NoError(t, bs.Close())
}

func TestBucketsValidateFunc(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-bucketstest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	writeDummyBucket(t, dir, 30, testutils.GenItems(30, 40, 1))
	writeDummyBucket(t, dir, 50, testutils.GenItems(50, 60, 1))

	bs, err := LoadAll(dir, 1, DefaultOptions())
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

	bs, err := LoadAll(dir, 1, DefaultOptions())
	require.NoError(t, err)

	// Delete non-existing yet.
	require.Error(t, bs.Delete(50))

	// Create bucket and delete again:
	_, err = bs.ForKey(50)
	require.NoError(t, err)
	require.NoError(t, bs.Delete(50))
	require.Error(t, bs.Delete(50))
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
	_, err = LoadAll(dir, 1, DefaultOptions())
	require.Error(t, err)
}
