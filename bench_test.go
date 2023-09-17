package timeq

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func benchmarkPushPopWithSyncMode(b *testing.B, benchmarkPush bool, syncMode bucket.SyncMode) {
	dir, err := os.MkdirTemp("", "timeq-buckettest")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.SyncMode = syncMode

	bucketDir := filepath.Join(dir, item.Key(23).String())
	queue, err := Open(bucketDir, opts)
	require.NoError(b, err)

	// Add some dummy data:
	items := make([]item.Item, 2000)
	dstItems := make([]item.Item, 0, 2000)
	timeoff := 0

	b.ResetTimer()
	for run := 0; run < b.N; run++ {
		b.StopTimer()
		for idx := 0; idx < len(items); idx++ {
			// use a realistic size for each message:
			var buf [40]byte
			for pos := 0; pos < cap(buf); pos += 8 {
				binary.BigEndian.PutUint64(buf[pos:], uint64(timeoff+idx))
			}

			items[idx].Key = item.Key(timeoff + idx)
			items[idx].Blob = buf[:]
		}

		timeoff += len(items)
		if benchmarkPush {
			b.StartTimer()
		}
		require.NoError(b, queue.Push(items))
		if benchmarkPush {
			b.StopTimer()
		}

		if !benchmarkPush {
			b.StartTimer()
		}
		dstItems = dstItems[:0]
		_, err := queue.Pop(len(items), dstItems[:0])
		if !benchmarkPush {
			b.StopTimer()
		}
		require.NoError(b, err)
	}

	require.NoError(b, queue.Close())
}

func BenchmarkPopSyncNone(b *testing.B)   { benchmarkPushPopWithSyncMode(b, false, bucket.SyncNone) }
func BenchmarkPopSyncData(b *testing.B)   { benchmarkPushPopWithSyncMode(b, false, bucket.SyncData) }
func BenchmarkPopSyncIndex(b *testing.B)  { benchmarkPushPopWithSyncMode(b, false, bucket.SyncIndex) }
func BenchmarkPopSyncFull(b *testing.B)   { benchmarkPushPopWithSyncMode(b, false, bucket.SyncFull) }
func BenchmarkPushSyncNone(b *testing.B)  { benchmarkPushPopWithSyncMode(b, true, bucket.SyncNone) }
func BenchmarkPushSyncData(b *testing.B)  { benchmarkPushPopWithSyncMode(b, true, bucket.SyncData) }
func BenchmarkPushSyncIndex(b *testing.B) { benchmarkPushPopWithSyncMode(b, true, bucket.SyncIndex) }
func BenchmarkPushSyncFull(b *testing.B)  { benchmarkPushPopWithSyncMode(b, true, bucket.SyncFull) }

var globItems Items

func BenchmarkCopyItems(b *testing.B) {
	items := make(Items, 2000)
	for idx := 0; idx < len(items); idx++ {
		// use a realistic size for each message:
		var buf [40]byte
		for pos := 0; pos < cap(buf); pos += 8 {
			binary.BigEndian.PutUint64(buf[pos:], uint64(idx))
		}

		items[idx].Key = item.Key(idx)
		items[idx].Blob = buf[:]
	}

	b.Run("copy-naive-with-alloc", func(b *testing.B) {
		b.ResetTimer()
		for run := 0; run < b.N; run++ {
			globItems = items.Copy()
		}
	})

	c := make(Items, 2000)
	pseudoMmap := make([]byte, 2000*40)

	// Difference to above bench: It does not allocate anything
	// during the benchmark.
	b.Run("copy-with-pseudo-mmap", func(b *testing.B) {
		b.ResetTimer()
		for run := 0; run < b.N; run++ {
			// global variable to stop the compiler
			// from optimizing the call away:
			// globItems = items.Copy()
			off := 0
			for idx := 0; idx < len(items); idx++ {

				c[idx] = items[idx]
				s := pseudoMmap[off : off+40]
				copy(s, items[idx].Blob)
				c[idx].Blob = s
			}

			globItems = c
		}
	})
}

var globalKey Key

func BenchmarkDefaultBucketFunc(b *testing.B) {
	b.Run("default", func(b *testing.B) {
		globalKey = 23
		for run := 0; run < b.N; run++ {
			globalKey = DefaultBucketFunc(globalKey)
		}
	})

	b.Run("baseline", func(b *testing.B) {
		globalKey = 23
		const div = 9 * 60 * 1e9
		for run := 0; run < b.N; run++ {
			globalKey = (globalKey / div) * div
		}
	})
}

func BenchmarkShovel(b *testing.B) {
	b.StopTimer()

	dir, err := os.MkdirTemp("", "timeq-shovelbench")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	srcDir := filepath.Join(dir, "src")
	dstDir := filepath.Join(dir, "dst")
	srcQueue, err := Open(srcDir, DefaultOptions())
	require.NoError(b, err)
	dstQueue, err := Open(dstDir, DefaultOptions())
	require.NoError(b, err)

	for run := 0; run < b.N; run++ {
		require.NoError(b, srcQueue.Push(testutils.GenItems(0, 2000, 1)))
		b.StartTimer()
		_, err := Shovel(srcQueue, dstQueue)
		require.NoError(b, err)
		b.StopTimer()
		require.NoError(b, dstQueue.Clear())
	}

	defer srcQueue.Close()
	defer dstQueue.Close()
}
