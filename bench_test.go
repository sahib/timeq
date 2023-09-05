package timeq

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
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
