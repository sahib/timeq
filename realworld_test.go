//go:build slow
// +build slow

package timeq

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// We're going to use nanosecond epoch timestamps:
	keyOff = 1703242301745157676

	// Points usually come in "bursts", i.e. 10 point with timestamps
	// very close together and each burst being roughly 10ms from each other.
	burstSize = 10
)

func push(t *testing.T, rng *rand.Rand, q *Queue, batchIdx int64) {
	// Each batch should have a more or less random offset from the keyOff:
	batchOff := batchIdx * int64(2*time.Second+time.Duration(rand.Int63n(int64(500*time.Millisecond)))-500*time.Millisecond)

	// Length of each batch is also more or less random, but most of the time constant:
	batchLenOptions := []int64{
		2000,
		2000,
		2000,
		2000,
		2000,
		2000,
		1,
		32,
		768,
		1024,
		100,
	}

	batchLen := batchLenOptions[rng.Intn(len(batchLenOptions))]

	var batch Items

	for idx := int64(0); idx < batchLen; idx++ {
		burstOff := (idx / burstSize) * int64(10*time.Millisecond)
		key := keyOff + batchOff + burstOff + int64(idx%burstSize)

		// TODO: BUG: blob size may not be zero!
		blobSize := rng.Intn(100) + 1
		blob := make([]byte, blobSize)

		_, err := rng.Read(blob)
		require.NoError(t, err)

		item := Item{
			Key:  Key(key),
			Blob: blob,
		}

		batch = append(batch, item)
	}

	require.NoError(t, q.Push(batch))
}

func shovel(t *testing.T, waiting, unacked *Queue) {
	unackedLenBefore := unacked.Len()
	waitingLenBefore := waiting.Len()

	nshoveled, err := unacked.Shovel(waiting)
	require.NoError(t, err)

	unackedLenAfter := unacked.Len()
	waitingLenAfter := waiting.Len()

	require.Equal(t, 0, unackedLenAfter)
	require.Equal(t, unackedLenBefore+waitingLenBefore, waitingLenAfter)
	require.Equal(t, waitingLenAfter-waitingLenBefore, nshoveled)
}

func move(t *testing.T, waiting, unacked *Queue) {
	var lastKey Key
	var count int

	queueLenBefore := waiting.Len()
	const popSize = 2000
	dst := make(Items, popSize)
	require.NoError(t, waiting.Move(popSize, dst[:0], unacked, func(items Items) error {
		count += len(items)

		for idx, item := range items {
			if lastKey != 0 && item.Key < lastKey {
				diff := time.Duration(lastKey - item.Key)
				require.Fail(t, fmt.Sprintf(
					"item %d has lower key (%v) than the item before (%v) - diff: %v",
					idx,
					item.Key,
					lastKey,
					diff,
				))
			}

			lastKey = item.Key
		}

		return nil
	}))

	expect := popSize
	if popSize > queueLenBefore {
		expect = queueLenBefore
	}

	require.Equal(t, expect, count)
}

func ack(t *testing.T, rng *rand.Rand, waiting, unacked *Queue) {
	// Each batch should have a more or less random offset from the keyOff:
	deleteOff := Key(rng.Int63n(int64(time.Minute)) - int64(15*time.Second))

	var waitingOff Key
	require.NoError(t, waiting.Peek(1, nil, func(items Items) error {
		waitingOff = items[0].Key
		return nil
	}))

	var unackedOff Key
	require.NoError(t, unacked.Peek(1, nil, func(items Items) error {
		unackedOff = items[0].Key
		return nil
	}))

	var err error

	_, err = waiting.DeleteLowerThan(waitingOff + deleteOff)
	require.NoError(t, err)

	_, err = unacked.DeleteLowerThan(unackedOff + deleteOff)
	require.NoError(t, err)
}

func TestRealWorldAckQueue(t *testing.T) {
	// Still create test dir to make sure it does not error out because of that:
	dir, err := os.MkdirTemp("", "timeq-realworldtest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	rng := rand.New(rand.NewSource(0))

	opts := DefaultOptions()
	opts.BucketFunc = ShiftBucketFunc(30)
	opts.MaxParallelOpenBuckets = 4

	waitingDir := filepath.Join(dir, "waiting")
	unackedDir := filepath.Join(dir, "unacked")

	waitingQueue, err := Open(waitingDir, opts)
	require.NoError(t, err)

	unackedQueue, err := Open(unackedDir, opts)
	require.NoError(t, err)

	// Plan:
	//
	// - Push several times (varying sizes with real time, mostly increasing order)
	// - Move waiting -> unacked (verify results)
	// - Push again.
	// - Shovel from waiting to unacked
	// - Push.
	// - Another move round.
	// - Push.
	// - Delete some older message.
	// - Pop the rest.
	//
	// Basically the full lifecycle of an ack queue.

	for run := 0; run < 10; run++ {
		var batchIdx int64

		for idx := 0; idx < 10; idx++ {
			push(t, rng, waitingQueue, batchIdx)
			batchIdx++
		}

		for idx := 0; idx < 5; idx++ {
			move(t, waitingQueue, unackedQueue)
		}

		for idx := 0; idx < 10; idx++ {
			push(t, rng, waitingQueue, batchIdx)
			batchIdx++
		}

		shovel(t, waitingQueue, unackedQueue)

		for idx := 0; idx < 10; idx++ {
			push(t, rng, waitingQueue, batchIdx)
			batchIdx++
		}

		for idx := 0; idx < 5; idx++ {
			move(t, waitingQueue, unackedQueue)
		}

		ack(t, rng, waitingQueue, unackedQueue)

		for idx := 0; idx < 100; idx++ {
			move(t, waitingQueue, unackedQueue)
		}

		if run == 5 {
			// Re-open in between to make it a bit harder:
			require.NoError(t, waitingQueue.Close())
			require.NoError(t, unackedQueue.Close())

			waitingQueue, err = Open(waitingDir, opts)
			require.NoError(t, err)

			unackedQueue, err = Open(unackedDir, opts)
			require.NoError(t, err)
		}
	}

	require.NoError(t, waitingQueue.Close())
	require.NoError(t, unackedQueue.Close())
}
