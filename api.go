package timeq

import (
	"fmt"
	"slices"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
)

// Item is a single item that you push or pop from the queue.
type Item = item.Item

// Items is a list of items.
type Items []Item

// Key is the priority of each item in the queue.
// Lower keys will be popped first.
type Key = item.Key

// DefaultBucketFunc assumes that `key` is a nanosecond unix timestamps
// and divides data (roughly) in 15 minute buckets.
func DefaultBucketFunc(key Key) Key {
	// This should yield roughly 15m buckets.
	// (and saves us expensive divisions)
	return key & (^item.Key(0) << 39)
}

// Options gives you some knobs to configure the queue.
// Read the individual options carefully, as some of them
// can only be set on the first call to Open()
type Options struct {
	bucket.Options

	// BucketFunc defines what key goes to what bucket.
	// The provided function should clamp the key value to
	// a common value. Each same value that was returned goes
	// into the same bucket.
	//
	// NOTE: This may not be changed after you opened a queue with it!
	//       Only way to change is to create a new queue and shovel the
	//       old data into it.
	BucketFunc func(Key) Key
}

// DefaultOptions give you a set of options that are good to enough
// to try some expirements. Your mileage can vary a lot with different settings!
func DefaultOptions() Options {
	return Options{
		Options:    bucket.DefaultOptions(),
		BucketFunc: DefaultBucketFunc,
	}
}

// Queue is the high level API to the priority queue.
type Queue struct {
	buckets *bucket.Buckets
	opts    Options
}

// Open tries to open the priority queue structure in `dir`.
// If `dir` does not exist, then a new, empty priority queue is created.
// The behavior of the queue can be fine-tuned with `opts`.
func Open(dir string, opts Options) (*Queue, error) {
	bs, err := bucket.LoadAll(dir, opts.Options)
	if err != nil {
		return nil, err
	}

	return &Queue{
		opts:    opts,
		buckets: bs,
	}, nil
}

// Push pushes a batch of `items` to the queue.
func (q *Queue) Push(items Items) error {
	slices.SortFunc(items, func(i, j item.Item) int {
		return int(i.Key - j.Key)
	})

	// Sort items into the respective buckets:
	var lastKeyMod item.Key
	var lastKeyIdx int
	for idx := 0; idx < len(items); idx++ {
		keyMod := q.opts.BucketFunc(items[idx].Key)
		if keyMod == lastKeyMod && idx != len(items)-1 {
			continue
		}

		buck, err := q.buckets.ByKey(keyMod)
		if err != nil {
			return err
		}

		lastElemIdx := idx
		if idx == len(items)-1 {
			lastElemIdx = len(items)
		}

		if err := buck.Push(items[lastKeyIdx:lastElemIdx]); err != nil {
			return err
		}

		lastKeyMod = keyMod
		lastKeyIdx = idx
	}

	return nil
}

// Pop fetches up to `n` items from the queue.
// It will only return less items if the queue does not hold more items.
// If an error occured no items are returned.
//
// The `dst` argument can be used to pass a pre-allocated slice that
// the queue appends to. This can be done to avoid allocations.
// If you don't care you can also pass nil.
func (q *Queue) Pop(n int, dst Items) (Items, error) {
	var count = n
	var toBeDeleted []*bucket.Bucket

	err := q.buckets.Iter(func(b *bucket.Bucket) error {
		newDst, popped, err := b.Pop(count, dst)
		if err != nil {
			return err
		}

		if b.Empty() {
			toBeDeleted = append(toBeDeleted, b)
		}

		dst = newDst
		count -= popped
		if count <= 0 {
			return bucket.IterStop
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Delete buckets that were exhausted:
	for _, bucket := range toBeDeleted {
		if err := q.buckets.Delete(bucket.Key()); err != nil {
			return dst, fmt.Errorf("bucket delete: %w", err)
		}
	}

	return dst, nil
}

// DeleteLowerThan deletes all items lower than `key`.
func (q *Queue) DeleteLowerThan(key Key) (int, error) {
	var numDeleted int
	var deletableBucks []*bucket.Bucket

	err := q.buckets.Iter(func(bucket *bucket.Bucket) error {
		numDeletedOfBucket, err := bucket.DeleteLowerThan(key)
		if err != nil {
			return err
		}

		numDeleted += numDeletedOfBucket
		if bucket.Empty() {
			deletableBucks = append(deletableBucks, bucket)
		}

		return nil
	})

	if err != nil {
		return numDeleted, err
	}

	for _, bucket := range deletableBucks {
		if err := q.buckets.Delete(bucket.Key()); err != nil {
			return numDeleted, fmt.Errorf("bucket delete: %w", err)
		}
	}

	return numDeleted, nil
}

// Size returns the number of items in the queue.
// NOTE: This is a little more expensive than a simple getter.
// You should avoid calling this in a hot loop.
func (q *Queue) Size() int {
	var size int
	_ = q.buckets.Iter(func(b *bucket.Bucket) error {
		size += b.Size()
		return nil
	})

	return size
}

// Sync can be called to explicitly sync the queue contents
// to persistent storage, even if you configured SyncNone.
func (q *Queue) Sync() error {
	return q.buckets.Iter(func(b *bucket.Bucket) error {
		return b.Sync(true)
	})
}

func (q *Queue) Clear() error {
	return q.buckets.Iter(func(b *bucket.Bucket) error {
		// TODO: Can we iterate and delete?
		return q.buckets.Delete(b.Key())
	})
}

func (q *Queue) Shovel(src *Queue) error {
	// TODO: Shovel
	return nil
}

// Close should always be called and error checked when you're done
// with using the queue. Close might still flush out some data, depending
// on what sync mode you configured.
func (q *Queue) Close() error {
	return q.buckets.Iter(func(b *bucket.Bucket) error {
		return b.Close()
	})
}

// Shovel moves items from `src` to `dst`. The `src` queue will be completely drained
// afterwards. If items in the `dst` queue exists with the same timestamp, then they
// will be overwritten.
//
// This method can be used if you want to change options like the BucketFunc or if you
// intend to have more than one queue that are connected by some logic. Examples for the
// latter case would be a "deadletter queue" where you put failed calculations for later
// recalucations or a queue for unacknowledged items.
//
// NOTE: This operation is currently not implemented atomic. Data might be lost
// if a crash occurs between pop and push.
func Shovel(src, dst *Queue) (int, error) {
	buf := make(Items, 0, 2000)
	numPopped := 0
	for {
		items, err := src.Pop(cap(buf), buf[:0])
		if err != nil {
			return numPopped, fmt.Errorf("timeq: shovel-pop: %w", err)
		}

		numPopped += len(items)
		if len(items) == 0 {
			break
		}

		if err := dst.Push(items); err != nil {
			return numPopped, fmt.Errorf("timeq: shovel-push: %w", err)
		}

		if len(items) < cap(buf) {
			break
		}
	}

	return numPopped, nil
}
