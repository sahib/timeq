package timeq

import (
	"fmt"
	"slices"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
)

type Item = item.Item
type Items []Item
type Key = item.Key

func Trunc30mBuckets(key Key) Key {
	// This should yield roughly 30m buckets.
	// (and saves us expensive divisions)
	return key & (^item.Key(0) << 40)
}

type Options struct {
	bucket.Options
	TruncFunc func(Key) Key
	// TODO: Add option for sync mode here and write docs.
}

func DefaultOptions() Options {
	return Options{
		Options:   bucket.DefaultOptions(),
		TruncFunc: Trunc30mBuckets,
	}
}

type Queue struct {
	buckets *bucket.Buckets
	opts    Options
}

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

func (q *Queue) Push(items Items) error {
	slices.SortFunc(items, func(i, j item.Item) int {
		return int(i.Key - j.Key)
	})

	// Sort items into the respective buckets:
	var lastKeyMod item.Key
	var lastKeyIdx int
	for idx := 0; idx < len(items); idx++ {
		keyMod := q.opts.TruncFunc(items[idx].Key)
		if keyMod == lastKeyMod {
			continue
		}

		buck, err := q.buckets.ByKey(keyMod)
		if err != nil {
			return err
		}

		if err := buck.Push(items[lastKeyIdx:idx]); err != nil {
			return err
		}

		lastKeyMod = keyMod
		lastKeyIdx = idx
	}

	return nil
}

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
	var deleted int
	var err error
	return deleted, q.buckets.Iter(func(b *bucket.Bucket) error {
		deleted, err = b.DeleteLowerThan(key)
		return err
	})
}

func (q *Queue) Size() int {
	var size int
	q.buckets.Iter(func(b *bucket.Bucket) error {
		size += b.Size()
		return nil
	})

	return size
}

func (q *Queue) Sync() error {
	return q.buckets.Iter(func(b *bucket.Bucket) error {
		return b.Sync()
	})
}

func (q *Queue) Close() error {
	return q.buckets.Iter(func(b *bucket.Bucket) error {
		return b.Close()
	})
}
