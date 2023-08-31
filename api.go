package lemurq

import (
	"slices"

	"github.com/sahib/lemurq/bucket"
	"github.com/sahib/lemurq/item"
)

// TODO: own type for Key, Off and IndexID

// TODO: find out how to convince the compiler
// how to convert this
type Item item.Item

func keyTrunc(key item.Key) item.Key {
	// This should yield roughly 30m buckets.
	// (and saves us expensive divisions)
	return key & (^item.Key(0) << 40)
}

type Queue struct {
	buckets *bucket.Buckets
}

func Open(dir string) (*Queue, error) {
	bs, err := bucket.LoadAll(dir)
	if err != nil {
		return nil, err
	}

	return &Queue{buckets: bs}, nil
}

func (q *Queue) Push(items []item.Item) error {
	slices.SortFunc(items, func(i, j item.Item) int {
		return int(i.Key - j.Key)
	})

	// Sort items into the respective buckets:
	var lastKeyMod item.Key
	var lastKeyIdx int
	for idx := 0; idx < len(items); idx++ {
		keyMod := keyTrunc(items[idx].Key)
		if keyMod != lastKeyMod {
			b := q.buckets.ByKey(keyMod)
			if b == nil {
				continue
			}

			b.Push(items[lastKeyIdx:idx])
			lastKeyMod = keyMod
			lastKeyIdx = idx
		}
	}
	return nil
}

func (q *Queue) Pop(n int, dst []item.Item) ([]item.Item, error) {
	count := n
	return dst, q.buckets.Iter(func(b *bucket.Bucket) error {
		var err error
		dst, popped, err = b.Pop(count, dst)
		if err != nil {
			return err
		}

		count -= popped
		if count <= 0 {
			return bucket.IterStop
		}

		return nil
	})
}

func (q *Queue) ClearUntil(key int64) error {
	return nil
}

func (q *Queue) Size() int {
	return 0
}

func (q *Queue) Flush() error {
	return nil
}

func (q *Queue) Close() error {
	return nil
}
