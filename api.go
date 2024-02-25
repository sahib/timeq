// Package timeq  is a file-based priority queue in Go.
package timeq

import (
	"fmt"

	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
)

// Item is a single item that you push or pop from the queue.
type Item = item.Item

// Items is a list of items.
type Items = item.Items

// Key is the priority of each item in the queue.
// Lower keys will be popped first.
type Key = item.Key

// ReadFn is the type for functions passed to Pop/Move/Peek
type ReadFn = bucket.ReadFn

// Options gives you some knobs to configure the queue.
// Read the individual options carefully, as some of them
// can only be set on the first call to Open()
type Options = bucket.Options

// DefaultOptions give you a set of options that are good to enough to try some
// experiments. Your mileage can vary a lot with different settings, so make
// sure to do some benchmarking!
var DefaultOptions = bucket.DefaultOptions

// DefaultBucketFunc assumes that `key` is a nanosecond unix timestamps
// and divides data (roughly) in 2m minute buckets.
var DefaultBucketFunc = bucket.DefaultBucketFunc

// ShiftBucketFunc creates a fast BucketFunc that divides data into buckets
// by masking `shift` less significant bits of the key. With a shift
// of 37 you roughly get 2m buckets (if your key input are nanosecond-timestamps).
// If you want to calculate the size of a shift, use this formula:
// (2 ** shift) / (1e9 / 60) = minutes
var ShiftBucketFunc = bucket.ShiftBucketFunc

// FixedSizeBucketFunc returns a BucketFunc that divides buckets into
// equal sized buckets with `n` entries. This can also be used to create
// time-based keys, if you use nanosecond based keys and pass time.Minute
// to create a buckets with a size of one minute.
var FixedSizeBucketFunc = bucket.FixedSizeBucketFunc

// Queue is the high level API to the priority queue.
type Queue struct {
	buckets *bucket.Buckets
}

// Open tries to open the priority queue structure in `dir`.
// If `dir` does not exist, then a new, empty priority queue is created.
// The behavior of the queue can be fine-tuned with `opts`.
func Open(dir string, opts Options) (*Queue, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	bs, err := bucket.LoadAll(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("buckets: %w", err)
	}

	if err := bs.ValidateBucketKeys(opts.BucketFunc); err != nil {
		return nil, err
	}

	return &Queue{buckets: bs}, nil
}

// Push pushes a batch of `items` to the queue.
func (q *Queue) Push(items Items) error {
	return q.buckets.Push(items)
}

// Pop fetches up to `n` items from the queue. It will call the supplied `fn`
// one or several times until either `n` is reached or the queue is empty. If
// the queue is empty before calling Pop(), then `fn` is not called. If `n` is
// negative, then as many items as possible are returned until the queue is
// empty.
//
// The `dst` argument can be used to pass a preallocated slice that
// the queue appends to. This can be done to avoid allocations.
// If you don't care you can also simply pass nil.
//
// You should NEVER use the supplied items outside of `fn`, as they
// are directly sliced from a mmap(2). Accessing them outside will
// almost certainly lead to a crash. If you need them outside (e.g. for
// appending to a slice) then you can use the Copy() function of Items.
func (q *Queue) Pop(n int, dst Items, fn ReadFn) error {
	return q.buckets.Read(bucket.ReadOpPop, n, dst, "", fn, nil)
}

// Peek works like Pop, but does not delete the items in the queue.
// Please read the documentation of Pop() too.
func (q *Queue) Peek(n int, dst Items, fn ReadFn) error {
	return q.buckets.Read(bucket.ReadOpPeek, n, dst, "", fn, nil)
}

// Move works like Pop, but it pushes the popped items to `dstQueue` immediately.
// This implementation is safer than one that is build on this external API,
// as it deletes the popped data only when the push was successful.
// Please read the documentation of Pop() too.
func (q *Queue) Move(n int, dst Items, dstQueue *Queue, fn ReadFn) error {
	return q.buckets.Read(bucket.ReadOpMove, n, dst, "", fn, dstQueue.buckets)
}

// Delete deletes all items in the range `from` to `to`.
// Both `from` and `to` are including, i.e. keys with this value are deleted.
// The number of deleted items is returned.
func (q *Queue) Delete(from, to Key) (int, error) {
	return q.buckets.Delete("", from, to)
}

// Len returns the number of items in the queue.
// NOTE: This gets more expensive when you have a higher number of buckets,
// so you probably should not call that in a hot loop.
func (q *Queue) Len() int {
	return q.buckets.Len("")
}

// Sync can be called to explicitly sync the queue contents
// to persistent storage, even if you configured SyncNone.
func (q *Queue) Sync() error {
	return q.buckets.Sync()
}

// Clear fully deletes the queue contents.
func (q *Queue) Clear() error {
	return q.buckets.Clear()
}

// Shovel moves items from `src` to `dst`. The `src` queue will be completely drained
// afterwards. For speed reasons this assume that the dst queue uses the same bucket func
// as the source queue. If you cannot guarantee this, you should implement a naive Shovel()
// implementation that just uses Pop/Push.
//
// This method can be used if you want to change options like the BucketFunc or if you
// intend to have more than one queue that are connected by some logic. Examples for the
// latter case would be a "deadletter queue" where you put failed calculations for later
// re-calculations or a queue for unacknowledged items.
func (q *Queue) Shovel(dst *Queue) (int, error) {
	return q.buckets.Shovel(dst.buckets, "")
}

// Fork splits the reading end of the queue in two parts. If Pop() is
// called on the returned Fork (which implements the Consumer interface),
// then other forks and the original queue is not affected.
//
// The process of forking is relatively cheap and adds only minor storage and
// memory cost to the queue as a whole. Performance during pushing and popping
// is almost not affected at all.
func (q *Queue) Fork(name ForkName) (*Fork, error) {
	if err := q.buckets.Fork("", name); err != nil {
		return nil, err
	}

	return &Fork{name: name, q: q}, nil
}

// Forks returns a list of fork names. The list will be empty if there are no forks yet.
// In other words: The initial queue is not counted as fork.
func (q *Queue) Forks() []ForkName {
	return q.buckets.Forks()
}

// Close should always be called and error checked when you're done
// with using the queue. Close might still flush out some data, depending
// on what sync mode you configured.
func (q *Queue) Close() error {
	return q.buckets.Close()
}

// PopCopy works like a simplified Pop() but copies the items. It is less
// efficient and should not be used if you care for performance.
func PopCopy(c Consumer, n int) (Items, error) {
	var items Items
	return items, c.Pop(n, nil, func(popped Items) error {
		items = append(items, popped.Copy()...)
		return nil
	})
}

// PeekCopy works like a simplified Peek() but copies the items. It is less
// efficient and should not be used if you care for performance.
func PeekCopy(c Consumer, n int) (Items, error) {
	var items Items
	return items, c.Peek(n, nil, func(popped Items) error {
		items = append(items, popped.Copy()...)
		return nil
	})
}

// MoveCopy works like a simplified Move() but copies the items. It is less
// efficient and should not be used if you care for performance.
func MoveCopy(c Consumer, n int, dst *Queue) (Items, error) {
	var items Items
	return items, c.Move(n, nil, dst, func(popped Items) error {
		items = append(items, popped.Copy()...)
		return nil
	})
}

/////////////

// ForkName is the name of a fork. This is a special type to make it more
// explicit that not every string is a valid fork name. Only alphanumeric
// characters, dashes and underscores are allowed.
type ForkName = bucket.ForkName

// Fork is an implementation of the Consumer interface for a named fork.
// See the Fork() method for more explanation.
type Fork struct {
	name ForkName
	q    *Queue
}

var (
	// ErrNoSuchFork is returned whenever the name of a fork is not known,
	// or if the fork was deleted already.
	ErrNoSuchFork = bucket.ErrNoSuchFork
)

// Consumer is an interface that both Fork and Queue implement.
// It covers every consumer related API. Please refer to the respective
// Queue methods for details.
type Consumer interface {
	Pop(n int, dst Items, fn ReadFn) error
	Peek(n int, dst Items, fn ReadFn) error
	Move(n int, dst Items, dstQueue *Queue, fn ReadFn) error
	Delete(from, to Key) (int, error)
	Shovel(dst *Queue) (int, error)
	Len() int
	Fork(name ForkName) (*Fork, error)
}

// Check that Queue also implements the Consumer interface.
var _ Consumer = &Queue{}

// Pop is like Queue.Pop().
func (f *Fork) Pop(n int, dst Items, fn ReadFn) error {
	if f.q == nil {
		return ErrNoSuchFork
	}

	return f.q.buckets.Read(bucket.ReadOpPop, n, dst, f.name, fn, nil)
}

// Peek is like Queue.Peek().
func (f *Fork) Peek(n int, dst Items, fn ReadFn) error {
	if f.q == nil {
		return ErrNoSuchFork
	}
	return f.q.buckets.Read(bucket.ReadOpPeek, n, dst, f.name, fn, nil)
}

// Move is like Queue.Move().
func (f *Fork) Move(n int, dst Items, dstQueue *Queue, fn ReadFn) error {
	if f.q == nil {
		return ErrNoSuchFork
	}
	return f.q.buckets.Read(bucket.ReadOpMove, n, dst, f.name, fn, dstQueue.buckets)
}

// Len is like Queue.Len().
func (f *Fork) Len() int {
	if f.q == nil {
		return 0
	}

	// ignore the error, as it can only happen with bad consumer name.
	return f.q.buckets.Len(f.name)
}

// Delete is like Queue.Delete().
func (f *Fork) Delete(from, to Key) (int, error) {
	if f.q == nil {
		return 0, ErrNoSuchFork
	}

	return f.q.buckets.Delete(f.name, from, to)
}

// Remove removes this fork. If the fork is used after this, the API
// will return ErrNoSuchFork in all cases.
func (f *Fork) Remove() error {
	if f.q == nil {
		return ErrNoSuchFork
	}

	q := f.q
	f.q = nil // mark self as deleted.
	return q.buckets.RemoveFork(f.name)
}

// Shovel is like Queue.Shovel(). The data of the current fork
// is pushed to the `dst` queue.
func (f *Fork) Shovel(dst *Queue) (int, error) {
	if f.q == nil {
		return 0, ErrNoSuchFork
	}
	return f.q.buckets.Shovel(dst.buckets, f.name)
}

// Fork is like Queue.Fork(), except that the fork happens relative to the
// current state of the consumer and not to the state of the underlying Queue.
func (f *Fork) Fork(name ForkName) (*Fork, error) {
	if err := f.q.buckets.Fork(f.name, name); err != nil {
		return nil, err
	}

	return &Fork{name: name, q: f.q}, nil
}
