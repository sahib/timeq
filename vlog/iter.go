package vlog

import (
	"github.com/sahib/timeq/item"
)

type Iter struct {
	firstKey         item.Key
	currOff, currLen item.Off
	prevOff, prevLen item.Off
	item             item.Item
	exhausted        bool
	log              *Log
	err              error
	continueOnErr    bool
}

func (li *Iter) Next(itDst *item.Item) bool {
	if li.currLen == 0 || li.exhausted {
		li.exhausted = true
		return false
	}

	if len(li.log.mmap) > 0 && li.currOff >= item.Off(li.log.size) {
		// stop iterating when end of log reached.
		li.exhausted = true
		return false
	}

	for {
		if err := li.log.readItemAt(li.currOff, &li.item); err != nil {
			if !li.continueOnErr {
				li.err = err
				li.exhausted = true
				return false
			}

			li.currOff = li.log.findNextItem(li.currOff)
			if li.currOff >= item.Off(li.log.size) {
				li.exhausted = true
				return false
			}

			continue
		}

		break
	}

	if len(li.item.Blob) == 0 {
		// this can happen at the very end of the log
		// (if not iterating in "batch mode")
		li.exhausted = true
		return false
	}

	li.prevOff = li.currOff
	li.prevLen = li.currLen

	// advance iter to next position:
	li.currOff += item.Off(li.item.StorageSize())
	li.currLen--

	if itDst != nil {
		*itDst = li.item
	}

	return true
}

func (li *Iter) Exhausted() bool {
	return li.exhausted
}

// Key returns the key this iterator was created with
// This is not the current key of the item!
func (li *Iter) FirstKey() item.Key {
	return li.firstKey
}

// Item returns the current item.
// It is not valid before Next() has been called.
func (li *Iter) Item() item.Item {
	return li.item
}

// CurrentLocation returns the location of the current entry.
// It is not valid before Next() has been called.
func (li *Iter) CurrentLocation() item.Location {
	return item.Location{
		Key: li.item.Key,
		Off: li.prevOff,
		Len: li.prevLen,
	}
}

func (li *Iter) Err() error {
	return li.err
}

////////////////

type Iters []Iter

// Make LogIter usable by heap.Interface
func (ls Iters) Len() int      { return len(ls) }
func (ls Iters) Swap(i, j int) { ls[i], ls[j] = ls[j], ls[i] }
func (ls Iters) Less(i, j int) bool {
	ii, ij := ls[i], ls[j]
	if ii.exhausted != ij.exhausted {
		// sort exhausted iters to the back
		return !ii.exhausted
	}

	ki, kj := ii.item.Key, ij.item.Key
	return ki < kj
}

func (ls *Iters) Push(x any) {
	*ls = append(*ls, x.(Iter))
}

func (ls *Iters) Pop() any {
	// NOTE: This is currently unused.
	old := *ls
	n := len(old)
	x := old[n-1]
	*ls = old[0 : n-1]
	return x
}
