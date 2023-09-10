package vlog

import (
	"github.com/sahib/timeq/item"
)

type LogIter struct {
	key              item.Key
	currOff, currLen item.Off
	prevOff, prevLen item.Off
	item             item.Item
	exhausted        bool
	log              *Log
	err              error
}

func (li *LogIter) Next(itDst *item.Item) bool {
	if li.currLen == 0 || li.exhausted {
		li.exhausted = true
		return false
	}

	if len(li.log.mmap) > 0 && li.currOff >= item.Off(li.log.size) {
		// stop iterating when end of log reached.
		li.exhausted = true
		return false
	}

	if err := li.log.readItemAt(li.currOff, &li.item); err != nil {
		li.err = err
		li.exhausted = true
		return false
	}

	if len(li.item.Blob) == 0 {
		// this can happen at the very end of the log:
		li.exhausted = true
		return false
	}

	li.prevOff = li.currOff
	li.prevLen = li.currLen

	// advance iter to next position:
	li.currOff += item.Off(len(li.item.Blob) + ItemHeaderSize)
	li.currLen--

	if itDst != nil {
		*itDst = li.item
	}

	return true
}

func (li *LogIter) Exhausted() bool {
	return li.exhausted
}

// Key returns the key this iterator was created with
// This is not the current key of the item!
func (li *LogIter) FirstKey() item.Key {
	return li.key
}

// Item returns the current item.
// It is not valid before Next() has been called.
func (li *LogIter) Item() item.Item {
	return li.item
}

// CurrentLocation returns the location of the current entry.
// It is not valid before Next() has been called.
func (li *LogIter) CurrentLocation() item.Location {
	return item.Location{
		Key: li.item.Key,
		Off: li.prevOff,
		Len: li.prevLen,
	}
}

func (li *LogIter) Err() error {
	return li.err
}

////////////////

type LogIters []LogIter

// Make LogIter usable by heap.Interface
func (ls LogIters) Len() int      { return len(ls) }
func (ls LogIters) Swap(i, j int) { ls[i], ls[j] = ls[j], ls[i] }
func (ls LogIters) Less(i, j int) bool {
	ii, ij := ls[i], ls[j]
	if ii.exhausted != ij.exhausted {
		// sort exhausted iters to the back
		return !ls[i].exhausted
	}

	ki, kj := ii.item.Key, ij.item.Key
	return ki < kj
}

func (ls *LogIters) Push(x any) {
	*ls = append(*ls, x.(LogIter))
}

func (ls *LogIters) Pop() any {
	old := *ls
	n := len(old)
	x := old[n-1]
	*ls = old[0 : n-1]
	return x
}
