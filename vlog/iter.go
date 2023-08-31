package vlog

import "github.com/sahib/lemurq/item"

type LogIter struct {
	key       item.Key
	currOff   item.Off
	currLen   item.Off
	item      item.Item
	exhausted bool
	log       *Log
	err       error
}

// TODO: rather pass item as arg to Next()?
func (li LogIter) Next() bool {
	if li.currLen == 0 || li.exhausted {
		li.exhausted = true
		return false
	}

	it, err := li.log.at(li.currOff)
	if err != nil {
		li.err = err
		li.exhausted = true
		return false
	}

	// advance iter to next position:
	li.currOff += item.Off(len(it.Blob) + itemHeaderSize)
	li.currLen--
	li.item = it
	return true
}

func (li LogIter) Exhausted() bool {
	return li.exhausted
}

// Key returns the key this iterator was created with
// This is not the current key of the item!
func (li LogIter) Key() item.Key {
	return li.key
}

func (li LogIter) Item() item.Item {
	return li.item
}

func (li LogIter) CurrentLocation() item.Location {
	return item.Location{
		Key:  li.item.Key,
		Off:  li.currOff,
		Len:  li.currLen,
	}
}

func (li LogIter) Err() error {
	return li.err
}

////////////////

type LogIters []LogIter

// Make LogIter usable by heap.Interface
func (ls LogIters) Len() int      { return len(ls) }
func (ls LogIters) Swap(i, j int) { ls[i], ls[j] = ls[j], ls[i] }
func (ls LogIters) Push(x any)    {}
func (ls LogIters) Pop() any      { return nil }
func (ls LogIters) Less(i, j int) bool {
	if ls[i].exhausted != ls[j].exhausted {
		// sort exhausted iters to the back
		return !ls[i].exhausted
	}

	return ls[i].item.Key < ls[j].item.Key
}
