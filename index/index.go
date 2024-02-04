package index

import (
	"os"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
	"github.com/tidwall/btree"
)

// Index is an in-memory representation of the batch index as b-tree structure.
type Index struct {
	m        btree.Map[item.Key, []item.Location]
	len      item.Off
	nentries item.Off
}

// FromVlog produces an index from the data in the value log. It's
// main use is to re-generate the index in case the index file is
// damaged or broken in some way. The resulting index is likely not
// the same as before, but will include items that were popped already.
func FromVlog(log *vlog.Log) (*Index, error) {
	// we're cheating a little here by trusting the iterator
	// to go not over the end, even if the Len is bogus.
	iter := log.At(item.Location{
		Off: 0,
		Len: ^item.Off(0),
	}, true)

	index := &Index{}

	var prevLoc item.Location
	var lastLoc item.Location
	var isInitialItem = true

	// Go over the data and try to find runs of data that are sorted in
	// ascending order. Each deviant item is the start of a new run.
	for iter.Next() {
		it := iter.Item()
		if prevLoc.Key > it.Key {
			index.Set(lastLoc)
			lastLoc.Off = prevLoc.Off
			lastLoc.Key = it.Key
			lastLoc.Len = 0
		}

		lastLoc.Len++
		if isInitialItem {
			lastLoc.Key = it.Key
			isInitialItem = false
		}

		prevLoc.Off += item.HeaderSize + item.Off(len(it.Blob)) + item.TrailerSize
		prevLoc.Key = it.Key
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	// also pick up last run in the data:
	if lastLoc.Len > 0 {
		index.Set(lastLoc)
	}

	return index, nil
}

func Load(path string) (*Index, error) {
	flags := os.O_CREATE | os.O_RDONLY
	fd, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}

	defer fd.Close()

	rdr := NewReader(fd)

	var index Index
	var loc item.Location
	for rdr.Next(&loc) {
		if loc.Len == 0 {
			// len=0 means that the specific batch was fully consumed.
			// delete any previously read values.
			index.Delete(loc.Key)
		} else {
			index.Set(loc)
		}
	}

	return &index, rdr.Err()
}

func (i *Index) Set(loc item.Location) (item.Location, int) {
	oldLocs, _ := i.m.Get(loc.Key)
	i.m.Set(loc.Key, append(oldLocs, loc))
	i.len += loc.Len
	i.nentries += loc.Len
	return loc, 0
}

func (i *Index) Delete(key item.Key) (loc item.Location) {
	oldLocs, ok := i.m.Get(key)
	if !ok {
		return
	}

	i.len -= oldLocs[0].Len
	i.nentries += oldLocs[0].Len
	if len(oldLocs) > 1 {
		// delete one of the keys:
		i.m.Set(key, oldLocs[1:])
		return
	}

	i.m.Delete(key)
	return oldLocs[0]
}

// Len returns the number of items in the WAL.
// (Not the number of locations or batches!)
func (i *Index) Len() item.Off {
	return i.len
}

// NEntries returns the number of entries in the
// index. This is not the same Len() as a deleted
// item is also inserted into the index.
func (i *Index) NEntries() item.Off {
	return i.nentries
}

func (i *Index) Trailer() Trailer {
	return Trailer{
		TotalEntries: i.len,
	}
}

func (i *Index) Copy() *Index {
	return &Index{
		m:        *i.m.Copy(),
		len:      i.len,
		nentries: i.nentries,
	}
}

////////////

type Iter struct {
	iter btree.MapIter[item.Key, []item.Location]
	curr []item.Location
}

func (i *Iter) Next() bool {
	if len(i.curr) > 1 {
		i.curr = i.curr[1:]
		return true
	}

	if i.iter.Next() {
		i.curr = i.iter.Value()
		return true
	}

	return false
}

func (i *Iter) Value() item.Location {
	if len(i.curr) == 0 {
		// this should not happen in case of correct api usage.
		// just a guard if someone calls Value() without Next()
		return item.Location{}

	}
	return i.curr[0]
}

func (i *Index) Iter() Iter {
	return Iter{iter: i.m.Iter()}
}
