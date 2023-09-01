package index

import (
	"fmt"
	"os"

	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
)

// Index is an in-memory representation of the batch index as b-tree structure.
type Index struct {
	btree.Map[item.Key, item.Location]
}

func Load(path string) (*Index, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer fd.Close()

	rdr := NewReader(fd)

	var loc item.Location
	var index Index
	for rdr.Next(&loc) {
		if loc.Len == 0 {
			// len=0 means that the batch was fully consumed.
			// delete any previosuly read values.
			index.Delete(loc.Key)
		} else {
			index.Set(loc.Key, loc)
		}
	}

	return &index, rdr.Err()
}

func (i *Index) SetSkewed(loc item.Location, maxSkew int) (item.Location, error) {
	prev, ok := i.Set(loc.Key, loc)
	if !ok {
		// fast path: no dedup needed.
		return loc, nil
	}

	// restore previous state:
	i.Set(loc.Key, prev)

	// try to find a unique timestamp by cheating a little:
	var skew item.Key
	for skew < item.Key(maxSkew) {
		loc, ok := i.Get(loc.Key + skew)
		if !ok {
			// no entry yet.
			loc.Key += skew
			i.Set(loc.Key, loc)
			return loc, nil
		}

		skew++
	}

	return loc, fmt.Errorf("skew too high: %v", skew)
}
