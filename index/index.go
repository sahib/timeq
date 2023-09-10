package index

import (
	"os"

	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
)

// Index is an in-memory representation of the batch index as b-tree structure.
type Index struct {
	m btree.Map[item.Key, item.Location]
}

func FromTree(tree *btree.Map[item.Key, item.Location]) *Index {
	return &Index{m: *tree}
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
			// delete any previosuly read values.
			index.m.Delete(loc.Key)
		} else {
			index.m.Set(loc.Key, loc)
		}
	}

	return &index, rdr.Err()
}

func (i *Index) Set(loc item.Location, maxSkew int) (item.Location, int) {
	prev, ok := i.m.Set(loc.Key, loc)
	if !ok {
		// fast path: no dedup needed.
		return loc, 0
	}

	// restore previous state:
	i.m.Set(loc.Key, prev)

	// try to find a unique timestamp by cheating a little:
	// TODO: That could be probably optimized.
	var skew item.Key
	for skew < item.Key(maxSkew) {
		if _, ok := i.m.Get(loc.Key + skew); ok {
			skew++
			continue
		}

		// no entry with that key yet.
		loc.Key += skew
		i.m.Set(loc.Key, loc)
		return loc, int(skew)
	}

	return loc, maxSkew
}

func (i *Index) Delete(key item.Key) {
	i.m.Delete(key)
}

func (i *Index) Len() int {
	return i.m.Len()
}

type Iter struct {
	iter btree.MapIter[item.Key, item.Location]
}

func (i *Iter) Next() bool {
	return i.iter.Next()
}

func (i *Iter) Value() item.Location {
	return i.iter.Value()
}

func (i *Index) Iter() Iter {
	return Iter{iter: i.m.Iter()}
}
