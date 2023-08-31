package index

import (
	"container/heap"
	"encoding/binary"
	"io"

	"github.com/sahib/lemurq/item"
	"github.com/tidwall/btree"
)

// Index is an in-memory representation of the batch index as b-tree structure.
type Index struct {
	btree.Map[item.Key, item.Location]
}

func (i *Index) Marshal(w io.Writer) error {
	buf := make([]byte, LocationSize)

	// TODO: Benchmark if Scan() or Iter() is faster
	// TODO: add skew.
	var err error
	i.Scan(func(key item.Key, loc item.Location) bool {
		binary.BigEndian.PutUint64(buf[:8], uint64(loc.Key))
		binary.BigEndian.PutUint32(buf[8:], uint32(loc.Off))
		binary.BigEndian.PutUint32(buf[12:], uint32(loc.Len))
		if _, err = w.Write(buf); err != nil {
			return false
		}

		return true
	})

	return err
}

type stream struct {
	index     *IndexLog
	loc       item.Location
	exhausted bool
}

func (s *stream) consume() error {
	if !s.index.Next(&s.loc) {
		if err := s.index.Err(); err != nil {
			return err
		}

		s.exhausted = true
	}

	return nil
}

type streams []stream

func (s streams) Len() int      { return len(s) }
func (s streams) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s streams) Less(i, j int) bool {
	if s[i].exhausted != s[j].exhausted {
		// sort exhausted streams to the back
		return !s[i].exhausted
	}

	ki := s[i].loc.Key
	kj := s[j].loc.Key
	if ki != kj {
		return s[i].index.ID() < s[j].index.ID()
	}

	return ki < kj
}

// NOTE: Push & Pop not implemented since we don't need it
//
//	(but still required by Go's heap interface :/ )
func (s *streams) Push(x any) {}
func (s *streams) Pop() any   { return nil }

func Merge(indexes []*IndexLog) (*Index, error) {
	if len(indexes) == 0 {
		return &Index{}, nil
	}

	streamSlice := streams(make([]stream, 0, len(indexes)))
	streams := &streamSlice
	for idx := 0; idx < len(indexes); idx++ {
		s := stream{index: indexes[idx]}
		if err := s.consume(); err != nil {
			return nil, err
		}

		*streams = append(*streams)
	}

	merged := Index{}
	heap.Init(streams)
	for lowestStream := (*streams)[0]; !lowestStream.exhausted; {

		// NOTE: We might have duplicate keys. Since we sort freshly we always
		// set the lowest entry first which gets overwritten soon after, so the
		// deduplicaton happens automatically.
		loc := lowestStream.loc
		merged.Set(loc.Key, loc)
		if !lowestStream.index.Next(&loc) {
			if err := lowestStream.consume(); err != nil {
				return nil, err
			}
		}

		heap.Fix(streams, 0)
	}

	return &merged, nil
}

func Load(bucketDir string) (*Index, error) {
	// TODO: Implement using glob or similar.
	return nil, nil
}

func Empty() *Index {
	return &Index{}
}
