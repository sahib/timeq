package timeq

import (
	"os"
	"slices"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

// TODO:
// Fuzz ideas:
// - different pop sizes.
// - different number of re-opens in the middle.

func FuzzPushPop(f *testing.F) {
	f.Add(0, 10, 1, 2)
	f.Fuzz(func(t *testing.T, start, stop, step, reps int) {
		items := Items(testutils.GenItems(start, stop, step))
		if len(items) == 0 || reps <= 0 {
			// bogus seed input
			return
		}

		dir, err := os.MkdirTemp("", "timeq-fuzz")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		queue, err := Open(dir, DefaultOptions())
		require.NoError(t, err)

		exp := Items{}
		for rep := 0; rep < reps; rep++ {
			require.NoError(t, queue.Push(items))
			exp = append(exp, items...)
		}

		got, err := queue.Pop(reps*len(items), nil)
		require.NoError(t, err)

		slices.SortFunc(exp, func(i, j item.Item) int {
			return int(i.Key - j.Key)
		})

		require.Equal(t, exp, got)
		require.NoError(t, queue.Close())
	})
}
