package timeq

import (
	"testing"
	"time"

	"github.com/sahib/timeq/item"
	"github.com/stretchr/testify/require"
)

func TestKeyTrunc(t *testing.T) {
	stamp := time.Date(2023, 1, 1, 12, 13, 14, 15, time.UTC)
	trunc1 := Trunc30mBuckets(item.Key(stamp.UnixNano()))
	trunc2 := Trunc30mBuckets(item.Key(stamp.Add(time.Minute).UnixNano()))
	trunc3 := Trunc30mBuckets(item.Key(stamp.Add(time.Hour).UnixNano()))

	// First two stamps only differ by one minute. They should be truncated
	// to the same value. One hour further should yield a different value.
	require.Equal(t, trunc1, trunc2)
	require.NotEqual(t, trunc1, trunc3)
}
