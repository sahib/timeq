package item

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestItemCopy(t *testing.T) {
	blobOrig := []byte("hello")
	itemOrig := Item{
		Key:  23,
		Blob: blobOrig,
	}

	itemCopy := itemOrig.Copy()

	// check that it was indeed copied:
	require.Equal(t, itemOrig, itemCopy)
	require.True(t, unsafe.SliceData(blobOrig) != unsafe.SliceData(itemCopy.Blob))
}

func TestKeyFromString(t *testing.T) {
	ff, err := KeyFromString("99")
	require.NoError(t, err)
	require.Equal(t, Key(99), ff)

	ff, err = KeyFromString("K0000099")
	require.NoError(t, err)
	require.Equal(t, Key(99), ff)

	_, err = KeyFromString("ZZ")
	require.Error(t, err)
}

func TestItemsCopy(t *testing.T) {
	items := Items{
		Item{
			Key:  17,
			Blob: []byte("blob"),
		},
		Item{
			Key:  23,
			Blob: []byte(""),
		},
		Item{
			Key:  42,
			Blob: []byte(""),
		},
	}
	copied := items.Copy()
	require.Equal(t, items, copied)
	require.True(t, unsafe.SliceData(items) != unsafe.SliceData(copied))
}
