package testutils

import (
	"fmt"

	"github.com/sahib/timeq/item"
)

func ItemFromIndex(idx int) item.Item {
	return item.Item{
		Key:  item.Key(idx),
		Blob: []byte(fmt.Sprintf("%d", idx)),
	}
}

func GenItems(start, stop, step int) item.Items {
	if step == 0 {
		return nil
	}

	var its item.Items
	for idx := start; ; idx += step {
		if step > 0 && idx >= stop {
			break
		}

		if step < 0 && idx <= stop {
			break
		}

		its = append(its, ItemFromIndex(idx))
	}

	return its
}
