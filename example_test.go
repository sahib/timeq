package timeq

import (
	"fmt"
	"os"
	"reflect"
)

func ExampleQueue() {
	// error handling stripped for brevity:
	dir, _ := os.MkdirTemp("", "timeq-example")
	defer os.RemoveAll(dir)

	// Open the queue. If it does not exist, it gets created:
	queue, _ := Open(dir, DefaultOptions())

	// Push some items to it:
	pushItems := make(Items, 0, 10)
	for idx := 0; idx < 10; idx++ {
		pushItems = append(pushItems, Item{
			Key:  Key(idx),
			Blob: []byte(fmt.Sprintf("key_%d", idx)),
		})
	}

	_ = queue.Push(pushItems)

	// Retrieve the same items again:
	popItems, _ := queue.Pop(10, nil)

	// Just for example purposes, check if they match:
	if reflect.DeepEqual(pushItems, popItems) {
		fmt.Println("They match! :)")
	} else {
		fmt.Println("They do not match! :(")
	}

	// Output: They match! :)
}
