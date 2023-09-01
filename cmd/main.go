package main

import (
	"fmt"
	"os"

	"github.com/sahib/timeq/cmd/parser"
)

func main() {
	if err := parser.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}
