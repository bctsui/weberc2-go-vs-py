package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

func abort(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
	os.Exit(-1)
}

func abortf(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format, v...)
	os.Exit(-1)
}

func main() {
	if len(os.Args) < 3 {
		abortf("USAGE: %s <col-count> <row-count>\n", os.Args[0])
	}
	columnCount, err := strconv.Atoi(os.Args[1])
	if err != nil {
		abort(err)
	}
	rowCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		abort(err)
	}

	rand.Seed(0)

	for y := 0; y < rowCount; y++ {
		row := make([]string, 0, columnCount)
		for x := 0; x < columnCount; x++ {
			row = append(row, strconv.Itoa(rand.Int()))
		}
		fmt.Fprintln(os.Stdout, strings.Join(row, ","))
	}
}
