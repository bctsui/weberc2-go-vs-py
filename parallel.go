package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func scanInCSV(r io.Reader) ([][]string, error) {
	rows := [][]string{}
	s := bufio.NewScanner(r)
	for s.Scan() {
		rows = append(rows, strings.Split(s.Text(), ","))
	}

	return rows, s.Err()
}

func validateRows(rows [][]string, colSize int) {
	for rowID, row := range rows {
		if len(row) != colSize {
			msg := "Row %d has %d cells, but expected %d\n"
			fmt.Fprintf(os.Stderr, msg, rowID, len(row), colSize)
			continue
		}
		for colID, cell := range row {
			if _, err := strconv.Atoi(cell); err != nil {
				msg := "Err at (%d, %d): %v\n"
				fmt.Fprintf(os.Stderr, msg, colID, rowID, err)
			}
		}
	}
}

// Divides `slice` into `n` subslices such that the elements are distributed
// as evenly as possible. In other words, if there are 10 elements in `slice`,
// and `n` is 3, there will be one subslice with 4 elements and the others will
// have only 3.
func subslice(s [][]string, n int) (ret [][][]string) {
	for ; n > 0; n-- {
		s, ret = s[len(s)/n:], append(ret, s[:len(s)/n])
	}
	return ret
}

func validateParallel(rows [][]string, coreCount int) {
	wg := sync.WaitGroup{}
	wg.Add(coreCount) // Add `coreCount` goroutines to the WaitGroup

	// divide `rows` into `coreCount` blocks of rows, and then dispatch a
	// goroutine to process each block.
	for _, block := range subslice(rows, coreCount) {
		go func(block [][]string) {
			validateRows(block, len(rows[0]))
			wg.Done() // signal that this goroutine has finished execution
		}(block)
	}

	wg.Wait() // block until `wg.Done()` has been called `coreCount` times
}

func timeit(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Now().Sub(start)
}

func main() {
	rows, err := scanInCSV(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
	if len(rows) < 1 {
		fmt.Fprintln(os.Stderr, "No rows in file")
		os.Exit(-1)
	}

	coreCount := runtime.NumCPU()
	runtime.GOMAXPROCS(coreCount)
	fmt.Println("GOMAXPROCS:", coreCount)
	fmt.Println("Beginning validation...")

	fmt.Printf(
		"Validated %d rows of %d cells in %v\n",
		len(rows),
		len(rows[0]),
		timeit(func() { validateParallel(rows, coreCount) }),
	)
}
