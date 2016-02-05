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
				fmt.Fprintf(os.Stderr, "Err at (%d, %d): %v\n", colID, rowID, err)
			}
		}
	}
}

func timeit(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Now().Sub(start)
}

// Divides `slice` into `n` subslices such that the elements are distributed
// as evenly as possible. In other words, if there are 10 elements in `slice`,
// and `n` is 3, there will be one subslice with 4 elements and the others will
// have only 3.
func subslice(slice [][]string, n int) [][][]string {
	blockSize := len(slice) / n
	remainder := len(slice) % n
	subslices := make([][][]string, n)

	// It's not enough just to make n subslices of len(slice)/n elements,
	// because integer division means that n*len(slice)/n != len(slice). For
	// example, if slice contains 10 elements and n is 4, then we'd be creating
	// 4 subslices with 10/4=2 elements each, and 2*4 != 10. To work around
	// this, we'll initially compute the remainder (10%4 = 2) which tells us
	// how many subslices will need to have len(slice)/n + 1 elements
	// (10/4+1=3) elements, while the rest will have only len(slice)/n
	// elements.
	//
	// The first for loop creates those slightly-larger subslices, while the
	// second creates the slightly-smaller subslices. In this way, the
	// difference between the largest and smallest subslices will be at most 1
	// element.
	for i := 0; i < remainder; i++ {
		start := i * (blockSize + 1)
		end := (i + 1) * (blockSize + 1)
		subslices[i] = slice[start:end]
	}

	for i := remainder; i < n; i++ {
		start := i*blockSize + remainder
		end := (i+1)*blockSize + remainder
		subslices[i] = slice[start:end]
	}

	return subslices
}

func validateParallel(rows [][]string, coreCount int) {
	wg := sync.WaitGroup{}
	wg.Add(coreCount)

	for _, block := range subslice(rows, coreCount) {
		// Create a new variable exclusively for the goroutine that corresponds
		// to this loop iteration. All goroutines can't share one variable,
		// because the variable will be pointing to the last block returned by
		// subslice() before the first goroutine is kicked off, meaning all
		// goroutines would be operating on the last block and the previous
		// blocks would be ignored.
		block := block

		go func() {
			validateRows(block, len(rows[0]))
			wg.Done()
		}()
	}

	wg.Wait()
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
