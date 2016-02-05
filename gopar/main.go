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

func validateParallel(rows [][]string, coreCount int) {
	blockSize := len(rows) / coreCount

	wg := sync.WaitGroup{}
	wg.Add(coreCount)

	// Given N is the number of cores on the machine, spin up the first N-1 go-
	// routines to process the first (N-1) * len(rows) / N blocks of rows. For
	// example, if the machine has 4 cores, this for loop will spin up the first
	// 3 to process the first 3/4 of the total rows.
	for i := 0; i < coreCount-1; i++ {
		rows := rows[i*blockSize : (i+1)*blockSize]
		go func() {
			validateRows(rows, len(rows[0]))
			wg.Done()
		}()
	}

	// Do the last block separately just in case integer division caused
	// `blockSize * coreCount` to be less than `len(rows)`
	go func() {
		validateRows(rows[(coreCount-1)*blockSize:], len(rows[0]))
		wg.Done()
	}()
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
