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

	colSize := len(rows[0])
	coreCount := runtime.NumCPU()
	fmt.Println("GOMAXPROCS:", coreCount)
	runtime.GOMAXPROCS(coreCount)
	blockSize := len(rows) / coreCount
	fmt.Println("Beginning validation...")
	start := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(coreCount)
	for i := 0; i < coreCount-1; i++ {
		rows := rows[i*blockSize : (i+1)*blockSize]
		go func() {
			validateRows(rows, colSize)
			wg.Done()
		}()
	}
	// do the last block separately just in case integer division caused
	// `blockSize * coreCount` to be less than `len(rows)`
	go func() {
		validateRows(rows[(coreCount-1)*blockSize:], colSize)
		wg.Done()
	}()
	wg.Wait()

	msg := "Validated %d rows of %d cells in %v\n"
	dt := time.Now().Sub(start)
	fmt.Printf(msg, len(rows), colSize, dt)
}
