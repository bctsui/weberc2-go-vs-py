package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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

	fmt.Println("Beginning validation...")
	fmt.Printf(
		"Validated %d rows of %d cells in %v\n",
		len(rows),
		len(rows[0]),
		timeit(func() { validateRows(rows, len(rows[0])) }),
	)
}
