package main

// modified version of parallel.go without the creation of new arrays to give
// to the goroutines

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

func Validate(rows [][]string, colSize int) {
    wg.Add(1)
    defer wg.Done()

    for rowID, row := range rows {
        // actual validation
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

func Head(input [][]string, n int) (head, tail [][]string) {
    if n > len(input) {
        n = len(input)
    }
    fmt.Println(n)
    head, tail = input[:n], input[n:]
    return
}

var wg sync.WaitGroup

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
    
    wg = sync.WaitGroup{}
    start := time.Now()

    // start the workers
    coreCount := runtime.NumCPU()
    chunkSize := len(rows)/coreCount
    colSize := len(rows[0])
    chunk := [][]string{}
    tail := rows
    for len(tail) > 0 {
        chunk, tail = Head(tail, chunkSize)
        go Validate(chunk, colSize)
    }

    fmt.Println("Beginning validation...")
    wg.Wait()
    fmt.Printf(
        "Validated %d rows of %d cells in %v\n",
        len(rows),
        len(rows[0]),
        time.Now().Sub(start),
    )
}
