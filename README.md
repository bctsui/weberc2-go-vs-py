README
------

This repo contains tools for benchmarking string -> int conversion for large
CSV files. There are 3 principle implementations: a Python3 implementation,
a Go sequential implementation, and a Go parallelized implementation. All three
implementations read from stdin into a 2D string list/slice (`[][]string`)
before the timer is started (so as to eliminate noise from the filesystem) and
the string -> int conversion commences.

Included in the repository is the `csvgen` tool which generates CSV data. It
takes 2 arguments, a column count and a row count, and it writes that data to
stdout. The data generated is deterministically such that the same data is
generated for successive calls to the program (provided the arguments remain
the same).

``` bash
$ csvgen 1000 100000 | python3 py/main.py
Beginning validation...
Validated 100000 rows of 1000 cells in 30.714239s

$ csvgen 1000 100000 | go run goseq/main.go
Beginning validation...
Validated 100000 rows of 1000 cells in 6.805363362s

$ csvgen 1000 100000 | go run gopar/main.go
GOMAXPROCS: 4
Beginning validation...
Validated 100000 rows of 1000 cells in 3.093580738s
```
