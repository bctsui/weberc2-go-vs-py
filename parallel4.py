# modified version of parallel.py to avoid reallocation of lists

import multiprocessing
import multiprocessing.dummy
import datetime
import sys

def scan_in_csv(r):
    try:
        return [line.split(',') for line in r], None
    except Exception as e:
        return None, e

def validate_rows(rows, col_size, start, end):
    for row_id in range(start, end):
        row = rows[row_id]
        if len(row) != col_size:
            msg = "Row {} has {} cells, but expected {}\n"
            print(msg.format(row_id, len(row), col_size))
            continue
        for col_id, cell in enumerate(row):
            try:
                int(cell)
            except ValueError as e:
                print("Err at ({}, {}): {}".format(col_id, row_id, e))

def validate_parallel(rows, col_size, core_count):
    pool = multiprocessing.dummy.Pool(core_count)

    # Partition the input based on number of cores without allocating memory
    # for new lists, ie. just find the index boundaries of the global list.
    #
    # This is where golang's implementation of slices & arrays would shine...
    chunks = [] # pairs of (start, end)
    chunk_size = int(len(rows)/core_count)
    end = len(rows)
    while end > 0:
        start = max(end - chunk_size, 0)
        chunks.append((start, end))
        end = start

    # ... but perhaps it doesn't matter much since I have to fork `rows` to in its entirety.
    pool.map(lambda pairs: validate_rows(rows, col_size, pairs[0], pairs[1]), chunks)
    pool.close()
    pool.join()

def timeit(f):
    start = datetime.datetime.now()
    f()
    return datetime.datetime.now() - start

if __name__ == '__main__':
    rows, err = scan_in_csv(sys.stdin)
    if err is not None:
        print(err)
        sys.exit(-1)

    if len(rows) < 1:
        print('No rows in file')
        sys.exit(-1)

    core_count = multiprocessing.cpu_count()
    print("Beginning validation...")
    print("Validated {} rows of {} cells in {}".format(
        len(rows),
        len(rows[0]),
        timeit(lambda: validate_parallel(rows, len(rows[0]), core_count)),
    ))
