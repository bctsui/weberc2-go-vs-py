# This is my attempt at parallel Python implementation using shared memory and
# threads.

import multiprocessing
import multiprocessing.dummy
import datetime
import sys

def scan_in_csv(r):
    try:
        return [line.split(',') for line in r], None
    except Exception as e:
        return None, e

def validate_rows(rows, col_size):
    for row_id, row in enumerate(rows):
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
    chunks = [rows[i::core_count] for i in range(core_count)]
    pool.map(lambda r: validate_rows(r, col_size), chunks)
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
