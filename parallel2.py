# This version provided by [/u/civilization_phaze_3][1]
# [1]: https://www.reddit.com/user/civilization_phaze_3

import datetime
import sys
from multiprocessing import Pool, cpu_count
 
def scan_in_csv(r):
    try:
        return [line.split(',') for line in r], None
    except Exception as e:
        return None, e
 
def validate_rows(args):
    rows, col_size = args
    print('validating %s rows' % len(rows))
    for i, row in enumerate(rows):
        if len(row) != col_size:
            msg = "Row {} has {} cells, but expected {}\n"
            print(msg.format(row_id, len(row), col_size))
            continue
    for col_id, cell in enumerate(row):
        try:
            int(cell)
        except ValueError as e:
            print("Err at ({}, {}): {}".format(col_id, row_id, e))
 
def multi_validate_rows(rows, col_size):
    n_cores = 4
    print('N_CORES', n_cores)
 
    pool = Pool(n_cores)
    chunks = ((rows[i::n_cores], col_size) for i in range(n_cores))
    pool.imap(validate_rows, chunks)
    pool.close()
    pool.join()
 
def timeit(f):
    start = datetime.datetime.now()
    f()
    return (datetime.datetime.now() - start).total_seconds()
 
if __name__ == '__main__':
    rows, err = scan_in_csv(sys.stdin)
    if err is not None:
        print("No rows in file")
        sys.exit(-1)
 
    if len(rows) < 1:
        print('No rows in file')
        sys.exit(-1)
 
    print("Beginning validation...")
    #print("Validated {} rows of {} cells in {}".format(
    #    len(rows),
    #    len(rows[0]),
    #    timeit(lambda: validate_rows(rows, len(rows[0]))),
    #))
    print("Validated {} rows of {} cells in {}".format(
        len(rows),
        len(rows[0]),
        timeit(lambda: multi_validate_rows(rows, len(rows[0]))),
    ))
