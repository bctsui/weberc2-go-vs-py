# This version provided by [/u/hanpari][1]
# [1]: https://www.reddit.com/user/hanpari

from functools import partial
import datetime
import multiprocessing as mp
import sys
 
def scan_in_csv(r):
    try:
        return [line.split(',') for line in r], None
    except Exception as e:
        return None, e

def timeit(f):
    start = datetime.datetime.now()
    f()
    return datetime.datetime.now() - start
 
def check_cells(row_id, cells, col_size) -> "message or None":
    col_id = "Not defined"
    try:
        if len(cells) == col_size:
            for col_id, cell in enumerate(cells):
                int(cell)
            return None
    except ValueError:
        pass
    return "Trouble at row_id: {} with cells: {} at col_id: {}.".format(row_id, cells, col_id)
       
def validate_rows(rows, col_size):
    pool = mp.Pool(mp.cpu_count())
    for msg in filter(None, pool.starmap(partial(check_cells, col_size=col_size), enumerate(rows))):
        print(msg)
    pool.close()
   
if __name__ == "__main__":
    rows, err = scan_in_csv(sys.stdin)
    if err is not None:
        print("No rows in file")
        sys.exit(-1)

    if len(rows) < 1:
        print('No rows in file')
        sys.exit(-1)

    print("Beginning validation...")
    print("Validated {} rows of {} cells in {}".format(
        len(rows),
        len(rows[0]),
        timeit(lambda: validate_rows(rows, len(rows[0]))),
    ))
