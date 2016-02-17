# modified version of parallel.py to avoid reallocation of lists

import datetime, multiprocessing, os, sys

def scan_in_csv(r):
    try:
        return [line.split(',') for line in r], None
    except Exception as e:
        return None, e

def validate_rows(allrows, col_size, start, end):
    print('id of forked rows', id(allrows))
    for row_id in range(start, end):
	row = allrows[row_id]
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
    # Partition the input based on number of cores without reallocating memory
    # for new lists.
    chunks = [] # pairs of (start, end)
    chunk_size = int(len(rows)/core_count)
    end = len(rows)
    while end > 0:
        start = max(end - chunk_size, 0)
        chunks.append((start, end))
	end = start

    procs = []
    for start, end in chunks:
        # fork() happens here, is `rows` copied?
        p = multiprocessing.Process(target=validate_rows, args=(rows, col_size, start, end))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

def timeit(f):
    start = datetime.datetime.now()
    f()
    return datetime.datetime.now() - start

if __name__ == '__main__':
    _rows, err = scan_in_csv(sys.stdin)
    if err is not None:
        print(err)
        sys.exit(-1)

    if len(_rows) < 1:
        print('No rows in file')
        sys.exit(-1)

    core_count = multiprocessing.cpu_count()
    print("Beginning validation...")
    print('id of original rows', id(_rows))
    print("Validated {} rows of {} cells in {}".format(
	len(_rows),
	len(_rows[0]),
	timeit(lambda: validate_parallel(_rows, len(_rows[0]), core_count)),
    ))
