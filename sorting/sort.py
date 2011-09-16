import multiprocessing
import array
import heapq
from tempfile import NamedTemporaryFile as tmpf
import os
import sys

# ints in buffer
INT_PER_CHUNK = 4096

# ints per cutted file
#   more - sorting is quicker, but requires more memory
INT_PER_FILE = 4096 * 128 # 2mb

# where to store tmp files, None for default tmp dir
TMP_DIR = 'tmp'

# number or files to merge at once
#   - for best perfomance should be profiled for target system's cpu and hdd
#   - can hit open files limit
#   please don't set lower then 2
MERGES = 16

def file_reader(fn):
    '''read file to buffer, yield one integer'''
    with open(fn, 'rb') as f:
        has_more = True
        while has_more:
            a = array.array('i')
            try:
                a.fromfile(f, INT_PER_CHUNK)
            except EOFError:
                has_more = False
            for i in a:
                yield i

def merge_files(fn_list):
    '''merge files in fn_list, return filename of resulting temp file'''
    it = [file_reader(fn) for fn in fn_list]

    with tmpf('wb', delete=False, dir=TMP_DIR) as f:
 
        # ugly but much quicker then c.append() with len(c)
        c = array.array('i', [0] * INT_PER_CHUNK)
        cidx = 0
        for i in heapq.merge(*it):
            c[cidx] = i
            cidx += 1
            if cidx == INT_PER_CHUNK:
                c.tofile(f.file)
                cidx = 0
                c = array.array('i', [0] * INT_PER_CHUNK)
        c = array.array('i', [c[i] for i in range(cidx)])
        c.tofile(f.file)
    return f.name

def sorter(fn):
    '''sort integers in a file'''
    a = array.array('i')
    with open(fn, 'rb') as f:
        try:
            a.fromfile(f, INT_PER_FILE)
        except EOFError:
            pass
    c = array.array('i', sorted(a))
    with open(fn, 'wb') as f:
        c.tofile(f)
    return fn

def merger(fn_list):
    '''merge file in fn_list into new one, delete them, return resulting filename'''
    if len(fn_list) == 1:
        return fn_list[0]
    res = merge_files(fn_list)
    for fn in fn_list:
        os.unlink(fn)
    return res

def cutter(fn):
    '''cut file to smaller files, yield temp filename'''
    with open(fn, 'rb') as f:
        chunk = f.read(INT_PER_FILE * 4)
        while chunk:
            with tmpf('wb', delete=False, dir=TMP_DIR) as fout:
                fout.write(chunk)
            yield fout.name
            chunk = f.read(INT_PER_FILE * 4)

# files to merge will be here
sorted_fn_list = []

def merge_tasks(pool):
    '''generate tasks for merger, renew pool if needed'''

    global sorted_fn_list
    while True:
        # is there something that needs merging?
        if len(sorted_fn_list) > 1:

            # yield pool and list of files for merging, yank yielded list
            yield {'pool': pool, 'list': sorted_fn_list[:MERGES]}
            sorted_fn_list = sorted_fn_list[MERGES:]
        else:
            # wait for mergers to finish
            pool.close()
            pool.join()

            # did mergers bring something to work on?
            if len(sorted_fn_list) == 1:
                # no, we're done
                return
            else:
                # yes, renew pool
                pool = multiprocessing.Pool()

def cb(arg):
    '''callback for sorter and merger, add files to merge list'''
    sorted_fn_list.append(arg)

def rename(src, tgt):
    try:
        if os.path.isfile(tgt):
            os.unlink(tgt)
        os.rename(src, tgt)
    except Exception as e:
        print('could not rename "%s" to "%s"' % (src, tgt))
        print(e)
        exit(1)

def sort(input, output):
    '''sort input file to output file'''
    # cut & sort
    pool = multiprocessing.Pool()
    for fn in cutter(input):
        pool.apply_async(sorter, args=[fn], callback=cb)

    # initial sorting is relatively quick, we can join here
    pool.close()
    pool.join()

    # merge sort
    for task in merge_tasks(multiprocessing.Pool()):
        # pool = task[0]
        # merge_list = task[1]
        task['pool'].apply_async(merger, args=[task['list']], callback=cb)

    # rename resulting tmp file
    rename(sorted_fn_list[0], output)

def sort_profile(input, output):
    '''single processor version of sort()'''
    sorted_fn_list = map(sorter, cutter(input))
    def merge_tasks(l):
        while len(l) > 0:
            yield l[:MERGES]
            l = l[MERGES:]
    while len(sorted_fn_list) > 1:
        sorted_fn_list = map(merger, merge_tasks(sorted_fn_list))
    rename(sorted_fn_list[0], output)

if __name__ == '__main__':
    # no fancy argv parsing for simplicity...
    if len(sys.argv) != 3:
        print('usage: %s input output' % os.path.basename(sys.argv[0]))
        exit(1)

    input  = sys.argv[1]
    output = sys.argv[2]

    if not os.path.isfile(input):
        print('"%s" is not a file' % input)
        exit(1)

    sort(input, output)

    # import datetime
    # for m in [128, 64, 32, 16, 8, 4, 2]:
        # MERGES = m
        # sorted_fn_list = []
        # print('-' * 10)
        # print('%02d merges' % m)
        # start = datetime.datetime.now()
        # sort(input, '%s-%02d' % (output, m))
        # print('\ntotal time: %s' % (datetime.datetime.now() - start))

    # cProfile.run('profile("%s", "%s")' % (input, output), 'profile')
