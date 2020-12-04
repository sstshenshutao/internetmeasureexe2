import argparse
import multiprocessing
import os
import time

import pandas as pd

default_all_csv_files = []
default_working_dir = 'csvdata'
default_tmp_ids_filename = 'tmp_ids.csv'
default_chunksize = 100000
all_counter = multiprocessing.Value('i', 0)
worker_number = multiprocessing.Value('i', 0)


def init_argparse():
    parser = argparse.ArgumentParser(description='question_b')
    parser.add_argument('--all_csv_files', metavar='all_csv_files', nargs='?', default=default_all_csv_files,
                        help='all_csv_files')
    parser.add_argument('--working_dir', metavar='working_dir', type=str, nargs='?', default=default_working_dir,
                        help='working_dir')
    parser.add_argument('--tmp_ids_filename', metavar='tmp_ids_filename', type=str, nargs='?',
                        default=default_tmp_ids_filename,
                        help='tmp_ids_filename')
    parser.add_argument('--chunksize', metavar='chunksize', type=int, nargs='?', default=default_chunksize,
                        help='the max chunksize size')
    return parser.parse_args()


def csv_count_ids(all_csv_files, working_dir='csvdata', tmp_ids_filename='tmp_ids.csv', chunksize=100000):
    tmp_ids_filepath = os.path.join(working_dir, tmp_ids_filename)
    jobs = []
    lock = multiprocessing.Lock()
    # test the time
    start_time = time.time()
    for file in all_csv_files:
        current_filepath = os.path.join(working_dir, file)
        p = multiprocessing.Process(target=csv_worker, args=(lock, current_filepath, tmp_ids_filepath, chunksize))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

    # count all and clean file
    df = pd.read_csv(tmp_ids_filepath, escapechar='\\', index_col=0)
    # clean data file - no, saving for the question c)
    # os.remove(tmp_ids_filepath)
    # print the execution time
    print("used time: %f, total: %d" % (time.time() - start_time, df.size))
    # used
    # time: 166.251599, total: 5589
    # id_size: 5589
    return df.size


# the worker for the all_csv_files, each worker works for a single avro file that is extracted from the tar
def csv_worker(lck, filepath, tmp_ids_filepath, chunksize):
    ids = None
    with worker_number.get_lock():
        worker_number.value += 1
    dfs = pd.read_csv(filepath, escapechar='\\', chunksize=chunksize)
    counter = 0
    print("csv: %s ...[worker number: %d]" % (os.path.basename(filepath), worker_number.value))
    for df in dfs:
        column_id = df[['unit_id']].drop_duplicates()
        ids = pd.concat([ids, column_id]).drop_duplicates().reset_index(drop=True)
        counter += df.size
    lck.acquire()
    tmp_df_size = 0
    new_df_size = 0
    try:
        # read all and drop_duplicates, maintain a minimal set
        if os.path.isfile(tmp_ids_filepath):
            tmp_df = pd.read_csv(tmp_ids_filepath, escapechar='\\', index_col=0)
            tmp_df_size = tmp_df.size
        else:
            tmp_df = None
            tmp_df_size = 0
        new_df = pd.concat([tmp_df, ids]).drop_duplicates().reset_index(drop=True)
        new_df_size = new_df.size
        new_df.to_csv(tmp_ids_filepath, mode='w')
    except Exception as e:
        print("except!!!" + str(type(e)))
    finally:
        lck.release()
    with worker_number.get_lock():
        worker_number.value -= 1
    print("csv: %s, %s rows, id_size: %d, [ori/new]: %d/%d saved [worker number %d]" % (
        os.path.basename(filepath), counter, ids.size, tmp_df_size, new_df_size, worker_number.value))


if __name__ == '__main__':
    args = init_argparse()
    arg_working_dir = args.working_dir
    arg_tmp_ids_filename = args.tmp_ids_filename
    arg_all_csv_files = args.all_csv_files
    arg_chunksize = args.chunksize
    # if no specific files, all files in the working dir will be used
    if len(arg_all_csv_files) == 0:
        arg_all_csv_files = list(
            filter(lambda f: f.endswith(".csv") and f != arg_tmp_ids_filename, os.listdir(arg_working_dir)))
    # print(arg_all_csv_files)
    id_size = csv_count_ids(arg_all_csv_files, working_dir=arg_working_dir, tmp_ids_filename=arg_tmp_ids_filename,
                            chunksize=arg_chunksize)
    print("counter finished, id_size: ", id_size)
