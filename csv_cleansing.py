import argparse
import multiprocessing
import os
import time

import pandas as pd
from utils import convert
from csv_id_counter import csv_count_ids

tmp_filename = "tmp_ids.csv"
default_all_csv_files = ['curr_httpgetmt6.csv', 'curr_httpgetmt.csv', 'curr_httppostmt6.csv', 'curr_httppostmt.csv']
# default_all_csv_files = ['sample6.csv', 'sample.csv']
default_working_dir = ['csv2020', '202008']
working_dir_excl = 'csvexcl'

# default_tmp_ids_filename = 'tmp_ids.csv'
default_chunksize = 100000
all_counter = multiprocessing.Value('i', 0)
worker_number = multiprocessing.Value('i', 0)


def clean_csv(chunksize=100000):
    # prepare the exclude unit table
    convert(working_dir_excl, 'excluded-units-sept2018.xlsx', 'excluded-units-sept2018.csv',
            sheet_name='Excluded')
    csv_count_ids(['excluded-units-sept2018.csv'], working_dir=working_dir_excl,
                  tmp_ids_filename=tmp_filename)
    df_excl = pd.read_csv(os.path.join(working_dir_excl, tmp_filename), index_col=0)

    # generate files
    file_tuples = [(os.path.join(*default_working_dir, f), True if '6' in f else False)
                   for f in default_all_csv_files]
    jobs = []
    # test the time
    start_time = time.time()
    for file, ipv6_flag in file_tuples:
        p = multiprocessing.Process(target=csv_worker, args=(file, '_clean', ipv6_flag, df_excl, chunksize))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()
    # print the execution time
    print("used time: %f" % (time.time() - start_time))


# the worker for the all_csv_files
def csv_worker(filepath, output_suffix, ipv6_flag, df_excl, chunksize):
    output_filepath = filepath[:len(filepath) - 4] + output_suffix + ".csv"
    if os.path.isfile(output_filepath):
        os.remove(output_filepath)
    with worker_number.get_lock():
        worker_number.value += 1
    print("csv: %s ...[worker number: %d]" % (os.path.basename(filepath), worker_number.value))
    dfs = pd.read_csv(filepath, escapechar='\\', chunksize=chunksize)
    counter_ori = 0
    counter_cleansing = 0
    for df in dfs:
        counter_ori += df['unit_id'].size
        if ipv6_flag:
            query_command = 'successes != 0 & sequence <= 5'
        else:
            query_command = 'successes != 0 & sequence <= 5 & bytes_sec_interval > 0 & bytes_sec > 0'
        df_new0 = df.query(query_command)
        df_new0 = df_new0[df_new0["target"].str.contains("mlab|level3") & ~df_new0["target"].str.contains("lga05")]
        df_new0 = df_new0.merge(df_excl, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        df_new0 = df_new0.drop('_merge', 1)
        counter_cleansing += df_new0['unit_id'].size
        # merge the result to the file using append
        if os.path.isfile(output_filepath):
            df_new0.to_csv(output_filepath, mode='a', index=False, header=False)
        else:
            df_new0.to_csv(output_filepath, mode='a', index=False, header=True)

    with worker_number.get_lock():
        worker_number.value -= 1
    print("csv: %s, original %s rows, after cleansing:%d, [worker number %d]" % (
        os.path.basename(filepath), counter_ori, counter_cleansing, worker_number.value))


if __name__ == '__main__':
    clean_csv()

# csv: curr_httppostmt6.csv, original 8878 rows, after cleansing:6065, [worker number 3]
# csv: curr_httpgetmt6.csv, original 8898 rows, after cleansing:6095, [worker number 2]
# csv: curr_httppostmt.csv, original 1377829 rows, after cleansing:181392, [worker number 1]
# csv: curr_httpgetmt.csv, original 1382561 rows, after cleansing:183710, [worker number 0]
