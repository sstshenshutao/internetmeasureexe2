import argparse
import multiprocessing
import os
import time

import pandas as pd
from utils import convert

working_dir_2020 = ['csv2020', '202008']
clean_csv_files = ['curr_httpgetmt6_clean.csv', 'curr_httpgetmt_clean.csv', 'curr_httppostmt6_clean.csv',
                   'curr_httppostmt_clean.csv']

source_df = pd.read_csv(os.path.join(*working_dir_2020, clean_csv_files[3]), escapechar='\\')
dict_measured = {}
cc = source_df.groupby('unit_id')[['bytes_sec_interval']].median()
print(cc)
# dd = source_df[['unit_id']].value_counts()
# print(source_df[source_df['unit_id'] == 390]['unit_id'].size)
# print(dd)
