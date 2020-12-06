import os

import pandas as pd
from utils import convert
from datetime import datetime

# data 2020
working_dir_2020 = ['csv2020', '202008']
raw_csv_files_2020 = ['curr_httpgetmt6.csv', 'curr_httpgetmt.csv', 'curr_httppostmt6.csv',
                      'curr_httppostmt.csv']
download_files_2020 = raw_csv_files_2020[:2]
upload_files_2020 = raw_csv_files_2020[2:4]
v4_files_2020 = [raw_csv_files_2020[1], raw_csv_files_2020[3]]
v6_files_2020 = [raw_csv_files_2020[0], raw_csv_files_2020[2]]


def to_day(x):
    dt = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%d')


def to_group(raw_df):
    raw_df['day'] = raw_df['dtime'].apply(to_day)
    total_size = raw_df['successes'].size
    keep_size = 0
    df_dict = {}
    for n, df in raw_df.groupby(['day', 'unit_id']):
        # print(n)
        df_total_size = df['successes'].size
        success_size = df[df['successes'] == 1]['successes'].size
        success_rate = success_size / df_total_size
        if success_rate >= 0.5:
            df_dict[n] = df
            keep_size += df['successes'].size
    drop_size = total_size - keep_size
    print("total: %d, keep: %d, drop: %d, drop percentage: %f%%" % (
        total_size, keep_size, drop_size, (drop_size / total_size * 100)))
    return df_dict


if __name__ == '__main__':
    df_ipv6_2020 = pd.read_csv(os.path.join(*working_dir_2020, raw_csv_files_2020[0]))
    df_ipv6_2020_group = to_group(df_ipv6_2020)
    print(df_ipv6_2020_group['Download'].median())
