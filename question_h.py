import os

import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt

# data 2020
working_dir_2020 = ['csv2020', '202008']
raw_csv_files_2020 = ['curr_httpgetmt6.csv', 'curr_httpgetmt.csv', 'curr_httppostmt6.csv',
                      'curr_httppostmt.csv']
download_files_2020 = raw_csv_files_2020[:2]
upload_files_2020 = raw_csv_files_2020[2:4]
v4_files_2020 = [raw_csv_files_2020[1], raw_csv_files_2020[3]]
v6_files_2020 = [raw_csv_files_2020[0], raw_csv_files_2020[2]]
working_dir_up = 'csvup'
output_dir = 'question_h_output'


def to_day(x):
    dt = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%d')


def is_keep_group(x):
    count_1 = x['successes'].value_counts().get(1)
    count_0 = x['successes'].value_counts().get(0)
    if count_1 is None:
        return False
    else:
        if count_0 is None:
            return True
        else:
            return count_1 > count_0


def to_group(raw_df):
    # use dtime to generate column 'day'
    raw_df['day'] = raw_df['dtime'].apply(to_day)
    total_size = raw_df['unit_id'].size

    # group and filter
    groups = raw_df.groupby(['day', 'unit_id'])
    new_df = groups.filter(is_keep_group)

    keep_size = new_df['unit_id'].size
    drop_size = total_size - keep_size
    print("total: %d, keep: %d, drop: %d, drop percentage: %f%%" % (
        total_size, keep_size, drop_size, (drop_size / total_size * 100)))
    return new_df


def calculate_median(some_df, metric='download'):
    median_df = some_df.groupby(['day', 'unit_id'])['bytes_sec_interval'].median().reset_index(name=metric)
    # print(median_df)

    # prepare the unit profile data
    df_unit_profile = pd.read_csv(os.path.join(working_dir_up, "Unit-Profile-sept2018.csv"), escapechar='\\')

    # merge with the unit profile to get access tech
    cc = median_df.merge(df_unit_profile, left_on='unit_id', right_on='unit_id')[
        ['day', "unit_id", "Technology", metric]]
    cc[metric] = cc[metric] / 131072
    return cc


def plot(cc, metric='download', version='IPv4'):
    # start an axis
    plt.clf()
    ax = plt.gca()
    # the same style with 2019
    sns.ecdfplot(ax=ax, data=cc, x=metric, hue='Technology', stat="proportion")
    ax.set_xscale('log', base=2)
    ax.set_xlim(right=2048)
    # show plot
    ax.set_ylabel('CDF')
    ax.set_title('%s %s Throughput 2020' % (version, 'Downstream' if metric == 'download' else 'Upstream'))
    ax.set_xlabel('%s [Mbps]' % ('Download' if metric == 'download' else 'Upload'))
    plt.savefig(os.path.join(output_dir, ('%s_%s.png' % (metric, version))), dpi=300)


if __name__ == '__main__':
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    # ipv6_download
    dl_ipv6_2020 = pd.read_csv(os.path.join(*working_dir_2020, raw_csv_files_2020[0]))
    plot(calculate_median(to_group(dl_ipv6_2020), metric='download'), metric='download', version='IPv6')
    # ipv4_download
    dl_ipv4_2020 = pd.read_csv(os.path.join(*working_dir_2020, raw_csv_files_2020[1]))
    plot(calculate_median(to_group(dl_ipv4_2020), metric='download'), metric='download', version='IPv4')
    # ipv6_upload
    ul_ipv6_2020 = pd.read_csv(os.path.join(*working_dir_2020, raw_csv_files_2020[2]))
    plot(calculate_median(to_group(ul_ipv6_2020), metric='upload'), metric='upload', version='IPv6')
    # ipv4_upload
    ul_ipv4_2020 = pd.read_csv(os.path.join(*working_dir_2020, raw_csv_files_2020[3]))
    plot(calculate_median(to_group(ul_ipv4_2020), metric='upload'), metric='upload', version='IPv4')
