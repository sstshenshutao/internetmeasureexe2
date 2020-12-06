# source: remaining_probes_merge_unit_profile.csv
# download upload

import os
from csv_id_counter import csv_count_ids
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# common:
output_dir = 'question_d_output'
# year 2011:
working_dir_2011 = 'csv2011'
# year 2018:
working_dir_probes = 'csv_valid_probes'
cache_filename = "remaining_probes_merge_unit_profile_upper_bound.csv"


# y_ = [df_dl_ul['Download'].cumsum() / df_dl_ul['Download'].sum(),
#       df_dl_ul['Upload'].cumsum() / df_dl_ul['Upload'].sum()]
# plt.plot(x=df_dl_ul['Download'].values, y=y_, grid=True)

def count_ids_2011():
    files = ["curr_avail.csv", "curr_dlping.csv", "curr_dns.csv", "curr_httpgetmt.csv", "curr_httppostmt.csv",
             "curr_ping.csv", "curr_udpjitter.csv", "curr_udplatency.csv", "curr_ulping.csv", "curr_videostream.csv",
             "curr_webget.csv"]
    csv_count_ids(files, working_dir=working_dir_2011)


def preprocess_plot_source_2011():
    # prepare probes:
    count_ids_2011()
    source_df_2011 = pd.read_csv(os.path.join(working_dir_2011, 'tmp_ids.csv'), escapechar='\\', index_col=0)

    # prepare unit profile:
    df_unit_profile_2011 = pd.read_csv(os.path.join(working_dir_2011, "unit_metadata.csv"), escapechar='\\').rename(
        columns={'UnitID': 'unit_id'})
    # merge CABLE - BUSINESS into CABLE
    df_unit_profile_2011['TECHNOLOGY'] = df_unit_profile_2011['TECHNOLOGY'].apply(
        lambda x: 'CABLE' if str(x).strip() == 'CABLE - BUSINESS' else x)

    # remove the empty download or upload
    df_unit_profile_2011["ISP DOWN"] = df_unit_profile_2011["ISP DOWN"].astype(float)
    df_unit_profile_2011["ISP UP"] = df_unit_profile_2011["ISP UP"].astype(float)
    df_unit_profile_2011 = df_unit_profile_2011[df_unit_profile_2011['ISP DOWN'].notna()]
    df_unit_profile_2011 = df_unit_profile_2011[df_unit_profile_2011['ISP UP'].notna()]

    # merge two table, combine the information
    source_df_2011 = source_df_2011.merge(df_unit_profile_2011, left_on='unit_id', right_on='unit_id')[
        ["unit_id", "TECHNOLOGY", "ISP DOWN", "ISP UP"]]
    source_df_2011 = source_df_2011.rename(
        columns={'ISP DOWN': 'Download', 'TECHNOLOGY': 'Technology', "ISP UP": "Upload"})
    return source_df_2011


def plot(source_df, year, ax, linestyle='solid', metric='Download & Upload'):
    # get 3
    plot_df = source_df[['Technology', "Download", "Upload"]].copy()

    # generate groups by technology
    for name, group_df in plot_df.groupby('Technology'):
        # calculate cdf for all df

        # algo 1
        # download_column_name = "%s_%s Download" % (name, year)
        # dl_series = group_df["Download"].sort_values()
        # group_df[download_column_name] = dl_series.cumsum() / dl_series.sum()
        # ul_series = group_df["Upload"].sort_values()
        # upload_column_name = "%s_%s Upload" % (name, year)
        # group_df[upload_column_name] = ul_series.cumsum() / ul_series.sum()

        # algo 2
        # download_column_name = "%s_%s dl" % (name, year)
        # group_df[download_column_name] = group_df["Download"].rank(method='average', pct=True)
        # upload_column_name = "%s_%s ul" % (name, year)
        # group_df[upload_column_name] = group_df["Upload"].rank(method='average', pct=True)

        # sort and plot
        # group_df = group_df.sort_values('Download')
        # if metric == 'Download & Upload' or metric == 'Download':
        #     group_df.plot(x='Download', y=download_column_name, ax=ax, linestyle=linestyle)
        # group_df = group_df.sort_values('Upload')
        # if metric == 'Download & Upload' or metric == 'Upload':
        #     group_df.plot(x='Upload', y=upload_column_name, ax=ax, linestyle=linestyle)

        # use sns
        download_column_name = "%s_%s dl" % (name, year)
        group_df[download_column_name] = group_df["Download"]
        upload_column_name = "%s_%s ul" % (name, year)
        group_df[upload_column_name] = group_df["Upload"]

        if metric == 'Download & Upload' or metric == 'Download':
            sns.ecdfplot(ax=ax, data=group_df, x=download_column_name, label=download_column_name, linestyle=linestyle,
                         stat="proportion")
        if metric == 'Download & Upload' or metric == 'Upload':
            sns.ecdfplot(ax=ax, data=group_df, x=upload_column_name, label=upload_column_name, linestyle=linestyle,
                         stat="proportion")


def preprocess_plot_source_2018():
    source_df = pd.read_csv(os.path.join(working_dir_probes, cache_filename), escapechar='\\', index_col=0)
    return source_df


def plot_metric(source_2011, source_2018, metric):
    # start an axis
    plt.clf()
    ax = plt.gca()
    # plot 2011
    plot(source_2011, '2011', ax, linestyle='dashed', metric=metric)
    # plot 2018
    plot(source_2018, '2018', ax, metric=metric)
    # show plot
    ax.set_ylabel('CDF')
    ax.set_xlabel('Advertised Speed [Mbps]')
    ax.set_xscale('log')
    ax.legend(bbox_to_anchor=(0.9, 0.5), fontsize='xx-small')
    ax.set_title('2011/2018 %s' % metric)
    # plt.show()
    return plt


if __name__ == '__main__':
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    plot_source_2011 = preprocess_plot_source_2011()
    plot_source_2018 = preprocess_plot_source_2018()
    plot_metric(plot_source_2011, plot_source_2018, 'Download').savefig(os.path.join(output_dir, 'download.png'),
                                                                        dpi=300)
    plot_metric(plot_source_2011, plot_source_2018, 'Upload').savefig(os.path.join(output_dir, 'upload.png'), dpi=300)
    plot_metric(plot_source_2011, plot_source_2018, 'Download & Upload').savefig(os.path.join(output_dir, 'both.png'),
                                                                                 dpi=300)
