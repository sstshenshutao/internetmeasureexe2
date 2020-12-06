import os
import json

import pandas as pd


# data 2020
working_dir_2020 = ['csv2020', '202008']
clean_csv_files_2020 = ['curr_httpgetmt6_clean.csv', 'curr_httpgetmt_clean.csv', 'curr_httppostmt6_clean.csv',
                        'curr_httppostmt_clean.csv']
download_files_2020 = clean_csv_files_2020[:2]
upload_files_2020 = clean_csv_files_2020[2:4]
v4_files_2020 = [clean_csv_files_2020[1], clean_csv_files_2020[3]]
v6_files_2020 = [clean_csv_files_2020[0], clean_csv_files_2020[2]]

# data 2018
working_dir_2018 = ['csvdata']
clean_csv_files_2018 = ['curr_httpgetmt6.csv', 'curr_httpgetmt.csv', 'curr_httppostmt6.csv',
                        'curr_httppostmt.csv']
download_files_2018 = clean_csv_files_2018[:2]
upload_files_2018 = clean_csv_files_2018[2:4]
v4_files_2018 = [clean_csv_files_2018[1], clean_csv_files_2018[3]]
v6_files_2018 = [clean_csv_files_2018[0], clean_csv_files_2018[2]]

# common
unit_profile_file_2018 = ['csvup', "Unit-Profile-sept2018.csv"]
valid_unit_profile_file_2018 = ['csv_valid_probes', 'remaining_probes_merge_unit_profile_upper_bound.csv']


def compare_v4_v6(files, working_dir, unit_profile_file=None, year=2018):
    if unit_profile_file is None:
        unit_profile_file = unit_profile_file_2018

    # read ipv4
    source_df_v4 = pd.read_csv(os.path.join(*working_dir, files[1]), escapechar='\\')
    df_v4 = source_df_v4.groupby('unit_id')['bytes_sec_interval'].median().reset_index(name='speed_v4')

    # read ipv6
    source_df_v6 = pd.read_csv(os.path.join(*working_dir, files[0]), escapechar='\\')
    df_v6 = source_df_v6.groupby('unit_id')['bytes_sec_interval'].median().reset_index(name='speed_v6')

    # merge them to calculate the diff
    df_v4_v6 = df_v4.merge(df_v6, left_on='unit_id', right_on='unit_id')
    df_v4_v6['diff_v4_v6'] = df_v4_v6['speed_v4'] - df_v4_v6['speed_v6']
    df_v4_v6['diff_v4_v6_percentage'] = df_v4_v6['diff_v4_v6'] / df_v4_v6['speed_v6']

    # determinate where are those probes with significant difference from
    significant_diff_df = df_v4_v6.query('abs (diff_v4_v6_percentage) > 0.1')
    uf_df = pd.read_csv(os.path.join(*unit_profile_file))
    significant_diff_df = significant_diff_df.merge(uf_df, left_on='unit_id', right_on='unit_id')[["ISP", "Technology"]]
    print("the 10%% outliers ISP (%d)" % year)
    print(significant_diff_df[["ISP"]].value_counts().reset_index(name='num_probes'))
    print("the 10%% outliers Access Technology (%d)" % year)
    print(significant_diff_df[["Technology"]].value_counts().reset_index(name='num_probes'))

    # return the diff of ipv4 and ipv6
    return {
        "size": df_v4_v6['unit_id'].size,
        "absolute median (v4-v6)": "%f Bytes/s" % df_v4_v6['diff_v4_v6'].median(),
        "absolute mean (v4-v6)": "%f Bytes/s" % df_v4_v6['diff_v4_v6'].mean(),
        "relative median ((v4-v6)/v6)": "%f%%" % (df_v4_v6['diff_v4_v6_percentage'].median() * 100),
        "relative mean ((v4-v6)/v6)": "%f%%" % (df_v4_v6['diff_v4_v6_percentage'].mean() * 100)
    }


def rough_speed(files, working_dir, valid_unit_profile_file=None):
    if valid_unit_profile_file is None:
        valid_unit_profile_file = valid_unit_profile_file_2018

    # read download
    source_df_dl = pd.read_csv(os.path.join(*working_dir, files[0]), escapechar='\\')
    source_df_dl_speed = source_df_dl.groupby('unit_id')['bytes_sec_interval'].median().reset_index(name='mdl_speed')[
        ["unit_id", "mdl_speed"]]

    # read upload
    source_df_ul = pd.read_csv(os.path.join(*working_dir, files[1]), escapechar='\\')
    source_df_ul_speed = source_df_ul.groupby('unit_id')['bytes_sec_interval'].median().reset_index(name='mul_speed')[
        ["unit_id", "mul_speed"]]

    # merge them with uf
    df_dl_ul = source_df_dl_speed.merge(source_df_ul_speed, left_on='unit_id', right_on='unit_id')
    uf_df = pd.read_csv(os.path.join(*valid_unit_profile_file), index_col=0)
    merged_df = df_dl_ul.merge(uf_df, left_on='unit_id', right_on='unit_id')[
        ["unit_id", "Download", "Upload", "mdl_speed", "mul_speed"]]
    merged_df['Download'] = merged_df['Download'] * 131072
    merged_df['Upload'] = merged_df['Upload'] * 131072
    merged_df['dl_rate'] = merged_df['mdl_speed'] / merged_df['Download']
    merged_df['ul_rate'] = merged_df['mul_speed'] / merged_df['Upload']
    total_size = merged_df["Download"].size
    size_75 = merged_df.query('dl_rate > 0.75 & ul_rate > 0.75')["Download"].size
    size_95 = merged_df.query('dl_rate > 0.95 & ul_rate > 0.95')["Download"].size
    size_99 = merged_df.query('dl_rate > 0.99 & ul_rate > 0.99')["Download"].size
    merged_df['Download'] = merged_df['Download'] / 131072
    merged_df['Upload'] = merged_df['Upload'] / 131072
    var_upload = merged_df.query('dl_rate < 1 & ul_rate <1')['Upload'].var()
    var_download = merged_df.query('dl_rate < 1 & ul_rate <1')['Download'].var()
    return {
        "total number": total_size,
        "75%": "number: %d, percentage: %f%%" % (size_75, size_75 / total_size * 100),
        "95%": "number: %d, percentage: %f%%" % (size_95, size_95 / total_size * 100),
        "99%": "number: %d, percentage: %f%%" % (size_99, size_99 / total_size * 100),
        "Upload variance (not reached)": "%f Mbps^2" % var_upload,
        "Download variance (not reached)": "%f Mbps^2" % var_download,
    }


if __name__ == '__main__':
    # the dataset 2018:
    print("Download Difference v4 & v6 (2018): ",
          json.dumps(compare_v4_v6(download_files_2018, working_dir_2018), indent=4))
    print("Upload Difference v4 & v6 (2018): ",
          json.dumps(compare_v4_v6(upload_files_2018, working_dir_2018), indent=4))
    print("v4 Speed Comparison (2018): ", json.dumps(rough_speed(v4_files_2018, working_dir_2018), indent=4))
    print("v6 Speed Comparison (2018): ", json.dumps(rough_speed(v6_files_2018, working_dir_2018), indent=4))
    print("----------------------------------------------------")
    # the dataset 2020:
    print("Download Difference v4 & v6 (2020): ",
          json.dumps(compare_v4_v6(download_files_2020, working_dir_2020, year=2020), indent=4))
    print("Upload Difference v4 & v6 (2020): ",
          json.dumps(compare_v4_v6(upload_files_2020, working_dir_2020, year=2020), indent=4))
    print("v4 Speed Comparison (2020): ", json.dumps(rough_speed(v4_files_2020, working_dir_2020), indent=4))
    print("v6 Speed Comparison (2020): ", json.dumps(rough_speed(v6_files_2020, working_dir_2020), indent=4))
