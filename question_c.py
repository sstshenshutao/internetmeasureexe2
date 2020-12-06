import os

import pandas as pd
from utils import convert

working_dir_up = 'csvup'
working_dir_probes = 'csv_valid_probes'
output_dir = 'question_c_output'

# prepare the unit profile data
convert(working_dir_up, 'Unit-Profile-sept2018.xlsx', 'Unit-Profile-sept2018.csv')
df_unit_profile = pd.read_csv(os.path.join(working_dir_up, "Unit-Profile-sept2018.csv"), escapechar='\\')

# read probes
df_probes = pd.read_csv(os.path.join(working_dir_probes, "remaining_probes.csv"), index_col=0)
source_df = df_probes.merge(df_unit_profile, left_on='unit_id', right_on='unit_id')[
    ["unit_id", "ISP", "Technology", "State", "Census", "Download", "Upload"]]


def get_upper_bound(x):
    # print(x, type(x))
    cast_string = str(x).strip()
    if cast_string.startswith('['):
        # [1.1 - 3.0]
        start = cast_string.index('-') + 1
        end = cast_string.index(']')
        return float(cast_string[start:end].strip())
    return float(cast_string)


# clean the range data, use the upper bound
source_df['Upload'] = source_df['Upload'].apply(get_upper_bound)
source_df['Download'] = source_df['Download'].apply(get_upper_bound)
source_df.to_csv(os.path.join(working_dir_probes, "remaining_probes_merge_unit_profile_upper_bound.csv"), mode='w')


# # avg_dl,sd_dl,avg_ul,sd_ul,median_dl,median_ul
def final_table(source_table, group_by="ISP", output=None):
    if output is None:
        output = group_by
    isp_df = source_table[[group_by]].value_counts().reset_index(name='num_probes')
    # merge the avg_dl
    isp_df = isp_df.merge(source_table.groupby(group_by)['Download'].mean().reset_index(name='avg_dl'),
                          left_on=group_by, right_on=group_by)
    # merge the sd_dl
    isp_df = isp_df.merge(source_table.groupby(group_by)['Download'].std().reset_index(name='sd_dl'),
                          left_on=group_by, right_on=group_by)
    # merge the avg_ul
    isp_df = isp_df.merge(source_table.groupby(group_by)['Upload'].mean().reset_index(name='avg_ul'),
                          left_on=group_by, right_on=group_by)
    # merge the sd_ul
    isp_df = isp_df.merge(source_table.groupby(group_by)['Upload'].std().reset_index(name='sd_ul'),
                          left_on=group_by, right_on=group_by)
    # merge the median_dl
    isp_df = isp_df.merge(source_table.groupby(group_by)['Download'].median().reset_index(name='median_dl'),
                          left_on=group_by, right_on=group_by)
    # merge the median_ul
    isp_df = isp_df.merge(source_table.groupby(group_by)['Upload'].median().reset_index(name='median_ul'),
                          left_on=group_by, right_on=group_by)

    # to file
    isp_df.to_csv(os.path.join(output_dir, ("%s.csv" % output)), mode='w', index=False, float_format="%.4f")


if __name__ == '__main__':
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    final_table(source_df, group_by="ISP")
    final_table(source_df, group_by="Technology")
    final_table(source_df, group_by="State")
    final_table(source_df, group_by="Census")

    # merge the IPBB into DSL
    source_df['Technology'] = source_df['Technology'].apply(
        lambda x: 'DSL' if str(x).strip() == 'IPBB' else str(x).strip())
    final_table(source_df, group_by="Technology", output='Technology_without_IPBB')
