import os

import pandas as pd
from utils import convert

# prepare the unit profile information
working_dir_uf = 'csvup'
output_dir = 'question_c_output'
convert(working_dir_uf, 'Unit-Profile-sept2018.xlsx', 'Unit-Profile-sept2018.csv')
df_unit_profile = pd.read_csv(os.path.join(working_dir_uf, "Unit-Profile-sept2018.csv"), escapechar='\\')

df_probes = pd.read_csv("remaining_probes.csv", index_col=0)
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
    isp_df = isp_df.merge(source_table.groupby(group_by)['Download'].mean().reset_index(name='median_dl'),
                          left_on=group_by, right_on=group_by)
    # merge the median_ul
    isp_df = isp_df.merge(source_table.groupby(group_by)['Upload'].std().reset_index(name='median_ul'),
                          left_on=group_by, right_on=group_by)
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
#
# technology_count_df = source_df[["Technology"]].value_counts().reset_index(name='num_probes')
# print(technology_count_df)
#
# state_count_df = source_df[["State"]].value_counts().reset_index(name='num_probes')
# print(state_count_df)

# census_count_df = source_df[["Census"]].value_counts().reset_index(name='num_probes')
# print(census_count_df)

# cc = df_probes[["unit_id"]].apply(to_technology, axis=1, result_type='expand')
# def to_technology(series):
#     print(series, type(series), series['unit_id'], type(series['unit_id']))
#     search = df_unit_profile[df_unit_profile['unit_id'] == series['unit_id']]
#     print(search.size)
#     return search[["unit_id", "ISP", "Technology", "State", "Census"]].values[0]
#
#
# # ser_to = pd.Series({"kkk": 27681})
# ser_to = to_technology(pd.Series({"unit_id": 458}))
# print(ser_to, type(ser_to))

# df_probes["unit_id"], df_probes["ISP"], df_probes["Technology"], df_probes["State"], df_probes[
#     "Census"] = to_technology(df_probes["unit_id"])
# ser = df_probes.iloc[:, 0]
# print(ser, type(ser))
# df_probes[["unit_id", "ISP", "Technology", "State", "Census"]] = df_probes.iloc[:, 0].map(to_technology, axis=1)
# print(df_probes)
# cc = df_probes.iloc[:, 0].map(df_unit_profile.set_index('unit_id')[["unit_id", "ISP", "Technology", "State", "Census"]])
# print(cc)
# cc = df_probes["unit_id"].map(dict(df_unit_profile[["unit_id", "ISP", "Technology", "State", "Census"]].values[0]))
# print(cc)
