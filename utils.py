import os

import pandas as pd


def convert(working_dir, filename, output_filename, sheet_name=0, rename='Unit ID'):
    read_file = pd.read_excel(os.path.join(working_dir, filename), sheet_name=sheet_name)
    read_file = read_file.rename(columns={rename: 'unit_id'})
    read_file.to_csv(os.path.join(working_dir, output_filename), index=None, header=True)


def convert_delete(working_dir, filename, output_filename, sheet_name=0):
    convert(working_dir, filename, output_filename, sheet_name=sheet_name)
    os.remove(os.path.join(working_dir, filename))


def count(file, working_dir):
    huge_filename = os.path.join(working_dir, file)
    dfs = pd.read_csv(huge_filename, chunksize=100000)
    pd.set_option('max_columns', None)
    counter = 0
    for df in dfs:
        counter += df.size
        print("now:", counter)
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
