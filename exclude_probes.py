import os

import pandas as pd
from utils import convert
from csv_id_counter import csv_count_ids

tmp_filename = "tmp_ids.csv"
working_dir1 = 'csvdata'
working_dir_excl = 'csvexcl'
output_dir = 'csv_valid_probes'


def diff():
    df1 = pd.read_csv(os.path.join(working_dir1, tmp_filename), index_col=0)
    df2 = pd.read_csv(os.path.join(working_dir_excl, tmp_filename), index_col=0)
    df = df1.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
    return df[['unit_id']].drop_duplicates().reset_index(drop=True)


if __name__ == '__main__':
    # convert to csv and change the column name
    convert(working_dir_excl, 'excluded-units-sept2018.xlsx', 'excluded-units-sept2018.csv',
            sheet_name='Excluded')
    # call the csv_count_ids to generate a tmp file
    id_size = csv_count_ids(['excluded-units-sept2018.csv'], working_dir=working_dir_excl,
                            tmp_ids_filename=tmp_filename)
    # run diff of two dfs
    valid_excluded_id_df = diff()
    print("number of valid excluded probes: %d" % valid_excluded_id_df.size)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    valid_excluded_id_df.to_csv(os.path.join(output_dir, "remaining_probes.csv"), mode='w')
    # convert('csvup', 'Unit-Profile-sept2018.xlsx', 'Unit-Profile-sept2018.csv')
