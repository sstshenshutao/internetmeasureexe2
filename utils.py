import os

import pandas as pd


def convert(working_dir, filename, output_filename, sheet_name=0):
    read_file = pd.read_excel(os.path.join(working_dir, filename), sheet_name=sheet_name)
    read_file = read_file.rename(columns={'Unit ID': 'unit_id'})
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
