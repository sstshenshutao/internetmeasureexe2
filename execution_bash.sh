#/bin/bash
# question b)

# count all csv and generate a file csvdata/tmp_ids.csv
python csv_id_counter.py

# convert exclude unit excel to csv and generate two files: csvexcl/tmp_ids.csv ./remaining_probes.csv
python exclude_probes.py

# run gpd to generate the plots
python gpd_visual.py

# question c)

# the output csv in ./question_c_output/*.csv
python question_c.py

# question d)

# the output csv in ./question_d_output/*.png
python question_d.py

# question f)

# the output csv in folder 'csv2020/202008/*_clean.csv'
python question_f.py

# question g)

# the output in commandline
python question_g.py