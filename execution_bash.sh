#/bin/bash
# question b)

# count all csv and generate a file csvdata/tmp_ids.csv
python csv_id_counter.py

# convert exclude unit excel to csv and generate two files: csvexcl/tmp_ids.csv ./remaining_probes.csv
python exclude_probes.py

# question c)

# the output csv in ./question_c_output/*.csv
python question_c.py

# download the excluded-units
mkdir -p csvexcl
wget --directory-prefix='./csvexcl' --cipher 'DEFAULT:!DH' --secure-protocol tlsv1 http://data.fcc.gov/download/measuring-broadband-america/2019/excluded-units-sept2018.xlsx

# download the Unit-Profile
mkdir -p csvup
wget --directory-prefix='./csvup' --cipher 'DEFAULT:!DH' --secure-protocol tlsv1 http://data.fcc.gov/download/measuring-broadband-america/2019/Unit-Profile-sept2018.xlsx
