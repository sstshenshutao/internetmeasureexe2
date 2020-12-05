#/bin/bash
#question b)
# download the dataset
wget --cipher 'DEFAULT:!DH' --secure-protocol tlsv1 https://data.fcc.gov/download/measuring-broadband-america/2019/validated-data-sept2018.tar.gz
mkdir csvdata
tar -xvf ./validated-data-sept2018.tar.gz -C ./csvdata

# download the excluded-units
mkdir -p csvexcl
wget --directory-prefix='./csvexcl' --cipher 'DEFAULT:!DH' --secure-protocol tlsv1 http://data.fcc.gov/download/measuring-broadband-america/2019/excluded-units-sept2018.xlsx

# download the Unit-Profile
mkdir -p csvup
wget --directory-prefix='./csvup' --cipher 'DEFAULT:!DH' --secure-protocol tlsv1 http://data.fcc.gov/download/measuring-broadband-america/2019/Unit-Profile-sept2018.xlsx

# download the census-block
mkdir -p csvcb
wget --directory-prefix='./csvcb' --cipher 'DEFAULT:!DH' --secure-protocol tlsv1  http://data.fcc.gov/download/measuring-broadband-america/2019/UnitID-census-block-sept2018.xlsx

#question d)
mkdir -p csv2011
wget --directory-prefix='./csv2011' --cipher 'DEFAULT:!DH' --secure-protocol tlsv1  http://data.fcc.gov/download/measuring-broadband-america/validated-march-data-2011.tar.gz
tar -xvf ./csv2011/validated-data-sept2018.tar.gz -C ./csv2011/