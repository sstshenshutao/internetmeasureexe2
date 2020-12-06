#/bin/bash
# question b)
echo "Downloading the data for question b"
# download the dataset
mkdir -p csvdata
python downloader.py --url="https://data.fcc.gov/download/measuring-broadband-america/2019/validated-data-sept2018.tar.gz" --dist_dir="./csvdata"
tar -xvf "./csvdata/validated-data-sept2018.tar.gz" -C "./csvdata/"

# download the excluded-units
mkdir -p csvexcl
python downloader.py --url="http://data.fcc.gov/download/measuring-broadband-america/2019/excluded-units-sept2018.xlsx" --dist_dir="./csvexcl"

# download the Unit-Profile
mkdir -p csvup
python downloader.py --url="http://data.fcc.gov/download/measuring-broadband-america/2019/Unit-Profile-sept2018.xlsx" --dist_dir="./csvup"

# download the census-block
mkdir -p csvcb
python downloader.py --url="http://data.fcc.gov/download/measuring-broadband-america/2019/UnitID-census-block-sept2018.xlsx" --dist_dir="./csvcb"

# question d)
echo "Downloading the data for question d"
mkdir -p csv2011
python downloader.py --url="http://data.fcc.gov/download/measuring-broadband-america/validated-march-data-2011.tar.gz" --dist_dir="./csv2011"
tar -xvf "./csv2011/validated-march-data-2011.tar.gz" -C "./csv2011/"

# question f)
echo "Downloading the data for question f"
mkdir -p csv2020
python downloader.py --url="https://data.fcc.gov/download/measuring-broadband-america/2020/data-raw-2020-aug.tar.gz" --dist_dir="./csv2020"
tar -zxvf "./csv2020/data-raw-2020-aug.tar.gz" -C "./csv2020/"
