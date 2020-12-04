import os
import geopandas
import matplotlib.pyplot as plt
from utils import convert
import pandas as pd

working_dir_cb = "csvcb"
working_dir_probes = 'csv_valid_probes'
working_dir_uf = 'csvup'
output_dir = 'question_b_output'

# prepare the census data
convert(working_dir_cb, 'UnitID-census-block-sept2018.xlsx', 'UnitID-census-block-sept2018.csv', rename='unit id')
df_unit_census = pd.read_csv(os.path.join(working_dir_cb, "UnitID-census-block-sept2018.csv"), escapechar='\\')

# prepare the unit profile data
convert(working_dir_uf, 'Unit-Profile-sept2018.xlsx', 'Unit-Profile-sept2018.csv')
df_unit_profile = pd.read_csv(os.path.join(working_dir_uf, "Unit-Profile-sept2018.csv"), escapechar='\\')

df_probes = pd.read_csv(os.path.join(working_dir_probes, "remaining_probes.csv"), index_col=0)
source_df = df_probes.merge(df_unit_census, left_on='unit_id', right_on='unit_id')[
    ["unit_id", "latitude", "longitude"]
]
source_df = source_df.merge(df_unit_profile, left_on='unit_id', right_on='unit_id')[
    ["unit_id", "latitude", "longitude", "Technology"]
]
# 3120 rows, 8 missed
# print(source_df)

source_df_without_ipbb = source_df.copy()
# merge the IPBB into DSL
source_df_without_ipbb['Technology'] = source_df_without_ipbb['Technology'].apply(
    lambda x: 'DSL' if str(x).strip() == 'IPBB' else str(x).strip())


def generate_picture(some_source):
    # plot the USA map
    america = geopandas.read_file('geojson/States21basic.geojson')
    base = america.plot(color='white', edgecolor='black')
    # generate geopandas
    gdf = geopandas.GeoDataFrame(some_source,
                                 geometry=geopandas.points_from_xy(some_source.longitude, some_source.latitude))
    # plot it on the USA map
    gdf.plot(ax=base, marker='.', column='Technology', markersize=1, categorical=True, legend=True, cmap='rainbow')


if __name__ == '__main__':
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    # It shows the plot with ipbb
    generate_picture(source_df)
    plt.savefig(os.path.join(output_dir, 'map_with_ipbb.png'), dpi=300)

    # It shows the plot without ipbb
    generate_picture(source_df_without_ipbb)
    plt.savefig(os.path.join(output_dir, 'map_without_ipbb.png'), dpi=300)
