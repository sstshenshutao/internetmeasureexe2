import contextily as cx
import geopandas
import rasterio
from rasterio.plot import show as rioshow
import matplotlib.pyplot as plt

data_url = "https://ndownloader.figshare.com/files/20232174"

db = geopandas.read_file(data_url)
# print("db", db)
ax = db.plot(color="red", figsize=(9, 9))
# print("ax", ax)

# row0 = db.query("city_id=='ci000'")
ax = ax.plot(color="red", figsize=(9, 9), alpha=0.5)
cx.add_basemap(ax, crs=db.crs.to_string())

plt.show()
