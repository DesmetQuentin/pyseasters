from datetime import date
from time import time

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np

import pyseasters as pys

# Input
lonmin, lonmax = 115, 135
latmin, latmax = -15, 5
plot_date = "2016-01-02"
beg = date.fromisoformat("2015-01-01")
end = date.fromisoformat("2017-12-31")
query = f"lon >= {lonmin} and lon <= {lonmax} and lat >= {latmin} and lat <= {latmax}"
units = "mm/day"

# Loading
print("Loading data...")
time1 = time()
data, metadata = pys.load_1h_gauge_data(
    filter_condition=query, time_range=(beg, end), units=units
)
time2 = time()
print(f"Done ({(time2 - time1) * 1000:.2f} ms).", end="\n\n")

# Plotting
print("Plotting...")
time1 = time()
prj_ = ccrs.PlateCarree()  # source projection
_prj = ccrs.Orthographic(
    central_longitude=(lonmax + lonmin) / 2,
    central_latitude=(latmax + latmin) / 2,
)  # destination projection
vmin, vmax = np.nanpercentile(data.loc[plot_date].values, [2, 98])
fig = plt.figure()
ax = fig.add_subplot(111, projection=_prj, facecolor="lightgrey")
sc = ax.scatter(
    metadata.lon.values,
    metadata.lat.values,
    c=data.loc[plot_date].values,
    transform=prj_,
    marker="+",
    vmin=0,
    vmax=vmax,
)
plt.colorbar(sc, label=f"Precipitation ({data.attrs['units']})", extend="max")
plt.title(f"Precipitation by station on {plot_date}")
ax.coastlines(resolution="50m", lw=0.3)
gl = ax.gridlines(draw_labels=True, color="gray", ls="--", lw=0.5)
gl.top_labels = False
gl.right_labels = False
time2 = time()
print(f"Done ({(time2 - time1) * 1000:.2f} ms).", end="\n\n")

# Saving
fn = "example.png"
print(f"Writing {fn}...")
time1 = time()
plt.savefig("example.png", bbox_inches="tight", dpi=300)
time2 = time()
print(f"Done ({(time2 - time1) * 1000:.2f} ms).")
