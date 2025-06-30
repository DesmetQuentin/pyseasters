.. _guide-rain-gauge:

Rain gauge data
===============

The :func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data` function
----------------------------------------------------------------------

PySEASTERS provides the :func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data`
function to load rain gauge daily data,
optionally applying spatio-temporal filters and more.
The function returns two ``pandas`` DataFrames: the first contains precipitation data
by stations throughout time; the second provides metadata for those same stations.


.. _guide-gauge-format:

Returned format
~~~~~~~~~~~~~~~

Let us first have a look at the ``data`` and ``metadata`` DataFrames returned by the
:func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data` function. With the following,
filters are applied to load a
subset of the rain gauge database (more details in the next section on
:ref:`filtering <guide-gauge-filter>`), then we can see how the result is formatted:

.. code:: pycon

   >>> from datetime import datetime
   >>> import pyseasters as ps
   >>> data, metadata = ps.load_1h_gauge_data(
   ...     filter_condition="lon > 100 and lon < 130 and lat > 15 and lat < 25",
   ...     time_range=[datetime(2010, 1, 1), datetime(2011, 12, 31)],
   ...     usesources=["GHCNh"]
   ... )
   >>> data
                              GHCNh:THI0000VTCP  ...  GHCNh:TWI0000RCKH
   time                                          ...
   2010-01-02 12:00:00+00:00                NaN  ...                0.3
   2010-01-03 00:00:00+00:00                NaN  ...                0.5
   2010-01-03 02:00:00+00:00                NaN  ...                0.3
   2010-01-06 09:00:00+00:00                NaN  ...                NaN
   2010-01-06 21:00:00+00:00                NaN  ...                NaN
   ...                                      ...  ...                ...
   2011-12-13 03:00:00+00:00                NaN  ...                0.5
   2011-12-13 05:00:00+00:00                NaN  ...                0.8
   2011-12-13 06:00:00+00:00                NaN  ...                1.3
   2011-12-19 18:00:00+00:00                NaN  ...                0.5
   2011-12-19 19:00:00+00:00                NaN  ...                1.0

   [1692 rows x 29 columns]
   >>> data.attrs
   {'name': 'Total liquid precipitation', 'long_name': 'Total liquid precipitation (rain or melted snow). Totals are nominally for the hour, but may include intermediate reports within the hour.', 'note': "A 'T' in the measurement code column indicates a trace amount of precipitation.", 'units': 'millimeter'}
   >>> metadata
                          lat       lon  elevation           station_name
   station_id
   GHCNh:THI0000VTCP  18.1322  100.1647      164.0                  PHRAE
   GHCNh:THI0000VTPB  16.6760  101.1951      137.2             PHETCHABUN
   GHCNh:THI0000VTPN  15.6730  100.1368       34.4           NAKHON SAWAN
   GHCNh:THI0000VTUL  17.4391  101.7221      262.1                   LOEI
   GHCNh:THI0000VTUU  15.2513  104.8702      123.7       UBON RATCHATHANI
   GHCNh:THM00048307  19.4167  100.8833      335.0             TUNG CHANG
   GHCNh:THM00048315  19.1167  100.8000      237.0           THA WANG PHA
   GHCNh:THM00048333  18.8667  100.7500      264.0            NAN AGROMET
   GHCNh:THM00048350  17.4000  101.7333      264.0           LOEI AGROMET
   GHCNh:THM00048351  17.6167  100.1000       64.0              UTTARADIT
   GHCNh:THM00048355  17.1167  104.0500      192.0   SAKON NAKHON AGROMET
   GHCNh:THM00048357  17.4167  104.7833      148.0          NAKHON PHANOM
   GHCNh:THM00048358  17.4333  104.7833      153.0  NAKHON PHANOM AGROMET
   GHCNh:THM00048360  17.2167  102.4167      228.0          NONGBUALAMPHU
   GHCNh:THM00048374  16.7667  101.2500      145.0                 LOMSAK
   GHCNh:THM00048382  16.2500  103.0667      154.0            KOSUMPHISAI
   GHCNh:THM00048383  16.5333  104.7167      140.0               MUKDAHAN
   GHCNh:THM00048384  16.3333  102.8167      166.0       THA PHRA AGROMET
   GHCNh:THM00048386  16.3333  100.3667       39.4         PICHIT AGROMET
   GHCNh:THM00048390  16.3333  103.5833      140.9              KAMALASAI
   GHCNh:THM00048401  15.3500  100.5000       87.0          TAKFA AGROMET
   GHCNh:THM00048403  15.8000  102.0333      184.0             CHAIYAPHUM
   GHCNh:THM00048404  16.0667  103.6167      156.0         ROI ET AGROMET
   GHCNh:THM00048409  15.0853  104.3306      129.2       SI SAKET AGROMET
   GHCNh:THM00048413  15.6569  101.1053       69.7           WICHIAN BURI
   GHCNh:THM00048416  15.3167  103.6833      130.0                THA TUM
   GHCNh:THM00048418  15.2667  101.2000       51.0               BUA CHUM
   GHCNh:THM00048437  15.2333  103.2500      184.0                BURERAM
   GHCNh:TWI0000RCKH  22.5771  120.3500        7.9         KAOHSIUNG INTL


* ``data`` shows for each station across columns a time series of its precipitation data
  across a time axis in index that is shared for all stations (using ``NaN`` to fill
  the blanks). The DataFrame has several attributes, including "name", containing the
  standard variable name for the data, here "Total liquid precipitation", and "units"
  which contains unit information, in this case, "millimeter".

* ``metadata`` contains station metadata for all the stations in ``data.columns``.
  This includes "lat" and "lon" for latitude and longitude, respectively, "elevation"
  and "station_name" which, depending on the source and the station, may contain more
  specific information than just a city name, between square brackets (for this example,
  you may refer to this documentation's page on the :ref:`GHCNh dataset <ghcnh>`).

.. note::

   The "station_id" is formatted as ``<source>:<original_station_id>``.


.. _guide-gauge-filter:

Filtering
~~~~~~~~~

Calling :func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data` without any argument
would load **all time** rain gauge
data from **every station** in the database (i.e. from the
:ref:`extended Southeast Asian region <SEA>`). Such a call can take minutes or more:
**it is not recommended** for obvious memory concerns.

.. attention::

   .. code:: python

      """ NOT RECOMMENDED """
      import pyseasters as ps
      data, metadata = ps.load_1h_gauge_data()  # <-- /!\ No argument!


Instead, several arguments enable filtering the database.
You can for instance filter based on the **data source**
using the ``usesources`` keyword argument:

.. code:: python

   data, metadata = ps.load_1h_gauge_data(usesources=["GHCNd"])


.. admonition:: Currently supported sources

   .. hlist::
      :columns: 5

      * :ref:`GHCNd <ghcnd>`


The ``filter_condition`` argument also enables filtering using **station metadata**,
using specific keywords amongst "lat", "lon", "elevation", "station_id" and
"station_name". Note that filtering on the "station_id" refers to the
``<original_station_id>`` mentioned in the :ref:`previous section <guide-gauge-format>`
(i.e., the one without the ``<source>:`` prefix).
``filter_condition`` can be used to apply some spatial filtering, for instance with:

.. code:: python

   data, metadata = ps.load_1h_gauge_data(
       filter_condition="lon > 100 and lon < 130 and lat > 10 and lat < 30"
   )


Lastly, **time filtering** can be done using the ``time_range`` argument, with the
begining and ending dates of the desired interval:

.. code:: python

   from datetime import datetime

   data, metadata = ps.load_1h_gauge_data(
       time_range=[
           datetime(2018, 1, 1),
           datetime(2018, 3, 31)
       ]
   )


Naturally, all three types of filtering --
i.e., based on the source with ``usesources``,
on station metadata with ``filter_condition``
and on a time interval with ``time_range`` --
can be applied together:

.. code:: python

   from datetime import datetime

   data, metadata = ps.load_1h_gauge_data(
       filter_condition="lon > 100 and lon < 130 and lat > 10 and lat < 30",
       time_range=[
           datetime(2018, 1, 1),
           datetime(2018, 3, 31)
       ],
       usesources=["GHCNd"],
   )


.. note::

   Filtering is done **prior** to loading the data based on metadata stored in
   separate files. This ensures **filtering saves time and memory**.


Units
~~~~~

The :func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data` function also have a
``units`` keyword argument, allowing
users to choose the output unit of the result in ``data``. Although we are dealing
with daily rainfall data, hence limiting the application of such an option, the first
example of this page can be reran by changing units, as follows:

.. code:: pycon

   >>> data, metadata = ps.load_1h_gauge_data(
   ...     filter_condition="lon > 100 and lon < 130 and lat > 15 and lat < 25",
   ...     time_range=[datetime(2010, 1, 1), datetime(2011, 12, 31)],
   ...     usesources=["GHCNh"],
   ...     units="cm",
   ... )
   >>> data
                              GHCNh:THI0000VTCP  ...  GHCNh:TWI0000RCKH
   time                                          ...
   2010-01-02 12:00:00+00:00                NaN  ...               0.03
   2010-01-03 00:00:00+00:00                NaN  ...               0.05
   2010-01-03 02:00:00+00:00                NaN  ...               0.03
   2010-01-06 09:00:00+00:00                NaN  ...                NaN
   2010-01-06 21:00:00+00:00                NaN  ...                NaN
   ...                                      ...  ...                ...
   2011-12-13 03:00:00+00:00                NaN  ...               0.05
   2011-12-13 05:00:00+00:00                NaN  ...               0.08
   2011-12-13 06:00:00+00:00                NaN  ...               0.13
   2011-12-19 18:00:00+00:00                NaN  ...               0.05
   2011-12-19 19:00:00+00:00                NaN  ...               0.10

   [1692 rows x 29 columns]
   >>> data.attrs
   {'name': 'Total liquid precipitation', 'long_name': 'Total liquid precipitation (rain or melted snow). Totals are nominally for the hour, but may include intermediate reports within the hour.', 'note': "A 'T' in the measurement code column indicates a trace amount of precipitation.", 'units': 'cm'}


Integration with ``xarray``
---------------------------

Although ``xarray`` is not currently a dependency of PySEASTERS, using ``xarray`` tools
can be done quite quickly based on the outputs of PySEASTERS functions.
For instance, an ``xarray`` DataArray can be constructed using the data and metadata
results of the :func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data` function:

.. code:: python

   from datetime import datetime

   import pyseasters as ps
   import xarray as xr


   # Load
   d, md = ps.load_1h_gauge_data(
       filter_condition="lon > 100 and lon < 130 and lat > 10 and lat < 30",
       time_range=[
           datetime(2010, 1, 1),
           datetime(2011, 3, 31)
       ],
       usesources=["GHCNh"],
   )

   # Build the DataArray
   da = xr.DataArray(
       d.values,
       dims=["time", "station_id"],
       coords={
           "time": d.index,
           "station_id": d.columns,
           "lat": ("station_id", md["lat"]),
           "lon": ("station_id", md["lon"]),
           "elevation": ("station_id", md["elevation"]),
           "station_name": ("station_id", md["station_name"]),
       },
       attrs=d.attrs,
       name="precipitation",
   )


Integration with ``matplotlib``
-------------------------------

The following script is a minimal working example loading station data and metadata
using a given space-time filter with PySEASTERS
:func:`~pyseasters.gauge_data_loaders.load_1h_gauge_data` function,
then plotting one day's data over a map, using ``matplotlib`` and ``cartopy``.

.. code:: python

   from datetime import date

   import cartopy.crs as ccrs
   import matplotlib.pyplot as plt
   import numpy as np
   import pyseasters as ps


   # Input
   lonmin, lonmax = 115, 135
   latmin, latmax = -15, 5
   plot_date = "2011-01-02"
   beg = date.fromisoformat("2010-01-01")
   end = date.fromisoformat("2011-12-31")
   query = f"lon >= {lonmin} and lon <= {lonmax} and lat >= {latmin} and lat <= {latmax}"
   units = "mm"

   # Load
   data, metadata = ps.load_1h_gauge_data(
      filter_condition=query, time_range=(beg, end), units=units
   )

   # Plot
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

   plt.show()
