Rain gauge data
===============

The ``load_gauge_data()`` function
----------------------------------

PySEASTERS provides the ``load_gauge_data()`` function to load rain gauge daily data,
optionally applying spatio-temporal filters and more.
The function returns two ``pandas`` DataFrames: the first contains precipitation data
by stations throughout time; the second provides metadata for those same stations.


.. _guide-gauge-format:

Returned format
~~~~~~~~~~~~~~~

Let us first have a look at the ``data`` and ``metadata`` DataFrames returned by the
``load_gauge_data()`` function. With the following, filters are applied to load a
subset of the rain gauge database (more details in the next section on
:ref:`filtering <guide-gauge-filter>`), then we can see how the result is formatted:

.. code:: pycon

   >>> from datetime import datetime
   >>> import pyseasters as ps
   >>> data, metadata = ps.load_gauge_data(
   ...     filter_condition="lon > 100 and lon < 130 and lat > 15 and lat < 25",
   ...     time_range=[datetime(2017, 1, 1), datetime(2017, 12, 31)],
   ...     usesources=["GHCNd"]
   ... )
   >>> data
               GHCNd:CHM00056951  GHCNd:CHM00056964  ...  GHCNd:VMM00048848  GHCNd:VMM00048855
   time                                              ...
   2017-01-01                NaN                NaN  ...                0.0              297.0
   2017-01-02                NaN               58.0  ...                0.0              104.0
   2017-01-03              325.0              564.0  ...               43.0               33.0
   2017-01-04               38.0               64.0  ...               10.0               76.0
   2017-01-05               23.0               48.0  ...                8.0               76.0
   ...                       ...                ...  ...                ...                ...
   2017-12-27                NaN                8.0  ...               10.0              150.0
   2017-12-28                NaN                NaN  ...                0.0                0.0
   2017-12-29                NaN                NaN  ...                0.0                8.0
   2017-12-30                NaN                NaN  ...                0.0               23.0
   2017-12-31                NaN              213.0  ...              147.0                0.0

   [365 rows x 63 columns]
   >>> data.attrs
   {'name': 'Precipitation', 'units': 'mm'}
   >>> metadata
                         lat      lon  elevation                                  station_name
   station_id
   GHCNd:CHM00056951  23.950  100.217     1503.0  LINCANG                                56951
   GHCNd:CHM00056964  22.767  100.983     1303.0  SIMAO                                  56964
   GHCNd:CHM00056985  23.383  103.383     1302.0  MENGZI                         GSN     56985
   GHCNd:CHM00059023  24.700  108.050      214.0  HECHI                                  59023
   GHCNd:CHM00059082  24.667  113.600       68.0  SHAOGUAN                               59082
   ...                   ...      ...        ...                                           ...
   GHCNd:VMM00048830  21.833  106.767      263.0  LANG SON                               48830
   GHCNd:VMM00048840  19.750  105.783        5.0  THANH HOA                              48840
   GHCNd:VMM00048845  18.737  105.671        5.2  VINH                                   48845
   GHCNd:VMM00048848  17.483  106.600        8.0  DONG HOI                               48848
   GHCNd:VMM00048855  16.044  108.199       10.1  DANANG INTL                    GSN     48855

   [63 rows x 4 columns]


* ``data`` shows for each station across columns a time series of its precipitation data
  across a time axis in index that is shared for all stations (using ``NaN`` to fill
  the blanks). The DataFrame has two attributes: "name", containing the standard
  variable name for the data, here "Precipitation", and "units" which contains unit
  information, in this case, "mm".

* ``metadata`` contains station metadata for all the stations in ``data.columns``.
  This includes "lat" and "lon" for latitude and longitude, respectively, "elevation"
  and "station_name" which, depending on the source and the country, includes a standard
  name (e.g., the city), a postal code, etc.

.. note::

   The "station_id" is formatted as ``<source>:<original_station_id>``.


.. _guide-gauge-filter:

Filtering
~~~~~~~~~

Calling ``load_gauge_data()`` without any argument would load **all time** rain gauge
data from **every station** in the database (i.e. from the extended Southeast Asian
region). Such a call can take minutes or more: **it is not recommended** for obvious
memory concerns.

.. attention::

   .. code:: python

      """ NOT RECOMMENDED """
      import pyseasters as ps
      data, metadata = ps.load_gauge_data()  # <-- /!\ No argument!


Instead, several arguments enable filtering the database.
You can for instance filter based on the **data source**
using the ``usesources`` keyword argument:

.. code:: python

   data, metadata = ps.load_gauge_data(usesources=["GHCNd"])


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

   data, metadata = ps.load_gauge_data(
       filter_condition="lon > 100 and lon < 130 and lat > 10 and lat < 30"
   )


Lastly, **time filtering** can be done using the ``time_range`` argument, with the
begining and ending dates of the desired interval:

.. code:: python

   from datetime import datetime

   data, metadata = ps.load_gauge_data(
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

   data, metadata = ps.load_gauge_data(
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

The ``load_gauge_data()`` function also have a ``units`` keyword argument, allowing
users to choose the output unit of the result in ``data``. Although we are dealing
with daily rainfall data, hence limiting the application of such an option, the first
example of this page can be reran by changing units, as follows:

.. code:: pycon

   >>> data, metadata = ps.load_gauge_data(
   ...     filter_condition="lon > 100 and lon < 130 and lat > 15 and lat < 25",
   ...     time_range=[datetime(2017, 1, 1), datetime(2017, 12, 31)],
   ...     usesources=["GHCNd"],
   ...     units="cm",
   ... )
   >>> data
               GHCNd:CHM00056951  GHCNd:CHM00056964  ...  GHCNd:VMM00048848  GHCNd:VMM00048855
   time                                              ...
   2017-01-01                NaN                NaN  ...                0.0               29.7
   2017-01-02                NaN                5.8  ...                0.0               10.4
   2017-01-03               32.5               56.4  ...                4.3                3.3
   2017-01-04                3.8                6.4  ...                1.0                7.6
   2017-01-05                2.3                4.8  ...                0.8                7.6
   ...                       ...                ...  ...                ...                ...
   2017-12-27                NaN                0.8  ...                1.0               15.0
   2017-12-28                NaN                NaN  ...                0.0                0.0
   2017-12-29                NaN                NaN  ...                0.0                0.8
   2017-12-30                NaN                NaN  ...                0.0                2.3
   2017-12-31                NaN               21.3  ...               14.7                0.0

   [365 rows x 63 columns]
   >>> data.attrs
   {'name': 'Precipitation', 'units': 'cm'}


.. note::

   This feature relies on
   `Pint Python library <https://pint.readthedocs.io/en/stable/>`_, notably for parsing
   unit strings, making it quite **flexible**: e.g., "mm" is equivalent to
   "millimeter".


Integration with ``xarray``
---------------------------

Although ``xarray`` is not currently a dependency of PySEASTERS, using ``xarray`` tools
can be done quite quickly based on the outputs of PySEASTERS functions.
For instance, an ``xarray`` DataArray can be constructed using the data and metadata
results of the ``load_gauge_data()`` function:

.. code:: python

   from datetime import datetime

   import pyseasters as ps
   import xarray as xr


   # Load
   d, md = ps.load_gauge_data(
       filter_condition="lon > 100 and lon < 130 and lat > 10 and lat < 30",
       time_range=[
           datetime(2018, 1, 1),
           datetime(2018, 3, 31)
       ],
       usesources=["GHCNd"],
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
using a given space-time filter with PySEASTERS ``load_gauge_data()`` function,
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
   plot_date = "2016-01-02"
   beg = date.fromisoformat("2015-01-01")
   end = date.fromisoformat("2017-12-31")
   query = f"lon >= {lonmin} and lon <= {lonmax} and lat >= {latmin} and lat <= {latmax}"
   units = "mm"

   # Load
   data, metadata = ps.load_gauge_data(
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
