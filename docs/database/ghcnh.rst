.. _ghcnh:

Global Historical Climatology Network hourly (GHCNh)
====================================================

Description
-----------

.. epigraph::

   The Global Historical Climatology Network hourly (GHCNh) is a next generation
   hourly/synoptic dataset that replaces the Integrated Surface Dataset (ISD). GHCNh
   consists of hourly and synoptic surface weather observations from fixed, land-based
   stations. This dataset is compiled from numerous data sources maintained by NOAA, the
   U.S. Air Force, and many other meteorological agencies (Met Services) around the
   world. These sources have been reformatted into a common data format and then
   harmonized into a set of unique period-of-record station files, which are then
   provided as GHCNh.

   **Source:** `NOAA NCEI on GHCNh <https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly>`_
   (last accessed 2025-05-12)


.. important::

   Based in the USA, this dataset may become subject to download restrictions in the
   future. However, the data already downloaded into the SEASTERS database should remain
   unaffected.


Data access with PySEASTERS
---------------------------

With PySEASTERS, the most direct way to access GHCNh data is by using the
:doc:`load_ghcnh() <../api/pyseasters.ghcnh.data_loaders>` function. Hereafter is a code
snippet applying this function with some filtering:

.. code:: pycon

   >>> from datetime import datetime
   >>> import pyseasters as ps
   >>> data, metadata = ps.load_ghcnh(
   ...    var="temperature",  # Looks for temperature
   ...    filter_condition="lon > 100 and lon < 130 and lat > 15 and lat < 25",
   ...    time_range=[datetime(2017, 1, 1), datetime(2017, 12, 31)],
   ... )
   >>> data
                              CHI0000ZGBH  CHI0000ZGGG  ...  VMM00048848  VMW00041024
   time                                                 ...
   2017-01-01 00:00:00+00:00          NaN         13.0  ...         18.0         20.0
   2017-01-01 00:07:00+00:00          NaN          NaN  ...          NaN          NaN
   2017-01-01 00:11:00+00:00          NaN          NaN  ...          NaN          NaN
   2017-01-01 00:30:00+00:00          NaN         14.0  ...          NaN         21.0
   2017-01-01 00:40:00+00:00          NaN          NaN  ...          NaN          NaN
   ...                                ...          ...  ...          ...          ...
   2017-12-30 23:00:00+00:00          NaN         14.0  ...          NaN         22.0
   2017-12-30 23:30:00+00:00          NaN         14.0  ...          NaN         22.0
   2017-12-30 23:45:00+00:00          NaN          NaN  ...          NaN          NaN
   2017-12-30 23:48:00+00:00          NaN          NaN  ...          NaN          NaN
   2017-12-31 00:00:00+00:00          NaN         14.0  ...         19.1         22.0

   [27441 rows x 130 columns]
   >>> metadata
                    lat       lon  elevation    station_name
   station_id
   CHI0000ZGBH  21.5394  109.2939       16.0  BEIHAI AIRPORT
   CHI0000ZGGG  23.3924  113.2988       15.2     BAIYUN INTL
   CHI0000ZGOW  23.4000  116.6833        3.0         SHANTOU
   CHI0000ZGSZ  22.6393  113.8107        4.0      BAOAN INTL
   CHI0000ZJHK  19.9349  110.4590       22.9          MEILAN
   ...              ...       ...        ...             ...
   VMM00048839  20.1333  107.7167       56.0    BACH LONG VI
   VMM00048840  19.7500  105.7833        5.0       THANH HOA
   VMM00048846  18.3500  105.9000        3.0         HA TINH
   VMM00048848  17.4833  106.6000        8.0        DONG HOI
   VMW00041024  16.0439  108.1994       10.1      MARBLE MTN

   [130 rows x 4 columns]


.. note::

   For accumulation (e.g., precipitation) and statistical data (e.g., wind gusts), the
   time axis indicates the end of the measurement period. This is to be accounted for
   in the ``time_range`` argument.


.. seealso::

   :ref:`User guide \> Rain gauge data <guide-rain-gauge>`
      User guide page introducing rain gauge data loading functions looking into all
      available datasets -- including GHCNh --, and giving more details about filtering
      and searching in PySEASTERS.


Variables
---------

Below is a table gathering variable information from the documentation:

.. list-table::
   :header-rows: 1

   * - Code
     - Name
     - Default units
     - Details
   * - ``temperature``
     - Temperature
     - Tenths of degree Celsius
     - 2 meter (circa) Above Ground Level Air (dry bulb) Temperature
   * - ``dew_point_temperature``
     - Dew point temperature
     - Tenths of degree Celsius
     - None
   * - ``station_level_pressure``
     - Station level pressure
     - hPa
     - Pressure observed at a specific elevation (true barometric pressure of a location). It is the pressure exerted by the atmosphere at a point as a result of gravity acting upon the 'column' of air that lies directly above the point.
   * - ``sea_level_pressure``
     - Sea level pressure
     - hPa
     - Estimates the pressure that would exist at sea level at a point directly below the station using a temperature profile based on temperatures that actually exist at the station
   * - ``wind_direction``
     - Wind direction
     - degree
     - Wind Direction from true north using compass directions (e.g. 360 = true north, 180 = south, 270 = west, etc.). Note: A direction of '000' is given for calm winds.
   * - ``wind_speed``
     - Wind speed
     - m/s
     - None
   * - ``wind_gust``
     - Wind gust
     - m/s
     - Peak short duration (usually < 20 seconds) wind speed (meters per second) that exceeds the wind_speed average
   * - ``precipitation``
     - Total liquid precipitation
     - mm
     - Total liquid precipitation (rain or melted snow). Totals are nominally for the hour, but may include intermediate reports within the hour. Note: A 'T' in the measurement code column indicates a trace amount of precipitation.
   * - ``relative_humidity``
     - Relative humidity
     - percent
     - Depending on the source, relative humidity is either measured directly or calculated from air (dry bulb) temperature and dew point temperature
   * - ``wet_bulb_temperature``
     - Wet bulb temperature
     - Tenths of degree Celsius
     - Depending on the source, wet bulb temperature is either measured directly or calculated from air (dry bulb) temperature, dew point temperature, and station pressure
   * - ``snow_depth``
     - Snow depth
     - mm
     - Depth of snowpack on the ground
   * - ``visibility``
     - Visibility
     - km
     - Horizontal distance at which an object can be seen and identified
   * - ``altimeter``
     - Altimeter
     - mbar/hPa
     - The pressure 'reduced' to mean sea level using the temperature profile of the 'standard' atmosphere, which is representative of average conditions over the United States at 40 degrees north latitude
   * - ``pressure_3hr_change``
     - 3-hour pressure change
     - mbar/hPa
     - Change in atmospheric pressure measured at the beginning and end of a three hour period; accompanied by tendency code in measurement code field
   * - ``precipitation_x_hour`` with ``x`` being 3, 6, 9, 12, 15, 18, 21 or 24.
     - ``x``-hour total liquid precipitation
     - mm
     - ``x``-hour total liquid precipitation (rain or melted snow) accumulation from FM12/SYNOP reports. Note: A 'T' in the measurement code column indicates a trace amount of precipitation.
   * - ``remarks``
     - Hourly remarks
     - None
     - Raw surface observation data in the original format encoded into ICAO-standardized METAR (FM15) or FM12 (SYNOP), FM16 (SPECI), etc. format for global dissemination. Note: Further information on decoding these observations can be found in the Federal Meteorological Handbook (FMH) No. 1, Surface Weather Observations & Reports.


Station names and IDs
---------------------

.. _ghcnh-station-id:

Station IDs
~~~~~~~~~~~

Station IDs are eleven-character long, in the following form:

.. code:: console

   FFNIIIIIIII


e.g., ``GQW00041406``, where (the following is derived from GHCNh documentation):

* ``FF`` is a 2 character `FIPS 10-4 code <https://en.wikipedia.org/wiki/FIPS_10-4>`_
  indicating the territory (``GQ`` in the example, for "Guam").

  .. seealso::

     :doc:`pyseasters.COUNTRIES <../api/pyseasters.constants.countries>`
        PySEASTERS provides the ``COUNTRIES`` constant ``pandas`` DataFrame that relates
        country names with ISO and FIPS codes.


* ``N`` is a 1 character "network" code indicating how to interpret the following eight
  characters (``W`` in the example, indicating -- refering to the table below --
  that the last five characters will make the station's WBAN identification number).
  Below are the potential network code values with their meaning:

  .. list-table::
     :header-rows: 1

     * - Network code
       - Meaning
     * - A
       - Retired WMO Identifier used by the USAF 14th Weather Squadron
     * - U
       - Unspecified (station identified by up to eight alphanumeric characters)
     * - C
       - U.S. Cooperative Network identification number
         (last six characters of the GHCN ID)
     * - I
       - International Civil Aviation Organization (ICAO) identifier
     * - M
       - World Meteorological Organization ID (last five characters of the GHCN ID)
     * - N
       - Identification number used by a National Meteorological or Hydrological Center
         partner
     * - L
       - U.S. National Weather Service Location Identifier (NWSLI)
     * - W
       - WBAN identification number (last five characters of the GHCN ID)


* ``IIIIIIII`` is the actual 8 character ID of the station, to be read based on the
  associated network ``N`` (``00041406`` in the example, meaning that, since the network
  code was ``W``, the first three zeros are to be ignored, and the last five characters
  constitude the WBAN ID, i.e., ``41406``).


.. tip::

   Such station ID formatting can be used to filter stations when loading data,
   e.g., with PySEASTERS :doc:`load_1h_gauge_data() <../api/pyseasters.gauge_data_loaders>`
   function. For instance, Indonesian stations could be selected using the following
   ``filter_condition`` argument: ``filter_condition='station_id[:2] == "ID"'``.


.. _ghcnh-station-name:

Station names
~~~~~~~~~~~~~

Station names are formatted as follows:

.. code:: console

   <name> [US=<US state>, GSN=<GSN flag>, HCN=<HCN/CRN flag>, WMO=<WMO ID>]


where information between square brackets is not present for all stations. For instance,
the station with ``station_id='GQW00041406'`` has the following ``station_name``:

.. code:: console

   GUAM WFO [WMO=91212]


Below are explanations on the flags, derived from from GHCNh documentation:

* ``<US state>`` is the U.S. postal code for the state (for U.S. stations only).

* ``<GSN flag>`` is a flag that indicates whether the station is part of the GCOS
  Surface Network (GSN). The flag is assigned by cross-referencing
  the number in the WMO ID field with the official list of GSN
  stations. The flag equals ``GSN`` if the station is part of the network, and is blank
  otherwise.

* ``<HCN/CRN flag>`` is a flag that indicates whether the station is part of the U.S.
  Historical Climatology Network (HCN) or U.S. Climate Reference Network (CRN; also
  includes U.S. Regional Climate Network stations).
  The flag equals ``HCN`` if the former, ``CRN`` if the latter, and is blank otherwise.

* ``<WMO ID>`` is the World Meteorological Organization (WMO) number for the
  station. If the station has no WMO number (or one has not yet been matched to this
  station), then the field is blank.


.. tip::

   As for station IDs, station names can be used in the ``filter_condition`` argument
   of several PySEASTERS loading functions such as
   :doc:`load_1h_gauge_data() <../api/pyseasters.gauge_data_loaders>`. For example, stations
   with a WMO ID could be selected using ``filter_condition='"WMO=" in station_name'``.


How to cite?
------------

This is GHCNh **version 1.0.1**, **accessed May 12th, 2025**.
The documentation indicates to cite the dataset using Menne et al. (2023).


References
----------

.. bibliography::
   :list: bullet
   :filter: key % "GHCNh:"
