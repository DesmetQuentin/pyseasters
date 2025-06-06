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

     :doc:`pyseasters.COUNTRIES <../api/pyseasters.constants.countries>`:
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
   e.g., with PySEASTERS :doc:`load_gauge_data() <../api/pyseasters.gauge_data_loader>`
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
   :doc:`load_gauge_data() <../api/pyseasters.gauge_data_loader>`. For example, stations
   with a WMO ID could be selected using ``filter_condition='"WMO=" in station_name'``.


How to cite?
------------

This data provider do not provide any dataset-type citation.
We suggest simply including the references below.


References
----------

.. bibliography::
   :list: bullet
   :filter: key % "GHCNh:"
