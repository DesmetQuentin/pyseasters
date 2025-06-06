.. _ghcnd:

Global Historical Climatology Network daily (GHCNd)
===================================================

Description
-----------

.. epigraph::

   The Global Historical Climatology Network daily (GHCNd) is an integrated database of
   daily climate summaries from land surface stations across the globe. GHCNd is made up
   of daily climate records from numerous sources that have been integrated and
   subjected to a common suite of quality assurance reviews.

   GHCNd contains records from more than 100,000 stations in 180 countries and
   territories. NCEI provides numerous daily variables, including maximum and minimum
   temperature, total daily precipitation, snowfall, and snow depth.
   About half the stations only report precipitation. Both record length and period of
   record vary by station and cover intervals ranging from less than a year to more than
   175 years.

   **Source:** `NOAA NCEI on GHCNd <https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily>`_
   (last accessed 2025-04-30)


.. important::

   Based in the USA, this dataset may become subject to download restrictions in the
   future. However, the data already downloaded into the SEASTERS database should remain
   unaffected.


Station names and IDs
---------------------

.. _ghcnd-station-id:

``station_id``
~~~~~~~~~~~~~~

Station IDs are eleven-character long, in the following form:

.. code:: console

   FFNIIIIIIII


e.g., ``ASM00094299``, where (the following is derived from GHCNd documentation):

* ``FF`` is a 2 character `FIPS 10-4 code <https://en.wikipedia.org/wiki/FIPS_10-4>`_
  indicating the territory (``AS`` in the example, for "Australia").

  .. seealso::

     :doc:`pyseasters.COUNTRIES <../api/pyseasters.constants.countries>`: PySEASTERS
     provides the ``COUNTRIES`` constant ``pandas`` DataFrame that relates country names
     with ISO and FIPS codes.


* ``N`` is a 1 character "network" code indicating how to interpret the following eight
  characters (``M`` in the example, indicating -- refering to the table below --
  that the last five characters will make the station's WMO ID).
  Below are the potential network code values with their meaning:

  .. list-table::
     :header-rows: 1

     * - Network code
       - Meaning
     * - 0
       - Unspecified (station identified by up to eight 
         alphanumeric characters)
     * - 1
       - Community Collaborative Rain, Hail,and Snow (CoCoRaHS)
         based identification number.  To ensure consistency with
         with GHCN Daily, all numbers in the original CoCoRaHS IDs
         have been left-filled to make them all four digits long. 
         In addition, the characters ``-`` and ``_`` have been removed 
         to ensure that the IDs do not exceed 11 characters when 
         preceded by ``US1``. For example, the CoCoRaHS ID 
         ``AZ-MR-156`` becomes ``US1AZMR0156`` in GHCN-Daily
     * - C
       - U.S. Cooperative Network identification number (last six 
         characters of the GHCN-Daily ID)
     * - E
       - Identification number used in the ECA&D non-blended
         dataset
     * - M
       - World Meteorological Organization ID (last five
         characters of the GHCN-Daily ID)
     * - N
       - Identification number used in data supplied by a 
         National Meteorological or Hydrological Center
     * - P
       - "Pre-Coop" (an internal identifier assigned by NCEI for station
         records collected prior to the establishment of the U.S. Weather
         Bureau and their management of the U.S. Cooperative (Coop) 
         Observer Program
     * - R
       - U.S. Interagency Remote Automatic Weather Station (RAWS)
         identifier
     * - S
       - U.S. Natural Resources Conservation Service SNOwpack
         TELemtry (SNOTEL) station identifier
     * - W
       - WBAN identification number (last five characters of the 
         GHCN-Daily ID)


* ``IIIIIIII`` is the actual 8 character ID of the station, to be read based on the
  associated network ``N`` (``00094299`` in the example, meaning that, since the network
  code was ``W``, the first three zeros are to be ignored, and the last five characters
  constitude the WMO ID, i.e., ``94299``).


.. tip::

   Such station ID formatting can be used to filter stations when loading data,
   e.g., with PySEASTERS :doc:`load_gauge_data() <../api/pyseasters.gauge_data_loader>`
   function. For instance, Indonesian stations could be selected using the following
   ``filter_condition`` argument: ``filter_condition='station_id[:2] == "ID"'``.


.. _ghcnd-station-name:

``station_name``
~~~~~~~~~~~~~~~~

Station names are formatted as follows:

.. code:: console

   <name> [US=<US state>, GSN=<GSN flag>, HCN=<HCN/CRN flag>, WMO=<WMO ID>]


where information between square brackets is not present for all stations. For instance,
the station with ``station_id='ASM00094299'`` has the following ``station_name``:

.. code:: console

   WILLIS ISLAND [GSN=GSN, WMO=94299]


Below are explanations on the flags, derived from from GHCNd documentation:

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
   :filter: key % "GHCNd:"
