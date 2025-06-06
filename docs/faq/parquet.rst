.. _parquet:

Why the ``parquet`` format?
===========================

While using the API, GHCNd data is loaded from a database stored using the ``parquet``
format. Conversion from the original ``csv`` format to ``parquet`` is
conducted during :ref:`preprocessing <preprocess>`. This is justified by
**enormous gains** in terms of **storage** and **loading speed**,
as shortly demonstrated below.


3 times lighter
---------------

The same data takes about three times less space when stored in a parquet file:

.. code:: console

   $ du -sh ASN00009135.csv ASN00009135.parquet
   280K    ASN00009135.csv
   88K     ASN00009135.parquet
   $ du -sh ASN00010150.csv ASN00010150.parquet
   1.1M    ASN00010150.csv
   416K    ASN00010150.parquet


5 times faster
--------------

Loading the same data from a parquet file is between five and six times faster.

.. code:: pycon

   >>> # Import
   >>> import pandas as pd
   >>> from timeit import timeit  # Iterate a Python query -> Return the computing time
   >>>
   >>> # Time for reading a whole data file (x 1000)
   >>> timeit("pd.read_csv('ASN00010150.csv')", number=1000)
   65.43379709497094
   >>> timeit("pd.read_parquet('ASN00010150.parquet')", number=1000)
   13.217015915550292
   >>>
   >>> # Time for reading a single column (x 1000)
   >>> timeit("pd.read_csv('ASN00010150.csv', usecols=['PRCP'])", number=1000)
   30.48204466793686
   >>> timeit("pd.read_parquet('ASN00010150.parquet', columns=['PRCP'])", number=1000)
   5.669616661034524


.. note::

   For the case of :ref:`GSDR <gsdr>` data, which are originally stored in "efficient"
   ASCII files (only precipitation values are stored, and the time index is to be
   inferred from the starting date and timestep), while ``parquet`` format files obtained
   after preprocessing can be up to **two times heavier**, they are **between 10 and 60
   times faster to read**, when taking into account the time needed to build the time
   index. Since the entire GSDR dataset makes only about 5 Gb (in ``parquet``), the
   higher reading speed is prioritized.
