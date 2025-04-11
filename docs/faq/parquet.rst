About the ``parquet`` format
============================


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

.. code:: python

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
