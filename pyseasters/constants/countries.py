"""
Provide the ``COUNTRIES`` constant.

``COUNTRIES`` is a pandas DataFrame containing ISO and FIPS codes associated with the
countries considered for the :ref:`extended Southeast Asian region <SEA>`.

The data is loaded from an ASCII file stored at
'pyseasters/constants/data/countries.txt'.
``COUNTRIES`` can be used as is:

>>> from pyseasters import COUNTRIES
>>> COUNTRIES[:5]
  Alpha-2 Alpha-3 FIPS Country name
0      AU     AUS   AS    Australia
1      BD     BGD   BG   Bangladesh
2      BT     BTN   BT       Bhutan
3      BN     BRN   BX       Brunei
4      KH     KHM   CB     Cambodia

...or to build other useful variable like mapping dictionaries, e.g.,
mapping FIPS codes to country names:

>>> FIPS_TO_COUNTRY = COUNTRIES.set_index("FIPS")["Country name"].to_dict()
>>> FIPS_TO_COUNTRY("ID")
'Indonesia'
"""

import importlib.resources

import pandas as pd

__all__ = ["COUNTRIES"]

COUNTRIES: pd.DataFrame

with (
    importlib.resources.files("pyseasters.constants.data")
    .joinpath("countries.txt")
    .open("r") as file
):
    COUNTRIES = pd.read_csv(file, delimiter="\t", header=0)
