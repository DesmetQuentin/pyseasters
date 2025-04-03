"""
This module defines the `COUNTRIES` constant.
COUNTRIES is a pandas DataFrame containing ISO and FIPS codes associated with the
countries considered for the extended Southeast Asian region.
The data is loaded from an ASCII file stored at `pyseasters/data/countries.txt`.
The COUNTRIES variable can serve as is, or it can be used to build mapping
dictionaries, e.g., mapping FIPS codes to country names:

>>> FIPS_TO_COUNTRY = COUNTRIES.set_index("FIPS")["Country name"].to_dict()
>>> FIPS_TO_COUNTRY("ID")
'Indonesia'
"""

import importlib.resources

import pandas as pd

__all__ = ["COUNTRIES"]

COUNTRIES: pd.DataFrame

with importlib.resources.files("pyseasters.constants.data").joinpath(
    "countries.txt"
).open("r") as file:
    COUNTRIES = pd.read_csv(file, delimiter="\t", header=0)
