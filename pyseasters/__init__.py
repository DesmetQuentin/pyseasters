"""
PySEASTERS includes:

#. A Python API aiming to manipulate the data stored internally.
#. ``pyseasters.data_curation``: The scripts that served for data curation.


Importing ``pyseasters`` does not import the ``data_curation`` subpackage by default,
because it is not aimed at being used frequently, unlike the main API. The subpackage
is maintained for history, or in case data needs to be downloaded and preprocessed
again. If needed, one can access the ``data_curation`` subpackage by importing
specifically ``pyseasters.data_curation``.
"""

from ._version import __version__ as VERSION
from .constants import COUNTRIES, paths
from .ghcnd import (
    get_ghcnd_metadata,
    get_ghcnd_station_list,
    load_ghcnd_data,
    load_ghcnd_inventory,
    load_ghcnd_stations,
)
from .utils import check_dataframe_unit, convert_dataframe_unit
