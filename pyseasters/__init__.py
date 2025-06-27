"""
PySEASTERS includes:

#. A Python API aiming to manipulate the data stored internally.

#. :mod:`~pyseasters.data_curation`: The scripts that serve for
   :ref:`constructing the database <replicate>`.


Importing :mod:`pyseasters` does not import the :mod:`~pyseasters.data_curation`
subpackage by default, because it is not aimed at being used frequently, unlike the main
API. The subpackage is maintained for history, or in case data needs to be downloaded
and preprocessed again. If needed, one can access the :mod:`~pyseasters.data_curation`
subpackage by importing specifically :mod:`pyseasters.data_curation`.
"""

from ._version import __version__ as VERSION
from .constants import COUNTRIES, paths
from .gauge_data_loaders import (
    load_1h_gauge_data,
    load_1h_gauge_data_attrs,
    load_1h_gauge_data_concat,
    load_1h_gauge_data_noattrs,
    load_all_gauge_data,
    load_all_gauge_data_attrs,
    load_all_gauge_data_concat,
    load_all_gauge_data_noattrs,
    search_1h_gauge_data,
    search_all_gauge_data,
)
from .ghcnd import (
    get_ghcnd_metadata,
    get_ghcnd_station_list,
    load_ghcnd,
    load_ghcnd_attrs,
    load_ghcnd_concat,
    load_ghcnd_inventory,
    load_ghcnd_noattrs,
    load_ghcnd_single_var_station,
    load_ghcnd_single_var_station_attrs,
    load_ghcnd_single_var_station_noattrs,
    load_ghcnd_stations,
    search_ghcnd,
)
from .ghcnh import (
    get_ghcnh_station_list,
    load_ghcnh,
    load_ghcnh_attrs,
    load_ghcnh_concat,
    load_ghcnh_inventory,
    load_ghcnh_noattrs,
    load_ghcnh_single_var_station,
    load_ghcnh_single_var_station_attrs,
    load_ghcnh_single_var_station_noattrs,
    load_ghcnh_station_list,
    search_ghcnh,
)
from .gsdr import (
    get_gsdr_metadata,
    get_gsdr_station_list,
    load_gsdr,
    load_gsdr_concat,
    load_gsdr_inventory,
    load_gsdr_single_station,
    load_gsdr_stations,
    search_gsdr,
)
from .utils import check_dataframe_unit, convert_dataframe_unit
