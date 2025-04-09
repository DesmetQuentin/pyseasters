"""
PySEASTERS formally consists of two subpackages, namely ``api`` and ``cli``.
Importing ``pyseasters`` essentially imports the main API, i.e., ``pyseasters.api``.
The CLI base functions may be accessed by importing ``pyseasters.cli`` directly
(importing ``pyseasters`` will ignore those CLI-purpose features).
"""

from ._version import __version__ as VERSION
from .api import (
    COUNTRIES,
    get_ghcnd_metadata,
    get_ghcnd_station_list,
    load_gauge_data,
    load_ghcnd_data,
    load_ghcnd_inventory,
    load_ghcnd_stations,
    paths,
)
