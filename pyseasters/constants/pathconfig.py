"""
`paths`This module defines the paths variable with its dataclass, PathConfig.
paths aims at providing the paths to the external data employed in this package.
It adapts to the machine/network where the package is imported based on the information
provided in paths.yaml, a personal configuration file that must be placed in
pyseasters/constants/data.
TODO: details
"""

import importlib.resources
import logging
import socket
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import yaml

from pyseasters.utils.typing import PathType

__all__ = ["paths"]

log = logging.getLogger(__name__)

# Populate MACHINE_TO_PATH and NETWORK_TO_PATH from pyseasters/constants/data/paths.yaml
MACHINE_TO_PATH: Dict = {}
NETWORK_TO_PATH: Dict = {}
try:
    with importlib.resources.files("pyseasters.constants.data").joinpath(
        "paths.yaml"
    ).open("r") as file:
        data = yaml.safe_load(file)
        for k, v in data.items():
            if isinstance(v, list):
                for value in v:
                    [ttype, target] = value.split(":")
                    if ttype == "machine":
                        if target in MACHINE_TO_PATH.keys():
                            raise RuntimeError(
                                f"paths.yaml inconsistent: several paths found for machine '{target}'"
                            )
                        MACHINE_TO_PATH[target] = Path(k)
                    elif ttype == "network":
                        if target in NETWORK_TO_PATH.keys():
                            raise RuntimeError(
                                f"paths.yaml inconsistent: several paths found for network '{target}'"
                            )
                        NETWORK_TO_PATH[target] = Path(k)
            else:
                [ttype, target] = v.split(":")
                if ttype == "machine":
                    if target in MACHINE_TO_PATH.keys():
                        raise RuntimeError(
                            f"paths.yaml inconsistent: several paths found for machine '{target}'"
                        )
                    MACHINE_TO_PATH[target] = Path(k)
                if ttype == "network":
                    if target in NETWORK_TO_PATH.keys():
                        raise RuntimeError(
                            f"paths.yaml inconsistent: several paths found for network '{target}'"
                        )
                    NETWORK_TO_PATH[target] = Path(k)
        del data
except FileNotFoundError:
    log.warning("No path data found. Have you forgotten to download paths.yaml?")


# Detect current machine and network
CURRENT_MACHINE = socket.gethostname()
CURRENT_NETWORK = subprocess.run(
    f'nslookup {CURRENT_MACHINE} | grep Name | cut -d "." -f 2-',
    shell=True,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()


# Configure DATA_ROOT
DATA_ROOT: Path
if CURRENT_MACHINE in MACHINE_TO_PATH.keys():
    self.root = MACHINE_TO_PATH[CURRENT_MACHINE]
    if CURRENT_NETWORK in NETWORK_TO_PATH.keys():
        log.info(
            "Found configuration matches for both this machine and network:"
        )
        log.info(
            f"With the machine '{CURRENT_MACHINE}': {MACHINE_TO_PATH[CURRENT_MACHINE]}"
        )
        log.info(
            f"With the network '{CURRENT_NETWORK}': {NETWORK_TO_PATH[CURRENT_NETWORK]}"
        )
        log.info("Prioritizing machine configuration.")

elif CURRENT_NETWORK in NETWORK_TO_PATH.keys():
    self.root = NETWORK_TO_PATH[CURRENT_NETWORK]

else:
    log.warning(
        f"No default setting set for this machine ('{CURRENT_MACHINE}') or network ('{CURRENT_NETWORK}')."
    )
    log.warning(
        "Consider setting a custom data directory using `config.initialize(custom_path='/custom/path')`."
    )
    raise RuntimeError(
        f"No default settings set for this machine ('{CURRENT_MACHINE}') or network ('{CURRENT_NETWORK}')."
    )

@dataclass
class PathConfig:
    is_set: bool
    root: Path
    ghcnd: Path
    ghcnd_stations: Path
    ghcnd_inventory: Path

    def initialize(
        self, network_priority: bool = False, custom_path: Optional[PathType] = None
    ):
        """Initialize data directories based on the current machine or a provided custom_path."""



        else:
            path = Path(custom_path).resolve()
            if not path.exists():
                raise FileNotFoundError(
                    f"Custom data directory '{path}' does not exist."
                )

            if network_priority:
                if CURRENT_NETWORK in NETWORK_TO_PATH.keys():
                    log.warning(
                        "Custom data directory overrides existing settings for this network."
                    )
                    log.info(
                        f"Previous settings: {CURRENT_NETWORK} {NETWORK_TO_PATH[CURRENT_NETWORK]}"
                    )
                    log.info(f"     New settings: {CURRENT_NETWORK} {custom_path}")

            else:
                if CURRENT_MACHINE in MACHINE_TO_PATH.keys():
                    log.warning(
                        "Custom data directory overrides existing settings for this machine."
                    )
                    log.info(
                        f"Previous settings: {CURRENT_MACHINE} {MACHINE_TO_PATH[CURRENT_MACHINE]}"
                    )
                    log.info(f"     New settings: {CURRENT_MACHINE} {custom_path}")

            self.root = path

        self.ghcnd = self.root / "GHCNd"
        self.ghcnd_stations = self.ghcnd / "ghcnd-stations.txt"
        self.ghcnd_inventory = self.ghcnd / "ghcnd-inventory.txt"

    def is_initialized(self):
        return self.root is not None

    def ghcnd_file(self, station_id: str) -> str:
        return self.ghcnd / f"{station_id}.csv"


paths = PathConfig()
