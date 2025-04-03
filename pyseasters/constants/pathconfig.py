"""
This module defines the paths variable with its dataclass, PathConfig.
paths aims at providing the paths to the external data employed in this package.
It adapts to the machine/network where the package is imported based on the information
provided in paths.yaml, a personal configuration file that must be placed in
pyseasters/constants/data.
TODO: details
"""

import importlib.resources
import logging
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import yaml

__all__ = ["paths"]

log = logging.getLogger(__name__)

# Machine-specific root directory mapping
MACHINE_TO_PATH = {}
try:
    with importlib.resources.files("pyseasters.constants.data").joinpath(
        "paths.yaml"
    ).open("r") as file:
        data = yaml.safe_load(file)
        for k, v in data["path to machine"].items():
            if isinstance(v, list):
                already_recorded = np.array(
                    [machine in MACHINE_TO_PATH.keys() for machine in v]
                )
                if np.any(already_recorded):
                    raise RuntimeError(
                        f"paths.yaml inconsistent: several paths found for machine '{v[np.where(already_recorded)[0][0]]}'"
                    )
                MACHINE_TO_PATH.update({machine: Path(k) for machine in v})
            else:
                if v in MACHINE_TO_PATH.keys():
                    raise RuntimeError(
                        f"paths.yaml inconsistent: several paths found for machine '{v}'"
                    )
                MACHINE_TO_PATH[v] = Path(k)
        del already_recorded
        del data
except FileNotFoundError:
    log.warning("No path data found. Have you forgotten to download paths.yaml?")


# Detect current machine
CURRENT_MACHINE = socket.gethostname()
# TODO: CURRENT_NETWORK = subprocess.run(f'nslookup {CURRENT_MACHINE} | grep Name | cut -d "." -f 2-', shell=True, check=True)


@dataclass
class PathConfig:
    root: str = None
    ghcnd: str = None
    ghcnd_stations: str = None
    ghcnd_inventory: str = None

    def initialize(self, custom_path: Optional[str] = None):
        """Initialize data directories based on the current machine or a provided custom_path."""
        if custom_path is None:
            if CURRENT_MACHINE in MACHINE_TO_PATH.keys():
                self.root = MACHINE_TO_PATH[CURRENT_MACHINE]

            else:
                log.error(
                    f"No default setting set for this machine ('{CURRENT_MACHINE}')."
                )
                log.error(
                    "Consider setting a custom data directory using 'config.initialize(custom_path)'."
                )
                raise RuntimeError(
                    f"No default settings set for this machine ('{CURRENT_MACHINE}')."
                )

        else:
            custom_path = Path(custom_path).resolve()
            if not custom_path.exists():
                raise FileNotFoundError(
                    f"Custom data directory '{custom_path}' does not exist."
                )

            if CURRENT_MACHINE in MACHINE_TO_PATH.keys():
                log.warning(
                    "Custom data directory overrides existing settings for this machine."
                )
                log.info(
                    f"Previous settings: {CURRENT_MACHINE} {MACHINE_TO_PATH[CURRENT_MACHINE]}"
                )
                log.info(f"     New settings: {CURRENT_MACHINE} {custom_path}")

            self.root = custom_path

        self.ghcnd = self.root / "GHCNd"
        self.ghcnd_stations = self.ghcnd / "ghcnd-stations.txt"
        self.ghcnd_inventory = self.ghcnd / "ghcnd-inventory.txt"

    def is_initialized(self):
        return self.root is not None

    def ghcnd_file(self, station_id: str) -> str:
        return self.ghcnd / f"{station_id}.csv"


paths = PathConfig()
