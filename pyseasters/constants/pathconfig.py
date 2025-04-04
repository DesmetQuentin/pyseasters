"""
This module provides the `paths` constant -- and defines its dataclass, `PathConfig`.

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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Dict

import yaml

__all__ = ["paths"]

log = logging.getLogger(__name__)


def _parse_pathsyaml():
    """
    Return two dictionaries mapping machines and networks to root directories,
    issued by reading and parsing paths.yaml.
    """

    machine_to_root, network_to_root = {}, {}
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
                            if target in machine_to_root.keys():
                                raise RuntimeError(
                                    f"paths.yaml inconsistent: several paths found for machine '{target}'"
                                )
                            machine_to_root[target] = Path(k)
                        elif ttype == "network":
                            if target in network_to_root.keys():
                                raise RuntimeError(
                                    f"paths.yaml inconsistent: several paths found for network '{target}'"
                                )
                            network_to_root[target] = Path(k)
                else:
                    [ttype, target] = v.split(":")
                    if ttype == "machine":
                        if target in machine_to_root.keys():
                            raise RuntimeError(
                                f"paths.yaml inconsistent: several paths found for machine '{target}'"
                            )
                        machine_to_root[target] = Path(k)
                    if ttype == "network":
                        if target in network_to_root.keys():
                            raise RuntimeError(
                                f"paths.yaml inconsistent: several paths found for network '{target}'"
                            )
                        network_to_root[target] = Path(k)
    except FileNotFoundError:
        log.warning("No path data found. Have you forgotten to download paths.yaml?")

    return machine_to_root, network_to_root


# Build dictionaries mapping machines and networks to root directories
MACHINE_TO_ROOT: Dict[str, Path]
NETWORK_TO_ROOT: Dict[str, Path]
MACHINE_TO_ROOT, NETWORK_TO_ROOT = _parse_pathsyaml()

# Detect current machine and network
CURRENT_MACHINE = socket.gethostname()
CURRENT_NETWORK = subprocess.run(
    f'nslookup {CURRENT_MACHINE} | grep Name | cut -d "." -f 2-',
    shell=True,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()


@dataclass
class PathConfig:
    root: Path = field(init=False)
    _dummy_root: Path = Path("/dummy/root")

    def __post_init__(self) -> None:
        global MACHINE_TO_ROOT, NETWORK_TO_ROOT, CURRENT_MACHINE, CURRENT_NETWORK

        if CURRENT_MACHINE in MACHINE_TO_ROOT.keys():
            self.root = MACHINE_TO_ROOT[CURRENT_MACHINE]
            if CURRENT_NETWORK in NETWORK_TO_ROOT.keys():
                log.info(
                    "Found configuration matches for both this machine and network:"
                )
                log.info(
                    f"With the machine '{CURRENT_MACHINE}': {MACHINE_TO_ROOT[CURRENT_MACHINE]}"
                )
                log.info(
                    f"With the network '{CURRENT_NETWORK}': {NETWORK_TO_ROOT[CURRENT_NETWORK]}"
                )
                log.info("Prioritizing machine configuration.")

        elif CURRENT_NETWORK in NETWORK_TO_ROOT.keys():
            self.root = NETWORK_TO_ROOT[CURRENT_NETWORK]

        else:
            self.root = self._dummy_root

            log.warning(
                f"No default setting set for this machine ('{CURRENT_MACHINE}') or network ('{CURRENT_NETWORK}')."
            )
            log.warning("Attempting to access data will yield an error.")
            log.warning(
                "Configure manually using `pyseasters.constants.pathconfig.paths.manual_config('/path/to/data/')"
            )

    def is_operational(self) -> bool:
        """Return a boolean to inform whether paths are accessible or not."""
        return self.root != self._dummy_root

    def manual_config(self, root: Union[str, Path]) -> None:
        """Manually set up the root directory for this session."""

        root_path = Path(root).resolve()
        if not root_path.exists():
            raise FileNotFoundError(f"Provided directory '{root_path}' does not exist.")

        if CURRENT_MACHINE in MACHINE_TO_ROOT.keys():
            log.warning("Overriding existing settings for this machine.")
            log.info(
                f"Previous settings: {CURRENT_MACHINE} {MACHINE_TO_ROOT[CURRENT_MACHINE]}"
            )
            log.info(f"     New settings: {CURRENT_MACHINE} {root_path}")

        elif CURRENT_NETWORK in NETWORK_TO_ROOT.keys():
            log.warning("Overriding existing settings for this network.")
            log.info(
                f"Previous settings: {CURRENT_NETWORK} {NETWORK_TO_ROOT[CURRENT_NETWORK]}"
            )
            log.info(f"     New settings: {CURRENT_NETWORK} {root_path}")

        self.root = root_path

    def ghcnd(self) -> Path:
        """Return GHCNd data root directory."""
        return self.root / "GHCNd"

    def ghcnd_stations(self) -> Path:
        """Return path to the GHCNd station metadata file."""
        return self.ghcnd() / "ghcnd-stations.txt"

    def ghcnd_inventory(self) -> Path:
        """Return path to the GHCNd inventory file."""
        return self.ghcnd() / "ghcnd-inventory.txt"

    def ghcnd_file(self, station_id: str) -> Path:
        """Return path to the GHCNd file associate with the station `station_id`."""
        return self.ghcnd() / f"{station_id}.csv"


paths = PathConfig()
