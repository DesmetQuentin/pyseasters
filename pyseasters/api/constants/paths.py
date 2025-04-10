"""
This module provides the ``paths`` constant -- and defines its dataclass, ``PathConfig``.

``paths`` aims at providing the paths to the external data employed in this package.
It adapts to the session's machine/network based on the information provided in
'paths.yaml', a personal configuration file that must be placed in
'pyseasters/constants/data'.
"""

import importlib.resources
import logging
import socket
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

import yaml

__all__ = ["paths"]

log = logging.getLogger(__name__)


def _parse_pathsyaml():
    """
    Return two dictionaries mapping machines and networks to root directories,
    issued by reading and parsing 'paths.yaml'.
    """

    machine_to_root, network_to_root = {}, {}
    try:
        with importlib.resources.files("pyseasters.api.constants.data").joinpath(
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
                                    "paths.yaml inconsistent: several paths found for "
                                    + f"machine '{target}'"
                                )
                            machine_to_root[target] = Path(k)
                        elif ttype == "network":
                            if target in network_to_root.keys():
                                raise RuntimeError(
                                    "paths.yaml inconsistent: several paths found for "
                                    + f"network '{target}'"
                                )
                            network_to_root[target] = Path(k)
                else:
                    [ttype, target] = v.split(":")
                    if ttype == "machine":
                        if target in machine_to_root.keys():
                            raise RuntimeError(
                                "paths.yaml inconsistent: several paths found for "
                                + f"machine '{target}'"
                            )
                        machine_to_root[target] = Path(k)
                    if ttype == "network":
                        if target in network_to_root.keys():
                            raise RuntimeError(
                                "paths.yaml inconsistent: several paths found for "
                                + f"network '{target}'"
                            )
                        network_to_root[target] = Path(k)
    except FileNotFoundError:
        log.warning("No path data found. Have you forgotten to download paths.yaml?")

    return machine_to_root, network_to_root


# Build dictionaries mapping machines and networks to root directories
_MACHINE_TO_ROOT, _NETWORK_TO_ROOT = _parse_pathsyaml()

# Detect current machine and network
_CURRENT_MACHINE = socket.gethostname()
_CURRENT_NETWORK = subprocess.run(
    f'nslookup {_CURRENT_MACHINE} | grep Name | cut -d "." -f 2-',
    shell=True,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()


@dataclass
class PathConfig:
    """
    Class to handle data access based on the session's machine/network
    (takes no argument).

    On instantiation, a ``PathConfig`` object attemps to assign its ``root`` attribute with
    the data root path associated with the current session, based on the predefined
    mappings parsed from the 'paths.yaml' file located in 'pyseasters/constants/data/'.
    If the current session's machine or network does not match any predefined data root
    path, then the ``PathConfig`` object is considered not operational and a warning is
    emitted to notify the user that no data will be accessible unless configured
    manually (see the ``manual_config()`` method to override or define the data root path
    explicitly).

    Once operational, a ``PathConfig`` object provides plenty of paths to various data
    under the data root directory, accessible via methods.
    """

    root: Path = field(init=False)
    _dummy_root: Path = Path("/dummy/root")

    def __post_init__(self) -> None:
        global _MACHINE_TO_ROOT, _NETWORK_TO_ROOT, _CURRENT_MACHINE, _CURRENT_NETWORK

        if _CURRENT_MACHINE in _MACHINE_TO_ROOT.keys():
            self.root = _MACHINE_TO_ROOT[_CURRENT_MACHINE]
            if _CURRENT_NETWORK in _NETWORK_TO_ROOT.keys():
                log.info(
                    "Found configuration matches for both this machine and network:"
                )
                log.info(
                    f"With the machine '{_CURRENT_MACHINE}': "
                    f"{_MACHINE_TO_ROOT[_CURRENT_MACHINE]}"
                )
                log.info(
                    f"With the network '{_CURRENT_NETWORK}': "
                    + f"{_NETWORK_TO_ROOT[_CURRENT_NETWORK]}"
                )
                log.info("Prioritizing machine configuration.")

        elif _CURRENT_NETWORK in _NETWORK_TO_ROOT.keys():
            self.root = _NETWORK_TO_ROOT[_CURRENT_NETWORK]

        else:
            self.root = self._dummy_root

            log.warning(
                f"No default setting set for this machine ('{_CURRENT_MACHINE}') "
                + f"or network ('{_CURRENT_NETWORK}')."
            )
            log.warning("Attempting to access data will yield an error.")
            log.warning(
                "Configure manually using `paths.manual_config('/path/to/data/')`"
            )

    def is_operational(self) -> bool:
        """Return a boolean to inform whether paths are accessible or not."""
        return self.root != self._dummy_root

    def manual_config(self, root: Union[str, Path]) -> None:
        """Manually set up the data root directory for this session.

        Args:
            root: The path to use as root for data access.

        Raises:
            FileNotFoundError: If the provided path does not exist.
        """

        root_path = Path(root).resolve()
        if not root_path.exists():
            raise FileNotFoundError(f"Provided directory '{root_path}' does not exist.")

        if _CURRENT_MACHINE in _MACHINE_TO_ROOT.keys():
            log.warning("Overriding existing settings for this machine.")
            log.info(
                f"Previous settings: {_CURRENT_MACHINE} "
                + f"{_MACHINE_TO_ROOT[_CURRENT_MACHINE]}"
            )
            log.info(f"     New settings: {_CURRENT_MACHINE} {root_path}")

        elif _CURRENT_NETWORK in _NETWORK_TO_ROOT.keys():
            log.warning("Overriding existing settings for this network.")
            log.info(
                f"Previous settings: {_CURRENT_NETWORK} "
                + f"{_NETWORK_TO_ROOT[_CURRENT_NETWORK]}"
            )
            log.info(f"     New settings: {_CURRENT_NETWORK} {root_path}")

        self.root = root_path

    def ghcnd(self) -> Path:
        """Return GHCNd data root directory."""
        return self.root / "GHCNd"

    def ghcnd_stations(self, ext: str = "parquet") -> Path:
        """Return path to the GHCNd station metadata file."""
        return self.ghcnd() / "metadata" / ("ghcnd-stations." + ext)

    def ghcnd_inventory(self, ext: str = "parquet") -> Path:
        """Return path to the GHCNd inventory file."""
        return self.ghcnd() / "metadata" / ("ghcnd-inventory." + ext)

    def ghcnd_file(self, station_id: str, ext: str = "parquet") -> Path:
        """Return path to the GHCNd file associate with the station ``station_id``."""
        return self.ghcnd() / "data" / (station_id + "." + ext)


paths = PathConfig()
