"""
This module provides the ``paths`` constant
-- and defines its dataclass, ``PathConfig``.

``paths`` aims at providing the paths to the external data employed in this package.
TODO It adapts to the session's machine/network based on the information provided in
'paths.yaml', a personal configuration file that must be placed in
'pyseasters/constants/data'.
"""

import importlib.resources
import logging
import socket
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

import yaml

from pyseasters.utils._typing import PathLike

__all__ = ["paths"]

log = logging.getLogger(__name__)


def _parse_pathsyaml():
    """
    Return two dictionaries mapping machines and networks to root directories,
    issued by reading and parsing 'paths.yaml'.
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


@dataclass
class PathConfig:
    """Class to handle data access.

    TODO
    On instantiation, a ``PathConfig`` object attemps to assign its ``root`` attribute
    with the data root path associated with the current session, based on the predefined
    mappings parsed from the 'paths.yaml' file located in 'pyseasters/constants/data/'.
    If the current session's machine or network does not match any predefined data root
    path, then the ``PathConfig`` object is considered not operational and a warning is
    emitted to notify the user that no data will be accessible unless configured
    manually (see the ``manual_config()`` method to override or define the data root
    path explicitly).

    Once operational, a ``PathConfig`` object provides plenty of paths to various data
    under the data root directory, accessible via methods.
    """

    init_mode: str = "auto"
    root: Path = field(init=False)
    _current_machine: str = field(init=False)
    _current_network: str = field(init=False)
    _machine_to_root: Dict[str, Path] = field(init=False)
    _network_to_root: Dict[str, Path] = field(init=False)
    _dummy_root: Path = Path("/dummy/root")

    def __post_init__(self) -> None:
        if self.init_mode == "auto":
            log.info("Initialize in auto mode (requires paths.yaml).")

            # Build dictionaries mapping machines and networks to root directories
            self._machine_to_root, self._network_to_root = _parse_pathsyaml()

            # Detect current machine and network
            self._current_machine = socket.gethostname()
            self._current_network = subprocess.run(
                f'nslookup {self._current_machine} | grep Name | cut -d "." -f 2-',
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

            # Assign self.root
            if self._current_machine in self._machine_to_root.keys():
                root_path = self._machine_to_root[self._current_machine]
                if not root_path.exists():
                    self.root = self._dummy_root
                    log.warning(
                        f"Matching path in paths.yaml ('{root_path}') does not exist."
                    )
                    log.warning("Attempting to access data will yield an error.")
                    log.warning(
                        "Configure manually using `paths.manual_config('/path/to/data/')`"
                    )
                else:
                    self.root = root_path
                    if self._current_network in self._network_to_root.keys():
                        log.info(
                            "Found configuration matches for both machine and network:"
                        )
                        log.info(
                            f"With machine '{self._current_machine}': "
                            f"{self._machine_to_root[self._current_machine]}"
                        )
                        log.info(
                            f"With network '{self._current_network}': "
                            + f"{self._network_to_root[self._current_network]}"
                        )
                        log.info("Prioritizing machine configuration.")

            elif self._current_network in self._network_to_root.keys():
                root_path = self._network_to_root[self._current_network]
                if not root_path.exists():
                    self.root = self._dummy_root
                    log.warning(
                        f"Matching path in paths.yaml ('{root_path}') does not exist."
                    )
                    log.warning("Attempting to access data will yield an error.")
                    log.warning(
                        "Configure manually using `paths.manual_config('/path/to/data/')`"
                    )
                else:
                    self.root = root_path

            else:
                self.root = self._dummy_root
                log.warning(
                    f"No default setting set for machine '{self._current_machine}' "
                    + f"or network '{self._current_network}'."
                )
                log.warning("Attempting to access data will yield an error.")
                log.warning(
                    "Configure manually using `paths.manual_config('/path/to/data/')`"
                )

        elif self.init_mode == "preconfig":
            log.info("Initialize in preconfig mode (req. running configure_api first).")

            try:
                with importlib.resources.files("pyseasters.constants.data").joinpath(
                    "path.txt"
                ).open("r") as file:
                    root_path = Path(file.read())
                    if not root_path.exists():
                        self.root = self._dummy_root
                        log.warning(f"Path in path.txt ('{root_path}') does not exist.")
                        log.warning("Attempting to access data will yield an error.")
                        log.warning(
                            "Configure manually using `paths.manual_config('/path/to/data/')`"
                        )
                    else:
                        self.root = root_path

            except FileNotFoundError:
                self.root = self._dummy_root
                log.warning("No path found.")
                log.warning(
                    "Have you forgotten to run configure_api from the data directory?"
                )
                log.warning("Attempting to access data will yield an error.")
                log.warning(
                    "Configure manually using `paths.manual_config('/path/to/data/')`"
                )

        elif self.init_mode.split(":")[0] == "manual":
            log.info("Initialize in manual mode.")
            self.manual_config(self.init_mode.split(":")[1])

        else:
            raise ValueError(
                f"Provided `init_mode` ('{self.init_mode}') is invalid. Valid modes are "
                "'auto', 'preconfig' and 'manual:/data/root/path'."
            )

    def is_operational(self) -> bool:
        """Return a boolean to inform whether paths are accessible or not."""
        return self.root != self._dummy_root and self.root.exists()

    def manual_config(self, root: PathLike) -> None:
        """Manually set up the data root directory for this session.

        Parameters
        ----------
        root
            The path to use as root for data access.

        Raises
        ------
        FileNotFoundError
            If the provided path does not exist.
        """

        root_path = Path(root).resolve()
        if not root_path.exists():
            raise FileNotFoundError(f"Provided directory '{root_path}' does not exist.")

        if self.init_mode == "auto":
            if self._current_machine in self._machine_to_root.keys():
                log.warning("Overriding existing settings for this machine.")
                log.info(
                    f"Previous settings: {self._current_machine} "
                    + f"{self._machine_to_root[self._current_machine]}"
                )
                log.info(f"     New settings: {self._current_machine} {root_path}")

            elif self._current_network in self._network_to_root.keys():
                log.warning("Overriding existing settings for this network.")
                log.info(
                    f"Previous settings: {self._current_network} "
                    + f"{self._network_to_root[self._current_network]}"
                )
                log.info(f"     New settings: {self._current_network} {root_path}")

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


paths = PathConfig(init_mode="preconfig")
