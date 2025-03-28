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
    with importlib.resources.files("pyseasters.data").joinpath("paths.yaml").open(
        "r"
    ) as file:
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


@dataclass
class PathConfig:
    root: str = None

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

    def is_initialized(self):
        return self.root is not None


paths = PathConfig()
