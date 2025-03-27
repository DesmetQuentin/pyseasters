import logging
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

__all__ = ["paths"]

log = logging.getLogger(__name__)

# Machine-specific root directory mapping
MACHINE_CONFIG = {
    "LALL2423D3": Path(
        "/home/desq/work/DATA/SEASTERS"
    ),  # Q. Desmet's laptop for the package development
}

# Detect current machine
CURRENT_MACHINE = socket.gethostname()


@dataclass
class PathConfig:
    root: str = None

    def initialize(self, custom_path: Optional[str] = None):
        """Initialize data directories based on the current machine or a provided custom_path."""
        if custom_path is None:
            if CURRENT_MACHINE in MACHINE_CONFIG.keys():
                self.root = MACHINE_CONFIG[CURRENT_MACHINE]

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

            if CURRENT_MACHINE in MACHINE_CONFIG.keys():
                log.warning(
                    "Custom data directory overrides existing settings for this machine."
                )
                log.info(
                    f"Previous settings: {CURRENT_MACHINE} {MACHINE_CONFIG[CURRENT_MACHINE]}"
                )
                log.info(f"     New settings: {CURRENT_MACHINE} {custom_path}")

            self.root = custom_path

    def is_initialized(self):
        return self.root is not None


paths = PathConfig()
