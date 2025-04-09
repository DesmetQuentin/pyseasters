#!/bin/usr python3

"""
Entry point for the ``preprocess_ghcnd_metadata`` command.
The command has a help option:

.. code-block:: console

    $ preprocess_ghcnd_metadata --help
    usage: preprocess_ghcnd_metadata [-h] [-v]

    Preprocess GHCNd metadata files (filter countries and remove duplicate columns).

    options:
    -h, --help     show this help message and exit
    -v, --verbose  Enable debug output
"""

import argparse
import logging
import sys

from ._utils import setup_cli_logging
from .preprocess import preprocess_ghcnd_metadata


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Preprocess GHCNd metadata files "
            + "(filter countries and remove duplicate columns)."
        )
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug output"
    )
    args = parser.parse_args()

    response = (
        input(
            "This program modifies files in place. Are you sure you want to continue? "
            + "(y/[n]): "
        )
        .strip()
        .lower()
    )
    if response not in ("y", "yes"):
        sys.stderr.write("Aborted by user.\n")
        sys.exit(0)

    setup_cli_logging(logging.DEBUG if args.verbose else logging.INFO)
    preprocess_ghcnd_metadata()


if __name__ == "__main__":
    main()
