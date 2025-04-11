#!/bin/usr python3

"""
Entry point for the ``preprocess_ghcnd_data`` command. The command has a help option:

.. code-block:: console

    $ preprocess_ghcnd_data --help
    usage: preprocess_ghcnd_data [-h] [-m] [-v] [-s] [-f]

    Preprocess GHCNd data files (remove duplicate columns and compress).

    options:
    -h, --help     show this help message and exit
    -m, --minimal  keep orignial csv format (default: convert to parquet)
    -v, --verbose  enable debug output
    -s, --silent   disable info output (priority to --verbose)
    -f, --force    disable confirmation prompt
"""

import argparse
import logging
import sys

from ._utils import setup_cli_logging
from .preprocess import preprocess_ghcnd_data


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess GHCNd data files "
        + "(remove duplicate columns and compress)."
    )
    parser.add_argument(
        "-m",
        "--minimal",
        action="store_true",
        help="keep orignial csv format (default: convert to parquet)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug output"
    )
    parser.add_argument(
        "-s",
        "--silent",
        action="store_true",
        help="disable info output (priority to --verbose)",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="disable confirmation prompt",
    )
    args = parser.parse_args()

    if not args.force:
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

    setup_cli_logging(
        logging.DEBUG
        if args.verbose
        else (logging.INFO if not args.silent else logging.WARNING)
    )
    preprocess_ghcnd_data(to_parquet=not args.minimal)


if __name__ == "__main__":
    sys.exit(main())
