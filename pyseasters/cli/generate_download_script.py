#!/bin/usr python3

"""
Entry point for the ``generate_download_script`` command. The command has a help option:

.. code-block:: console

    $ generate_download_script --help
    usage: generate_download_script [-h] key

    Generate a download bash script for the desired data.

    positional arguments:
    key                   key associated with the desired data (one of 'GHCNd', 'GHCNd metadata', 'GHCNh', 'GHCNh metadata')

    options:
    -h, --help            show this help message and exit
"""

import argparse
import logging
import sys

from pyseasters.data_curation import generate_download_script
from pyseasters.data_curation.download.main import (
    _dispatcher,
)
from pyseasters.utils._logging import setup_cli_logging


def main():
    parser = argparse.ArgumentParser(
        description="Generate a download bash script for the desired data."
    )
    parser.add_argument(
        "key",
        help="key associated with the desired data "
        + f"""(one of {", ".join(["'%s'" % (key) for key in _dispatcher.keys()])})""",
    )
    args = parser.parse_args()

    setup_cli_logging(logging.INFO)
    generate_download_script(args.key)


if __name__ == "__main__":
    sys.exit(main())
