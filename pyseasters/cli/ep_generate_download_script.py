#!/bin/usr python3

"""
Entry point for the ``generate_download_script`` command. The command has a help option:

.. code-block:: console

    $ generate_download_script --help
    usage: generate_download_script [-h] [-o OUTPUT] key

    Generate a download bash script for the desired data.

    positional arguments:
    key                   key associated with the desired data (one of 'GHCNd', 'GHCNd metadata')

    options:
    -h, --help            show this help message and exit
    -o OUTPUT, --output OUTPUT
                          path to output file (default: print to stdout)
"""

import argparse
import sys

from .generate_download_script import generate_download_script
from .generate_download_script.generate_download_script import _dispatcher


def main():
    parser = argparse.ArgumentParser(
        description="Generate a download bash script for the desired data."
    )
    parser.add_argument(
        "key",
        help="key associated with the desired data "
        + f"""(one of {",".joint(["'%s'" %(key) for key in _dispatcher.keys()])})""",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="path to output file (default: print to stdout)",
    )

    args = parser.parse_args()
    script = generate_download_script(
        args.key, args.output if args.output != "" else None
    )
    if args.output == "":
        sys.stdout.write(script)


if __name__ == "__main__":
    main()
