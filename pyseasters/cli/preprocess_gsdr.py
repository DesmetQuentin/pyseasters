#!/bin/usr python3

"""
Entry point for the ``preprocess_gsdr`` command. The command has a help option:

.. code-block:: console

    $ preprocess_gsdr --help
    usage: preprocess_gsdr [-h] [-n NTASKS] [-m MEMORY_PER_TASK] [-v] [-s] [-f]

    Preprocess GSDR data files.

    options:
    -h, --help            show this help message and exit
    -n, --ntasks NTASKS   number of tasks to run in parallel (default: auto)
    -m, --memory-per-task MEMORY_PER_TASK
                          memory limit per task (default: auto)
    -v, --verbose         enable debug output
    -s, --silent          disable info output (priority to --verbose)
    -f, --force           disable confirmation prompt
"""

import argparse
import logging
import sys

from pyseasters.data_curation import preprocess_gsdr
from pyseasters.utils._logging import setup_cli_logging


def main():
    parser = argparse.ArgumentParser(description="Preprocess GSDR data files.")
    parser.add_argument(
        "-n",
        "--ntasks",
        type=int,
        default=None,
        help="number of tasks to run in parallel (default: auto)",
    )
    parser.add_argument(
        "-m",
        "--memory-per-task",
        type=str,
        default=None,
        help="memory limit per task (default: auto)",
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
    preprocess_gsdr(
        ntasks=args.ntasks,
        memory_limit=args.memory_per_task,
    )


if __name__ == "__main__":
    sys.exit(main())
