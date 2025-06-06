#!/bin/usr python3

"""
Entry point for the ``preprocess_ghcnh`` command. The command has a help option:

.. code-block:: console

    $ preprocess_ghcnh --help
    usage: preprocess_ghcnh [-h] [-S STEP] [-n NTASKS] [-m MEMORY_PER_TASK] [-v] [-s] [-f]

    Preprocess GHCNh data files.

    options:
    -h, --help            show this help message and exit
    -S, --step STEP       choose preprocessing step without prompting (1 or 2; default: prompt)
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

from pyseasters.data_curation import preprocess_ghcnh
from pyseasters.utils._logging import setup_cli_logging


def main():
    parser = argparse.ArgumentParser(description="Preprocess GHCNh data files.")
    parser.add_argument(
        "-S",
        "--step",
        type=int,
        default=None,
        help="choose preprocessing step without prompting (1 or 2; default: prompt)",
    )
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

    if not args.step:
        step = int(input("Choose preprocessing step (1/2): ").strip())
        if step not in (1, 2):
            sys.stderr.write("Chosen step is not valid (choose 1 or 2).\n")
            sys.stderr.write("Abort.\n")
            sys.exit(0)
    else:
        if args.step not in (1, 2):
            sys.stderr.write("Chosen step is not valid (choose 1 or 2).\n")
            sys.stderr.write("Abort.\n")
            sys.exit(0)

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
    preprocess_ghcnh(
        step if not args.step else args.step,
        ntasks=args.ntasks,
        memory_limit=args.memory_per_task,
    )


if __name__ == "__main__":
    sys.exit(main())
