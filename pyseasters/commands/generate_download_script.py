#!/bin/usr python3
import argparse
import sys
from pathlib import Path
from typing import Optional

from pyseasters.commands.ghcnd import _generate_main_script as _gen_ghcnd_main
from pyseasters.commands.ghcnd import _generate_metadata_script as _gen_ghcnd_meta
from pyseasters.constants.pathconfig import paths

__all__ = ["generate_download_script"]

_accepted_keys = [
    "GHCNd",
    "GHCNd metadata",
]


def generate_download_script(
    key: str,
    output: Optional[str] = None,
) -> Optional[str]:

    if not paths.is_initialized():
        paths.initialize()

    if output is not None:
        output = Path(output)

    if key == "GHCNd metadata":
        res = _gen_ghcnd_meta(output)
    elif key == "GHCNd":
        res = _gen_ghcnd_main(output)
    else:
        raise ValueError(
            f"Provided key ('{key}') invalid. Please provide one in {_accepted_keys}."
        )

    return res


def main():
    parser = argparse.ArgumentParser(
        description="Generate a download bash script for the desired data."
    )
    parser.add_argument(
        "key", help=f"key associated with the desired data (one in {_accepted_keys})"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="path to output file (default: print to stdout)",
    )

    args = parser.parse_args()
    res = generate_download_script(args.key, args.output if args.output != "" else None)
    if res is not None:
        sys.stdout.write(res)


if __name__ == "__main__":
    main()
