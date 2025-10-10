"""Provide the single :func:`preprocess_bsrn` function."""

import logging
import re
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, cast

import pandas as pd
from dask import compute, delayed
from dask.distributed import Client, LocalCluster

from pyseasters.constants import paths
from pyseasters.utils._logging import LoggingStack, LoggingStackPickle

__all__ = ["preprocess_bsrn"]

log = logging.getLogger(__name__)


def _parse_metadata_comment(text: str) -> Dict[str, Any]:
    """Parse a PANGAEA-style header block into a structured metadata dict."""
    lines = text.strip().splitlines()
    metadata: Dict[str, Any] = {}
    key = None
    buf = []

    for line in lines:
        m = re.match(r"^([A-Za-z0-9/() \[\]\-]+):\s*(.*)", line)
        if m:
            if key:
                metadata[key] = "\n".join(buf).strip()
            key = m.group(1).strip()
            buf = [m.group(2).strip()]
        elif key:
            buf.append(line.strip())
    if key:
        metadata[key] = "\n".join(buf).strip()

    def parse_keyval_block(block):
        """Parse key1: val1 * key2: val2 ... or key1: val1\nkey2: val2 into dict."""
        # Split on * or newline
        items = re.split(r"\*|\n", block)
        d = {}
        for s in items:
            s = s.strip()
            if not s:
                continue
            if ":" in s:
                k, v = s.split(":", 1)
                d[k.strip()] = v.strip()
        return d

    # Coverage
    if "Coverage" in metadata:
        metadata["Coverage"] = parse_keyval_block(metadata["Coverage"])

    # Event(s)
    if "Event(s)" in metadata:
        parts = [s.strip() for s in metadata["Event(s)"].split("*") if s.strip()]
        if parts:
            first = parts[0]
            events = {
                "STATION": first.split(":", 1)[-1].strip() if ":" in first else first
            }
            for s in parts[1:]:
                if ":" in s:
                    k, v = s.split(":", 1)
                    events[k.strip()] = v.strip()
            metadata["Event(s)"] = events

    # Parameter(s)
    if "Parameter(s)" in metadata:
        params = {}
        lines = re.sub(r"\n", " ", metadata["Parameter(s)"], flags=re.DOTALL).split(
            " * "
        )
        # Recombine lines split by newlines
        block_lines = (
            text.split("Parameter(s):", 1)[1].split("License:", 1)[0].splitlines()
        )
        block_lines = [line.strip() for line in block_lines if line.strip()]

        for line in block_lines:
            # Stop if the next header appears
            if re.match(r"^[A-Za-z0-9/() \[\]\-]+:", line) and not line.startswith(
                "\t"
            ):
                break
            # Each parameter line starts with a label, possibly multiline
            m = re.match(r"^(.*?)\s*\(([^)]+)\)\s*\*\s*(.*)", line)
            if not m:
                continue
            label, shortname, rest = m.groups()
            if any(x in label for x in ("DATE/TIME", "HEIGHT")):
                continue
            p = parse_keyval_block(rest)
            params[shortname] = {"Label": label.strip(), **p}

        metadata["Parameter(s)"] = params

    metadata.pop("DATA DESCRIPTION")

    return metadata


def _radiation_to_parquet(path: Path) -> Tuple[str, str]:
    """Convert the radiation-type file in ``path`` into parquet."""
    # Decode
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="latin-1")

    # Separate metadata from data
    match = re.search(r"/\*(.*?)\*/", text, re.DOTALL)
    assert match is not None
    comment_text = match.group(1).strip()
    data_text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL).strip()
    df = pd.read_csv(
        StringIO(data_text),
        sep="\t",
        header=0,
        parse_dates=["Date/Time"],
        date_format="%Y-%m-%dT%H:%M",
        index_col="Date/Time",
    )

    # Process
    assert isinstance(df.index, pd.DatetimeIndex)
    df.index = df.index.tz_localize("UTC")
    df.drop(columns=["Height [m]"], inplace=True, errors="ignore")
    df = df.rename(columns=lambda col: re.sub(" \[.*\]", "", col))  # noqa: W605
    metadata = _parse_metadata_comment(comment_text)
    df.attrs.update(cast(Mapping[object, Any], metadata))

    # Output
    parquet_path = path.with_suffix(".parquet")
    df.to_parquet(parquet_path)
    start, end = df.index[0].isoformat(), df.index[-1].isoformat()

    return start, end


def _horizon_to_parquet(path: Path) -> Tuple[str, str]:
    """Convert the horizon file in ``path`` into parquet."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="latin-1")

    # Separate metadata from data
    match = re.search(r"/\*(.*?)\*/", text, re.DOTALL)
    assert match is not None
    comment_text = match.group(1).strip()
    data_text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL).strip()
    df = pd.read_csv(
        StringIO(data_text),
        sep="\t",
        header=0,
        index_col="Azim [deg]",
    )

    # Process
    df = df.rename(columns=lambda col: re.sub(" \[.*\]", "", col))  # noqa: W605
    df.index.name = "Azim"
    metadata = _parse_metadata_comment(comment_text)
    df.attrs.update(cast(Mapping[object, Any], metadata))

    # Output
    parquet_path = path.with_suffix(".parquet")
    df.to_parquet(parquet_path)

    return "none", "none"


@delayed
def _preprocess_single_file(
    path: Path,
) -> Tuple[LoggingStackPickle, Tuple[str, str, int, int, str, str]]:
    """Dask task preprocessing a single file."""

    logger = LoggingStack(
        name=path.with_suffix("").name
    )  # Initiate logging stack for deferred logging

    # Parse station, typ and date
    split_path = path.with_suffix("").name.split("_")
    assert len(split_path) >= 2
    station = split_path[0]
    if len(split_path) == 4:
        typ = "_".join(split_path[1:3])
    else:
        typ = split_path[1]
    if len(split_path) >= 3:
        date = split_path[-1]
        year, month = list(map(int, date.split("-")))
    else:
        year, month = -1, -1

    # Convert to parquet
    try:
        if typ != "horizon":
            start, end = _radiation_to_parquet(path)
        else:
            start, end = _horizon_to_parquet(path)
    except Exception as e:
        logger.error("Could not read or convert: %s", e)
        return logger.picklable(), (station, typ, year, month, "none", "none")

    logger.info("Task completed for %s", path.with_suffix("").name)

    return logger.picklable(), (station, typ, year, month, start, end)


def preprocess_bsrn(
    ntasks: Optional[int] = None, memory_limit: Optional[str] = None
) -> None:
    """Preprocess BSRN files."""

    # Set up Dask
    cluster = (
        LocalCluster()
        if ntasks is None and memory_limit is None
        else (
            LocalCluster(n_workers=ntasks, threads_per_worker=1)
            if memory_limit is None
            else (
                LocalCluster(memory_limit=memory_limit)
                if ntasks is None
                else LocalCluster(
                    n_workers=ntasks, threads_per_worker=1, memory_limit=memory_limit
                )
            )
        )
    )
    client = Client(cluster)
    log.info("Dask cluster is running.")
    tasks = [
        _preprocess_single_file(file)
        for file in (paths.bsrn() / "data").glob("*/*.tab")
    ]

    # Run parallel Dask tasks
    try:
        results = compute(*tasks)
        [stacked_messages, file_info] = list(zip(*results))
        del results
    finally:
        client.shutdown()
        client.close()
        cluster.close()
        log.info("Dask cluster has been properly shut down.")

    # Flush logging statements per station
    log.info("Logging statements (if any) are printed below.")
    for args in stacked_messages:
        LoggingStack(*args).flush(logger=log)

    # Prepare inventory
    inventory = pd.DataFrame(
        file_info, columns=["station_id", "type", "year", "month", "start", "end"]
    )

    # Fill horizon list in station metadata file
    horizon = (
        inventory[inventory["type"] == "horizon"]
        .drop(columns=["year", "month", "start", "end"])
        .set_index("station_id")
    )
    col = "has horizon"
    horizon[col] = True
    horizon.drop(columns=["type"], inplace=True)
    stations = pd.read_parquet(paths.bsrn_stations())
    if col in stations.columns:
        stations.update(horizon)
    else:
        stations = stations.join(horizon, how="left")
    stations[col] = stations[col].fillna(False).astype(bool)
    stations.to_parquet(paths.bsrn_stations())

    # Write/update inventory
    inventory = inventory[inventory["type"] != "horizon"]
    inventory = inventory[inventory["start"] != "none"]
    inventory = inventory[inventory["end"] != "none"]
    inventory["start"] = pd.to_datetime(inventory["start"])
    inventory["end"] = pd.to_datetime(inventory["end"])
    inventory = inventory.set_index(["station_id", "type", "year", "month"])
    if paths.bsrn_inventory().exists():
        log.info("Update existing inventory file.")
        inventory = inventory.combine_first(pd.read_parquet(paths.bsrn_inventory()))
    inventory.sort_index().to_parquet(paths.bsrn_inventory())

    # Delete original files
    for file in (paths.bsrn() / "data").glob("*/*.tab"):
        if file.with_suffix(".parquet").exists():
            file.unlink()

    log.info("BSRN data preprocessing completed.")


"""
import numpy as np


def plot_horizon(origin_kw={}, horizon_kw={}):

    # Prepare data
    df = "TODO"
    df.index = np.deg2rad(df.index)
    theta = np.linspace(0, 2*np.pi, 100)
    origin = np.zeros_like(theta)

    # Prepare style
    _origin_kw = dict(ls="--", c="gray")
    _origin_kw.update(origin_kw)
    _horizon_kw = dict(ls="none", marker="x")
    _horizon_kw.update(horizon_kw)

    # Plot
    fig, ax = plt.subplots(subplot_kw=dict(projection="polar"))
    plot_origin = ax.plot(theta, origin, **_origin_kw)
    plot_horizon = df.plot(ax=ax, **_horizon_kw)

    # Adjust
    maximum = float(df.max().iloc[0])
    ax.set_ylim(- maximum, maximum * 11 / 10)
    ax.set_yticklabels(["" if i < 0 else i for i in ax.get_yticks()])

    return plot_origin, plot_horizon
"""
