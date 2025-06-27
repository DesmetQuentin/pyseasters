from pathlib import Path
from typing import List, Optional

import pyarrow.parquet as pq

__all__ = ["memory_estimate", "format_memory"]


def _single_file_memory_estimate(
    file: Path, usecols: Optional[List[str]] = None
) -> int:
    """
    Estimate the memory usage (in bytes) of a parquet file once loaded with ``pandas``.

    Parameters
    ----------
    file
        Path to a Parquet file.
    usecols
        Subset of column names to include, as passed to
        `pd.read_parquet() <https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html>`_.

    Returns
    -------
    est_bytes : int
        Estimated memory usage in bytes.
    """
    pq_file = pq.ParquetFile(file)
    meta = pq_file.metadata

    est_bytes = 0

    for row_group_idx in range(meta.num_row_groups):
        row_group = meta.row_group(row_group_idx)

        for col_idx in range(row_group.num_columns):
            col_meta = row_group.column(col_idx)
            col_name = col_meta.path_in_schema

            if usecols and col_name not in usecols:
                continue  # Skip this column

            physical_type = col_meta.physical_type
            num_values = col_meta.num_values

            # Estimate based on physical type
            if physical_type in {"BYTE_ARRAY", "STRING", "UTF8"}:
                avg_len = col_meta.total_compressed_size / max(1, num_values)
                est_bytes += int(num_values * (avg_len + 50))  # 50B Python overhead
            elif physical_type in {"INT64", "DOUBLE", "FLOAT"}:
                est_bytes += num_values * 8
            elif physical_type == "INT32":
                est_bytes += num_values * 4
            else:
                est_bytes += num_values * 16  # fallback

    return est_bytes


def memory_estimate(files: List[Path], usecols: Optional[List[str]] = None) -> float:
    """
    Estimate the memory usage (in bytes) of a list of parquet files once loaded with
    ``pandas``.
    """
    total_bytes = sum(_single_file_memory_estimate(f, usecols=usecols) for f in files)
    return total_bytes


def format_memory(mem: float) -> str:
    """Format ``mem`` bytes as a string using the appropriate units."""
    if mem > 1024**3:
        res = f"{mem / 1024 ** 3:.2f} GB"  # noqa: E231
    elif mem > 1024**2:
        res = f"{mem / 1024 ** 2:.2f} MB"  # noqa: E231
    else:
        res = f"{mem / 1024:.2f} KB"  # noqa: E231
    return res
