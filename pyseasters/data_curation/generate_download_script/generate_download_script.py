from typing import Optional

from pyseasters.utils._typing import PathLike

from ._ghcnd_data import generate_ghcnd_data_download_script
from ._ghcnd_metadata import generate_ghcnd_metadata_download_script

__all__ = ["generate_download_script"]

_dispatcher = {
    "GHCNd": generate_ghcnd_data_download_script,
    "GHCNd metadata": generate_ghcnd_metadata_download_script,
}


def generate_download_script(
    key: str,
    output: Optional[PathLike] = None,
) -> str:
    """Generate a download script in bash for the provided ``key``.

    Parameters
    ----------
    key
        The key associated with the desired download script.
        Available keys are: 'GHCNd' and 'GHCNd metadata'.
    output: PathLike, default None
        Optional path to an output file to write the script in.

    Returns
    -------
    script : str
        The download script.

    Raises
    ------
    ValueError
        If the ``key`` is not valid.
    """

    try:
        script = _dispatcher[key](output)
    except KeyError:
        raise ValueError(
            f"Provided key ('{key}') invalid. Please provide one of "
            + f"""{",".join(["'%s'" % (key) for key in _dispatcher.keys()])}."""
        )

    return script
