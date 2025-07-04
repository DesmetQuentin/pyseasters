"""
Provide the dispatching
:func:`generate_download_script` function.
"""

from ._ghcnd_data import generate_ghcnd_data_download_script
from ._ghcnd_metadata import generate_ghcnd_metadata_download_script
from ._ghcnh_data import generate_ghcnh_data_download_script
from ._ghcnh_metadata import generate_ghcnh_metadata_download_script

__all__ = ["generate_download_script"]

_dispatcher = {
    "GHCNd": generate_ghcnd_data_download_script,
    "GHCNd metadata": generate_ghcnd_metadata_download_script,
    "GHCNh": generate_ghcnh_data_download_script,
    "GHCNh metadata": generate_ghcnh_metadata_download_script,
}


def generate_download_script(key: str) -> None:
    """
    Generate a download script in bash for the provided ``key``, at the location of
    the related folder in the database.

    .. note::

       There is no key relating to the :ref:`GSDR <gsdr>` dataset because it cannot be
       retrieved online.


    Parameters
    ----------
    key
        The key associated with the desired download script.
        Available keys are: 'GHCNd', 'GHCNd metadata', 'GHCNh' and 'GHCNh metadata'.

    Raises
    ------
    ValueError
        If the ``key`` is not valid.
    """

    try:
        _dispatcher[key]()
    except KeyError:
        raise ValueError(
            f"Provided key ('{key}') invalid. Please provide one of "
            + f"""{",".join(["'%s'" % (key) for key in _dispatcher.keys()])}."""
        )
