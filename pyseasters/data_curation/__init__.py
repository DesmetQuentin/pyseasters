"""
This package provides core functions dedicated to data curation. It relies on several
unix tools.
"""

from .download import generate_download_script
from .preprocess import (
    preprocess_ghcnd_data,
    preprocess_ghcnd_metadata,
    preprocess_ghcnh_metadata,
)
