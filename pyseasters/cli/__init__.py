"""
This package provides the base functions of PySEASTERS' CLI tools and hosts the CLI
entry points ('ep').
"""

from .generate_download_script import generate_download_script
from .preprocess import preprocess_ghcnd_data, preprocess_ghcnd_metadata
