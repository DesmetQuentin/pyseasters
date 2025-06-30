"""
Provide core functions dedicated to data curation, all used in PySEASTERS' CLI.

.. important::

   Dependencies for this subpackage of PySEASTERS are labelled ``cli``. They can be
   installed using:

   .. code:: shell

      pip install -e .[cli]


   in the PySEASTERS directory (where ``pyproject.toml`` is located).


.. attention::

   Most functions rely on Unix tools. They are aimed to run on a Linux system.
"""

from .download import generate_download_script
from .preprocess import (
    preprocess_ghcnd_data,
    preprocess_ghcnd_metadata,
    preprocess_ghcnh,
    preprocess_ghcnh_metadata,
    preprocess_gsdr,
)
