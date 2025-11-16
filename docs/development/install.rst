Installation for developers
===========================

.. seealso::

   :doc:`Installation <../install>`
      Make sure first to clone SEASTERSdb' Github
      repository as in the main installation guide.


If you are a developer who wishes to contribute to SEASTERSdb in any way, you must
follow a specific workflow which also has some tool dependendies. They are divided
into pure development tools (``dev``) and tools for generating the documentation
(``docs``). Install them from the ``seastersdb`` environment and folder using:

.. code:: bash

   pip install -e .[dev,docs]
