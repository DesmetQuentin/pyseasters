Installation
============

.. note::

    PySEASTERS is not distributed in any public collection like Conda or pip.
    Thus, installation must be done manually (although it's not complicated).


Install the main API
--------------------

Three simple steps:

1. Ensure ``git`` is installed on your machine with (install it if this raises an error):

.. code:: bash

    which git


2. Clone the PySEASTERS repository:

.. code:: bash

    git clone https://github.com/DesmetQuentin/pyseasters.git


3. Install the package in editable mode (where ``</path/to/pyseasters>`` is the directory
that contains ``pyproject.toml``):

.. code:: bash

    pip install -e </path/to/pyseasters>


And you should now be able to import the package and use it's functionalities.
For instance, you could do something like the following in a Python environment:

.. code:: python

    >>> import pyseasters as ps
    >>> ps.VERSION
    1.0.0


.. note::

    Package updates must be done manually, using ``git pull`` in the ``pyseasters``
    repository then installing the package again following step 3.



Install command-line tools
--------------------------

Formally, the command-line interface (CLI) is already accessible after installing the
main API. However you may not have all dependencies right.

Python dependencies can be installed using:

.. code:: bash

    pip install -e </path/to/pyseasters>[cli]


Then the CLI also depends on several common system tools:

* awk
* cat
* tr
* cut
* wc

You may check whether you have them already installed on your machine using the
``which <tool>`` command, and install them in case they are not.


.. seealso::

    The installation for developers is explained
    :doc:`here <../development/installation>`.
