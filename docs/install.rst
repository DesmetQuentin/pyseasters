Installation
============

.. note::

   PySEASTERS is not distributed in any public collection like Conda or ``pip``.
   Thus, installation must be done manually as described on this page.


.. _venv:

Virtual environment
-------------------

First thing is to create a virtual environment to ensure PySEASTERS requirements
do not mess up the Python tools you have already set up on your machine.
Follow the adequate tab below according to your virtual environment manager:

.. tab-set::

   .. tab-item:: Conda

      Create:

      .. code:: bash

         conda create --name pyseasters

      Activate:

      .. code:: bash

         conda activate pyseasters

      Ensure Git and ``pip`` are installed using the ``which <tool>`` command.
      Install them if not (using ``conda install <tool>`` inside the environment).

      To deactivate the environment
      (keep it activated for continuing the installation, though):

      .. code:: bash

         conda deactivate


   .. tab-item:: venv

      Create:

      .. code:: bash

         python -m venv pyseasters

      Activate (you may need to adapt directories below):

      .. code:: bash

         source ~/venv/pyseasters/bin/activate

      Ensure Git and ``pip`` are installed using the ``which <tool>`` command.
      If not, then install them manually or contact your administrator.

      To deactivate the environment
      (keep it activated for continuing the installation, though):

      .. code:: bash

         deactivate


.. _install-main:

Main procedure
--------------

As mentionned while creating the environment above, we now rely on Git and ``pip``:

* Git is used to retrieve the package from its online repository and get its most updated version later on.
* ``pip`` serves to install the downloaded package on your local machine.

Follow the relevant tab below:

.. tab-set::

   .. tab-item:: First installation

      **Retrieve the online repository**:

      .. code:: bash

         git clone https://github.com/DesmetQuentin/pyseasters.git

      This should create a ``pyseasters`` folder. **Change directory**:

      .. code:: bash

         cd pyseasters/

      From within your newly created virtual environment, **install PySEASTERS**:

      .. code:: bash

         pip install -e .

      Now, you need to **let PySEASTERS know where is the database** on your machine.
      Contact your data maintainer to know the database root directory.
      Change directory into it and it should contain a ``configure_api.py`` script.
      Run it:

      .. code:: bash

         python configure_api.py

      .. note::

         We assume here that the database on your machine has been set up
         as guided throughout :ref:`this page <replicate>`.


   .. tab-item:: Update

      From within your ``pyseasters`` environment, and in the package directory
      (where the ``pyproject.toml`` is located),
      **update the local code** with the newer features online (if any):

      .. code:: bash

         git pull origin main

      Then, **reinstall** the package:

      .. code:: bash

         pip install -e .


You should now be able to import the package and use its functionalities *anywhere*,
as long as your ``pyseasters`` virtual environment is activated.
For instance:

.. code:: pycon

   >>> import pyseasters as ps
   >>> ps.VERSION
   '1.0.0'


.. _install-cli:

Command-line tools (optional)
-----------------------------

Formally, the command-line interface (CLI) is already accessible after installing the
main API. However you may not have all dependencies right.
In fact, the ``pip install -e`` command can be decorated with optional keywords,
and, in this case, CLI dependencies can be installed using:

.. code:: bash

   pip install -e .[cli]


But the above command only deals with Python dependencies.
The CLI also depends on several common system tools:

.. hlist::
   :columns: 6

   * awk
   * cat
   * tr
   * cut
   * wc
   * column


You may check whether you have them already installed on your machine using the
``which <tool>`` command, and install them in case they are not.


.. seealso::

   Installation for developers is explained
   :doc:`here <../development/install>`.
