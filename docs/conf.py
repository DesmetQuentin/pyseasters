# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pyseasters"
copyright = "2025, Quentin Desmet"
author = "Quentin Desmet"
release = "1.0.0-alpha.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "autodoc2",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]


# -- Options for myst-parser -------------------------------------------------
myst_enable_extensions = ["colon_fence"]


# -- Options for autodoc2 ----------------------------------------------------
autodoc2_packages = ["../pyseasters"]
# autodoc2_packages = [{"path": "../pyseasters", "auto_mode": False}]  # disable auto api
# autodoc2_render_plugin = "myst"  # produce apidocs file in myst
autodoc2_hidden_objects = ["dunder", "private", "inherited"]
# autodoc2_module_all_regexes = [r"my_package\..*"]  # only include what's in __all__
autodoc2_hidden_regexes = [
    "pyseasters.*log",
    "pyseasters.cli.*main",
    "pyseasters.constants.countries.COUNTRIES",
    "pyseasters.constants.paths.paths",
    "pyseasters.utils.units.ureg",
]
autodoc2_index_template = """API Reference
=========================

.. autodoc2-docstring:: pyseasters
   :allowtitles:

.. toctree::
   :titlesonly:
   
   pyseasters/pyseasters.load_gauge_data
   pyseasters/pyseasters.ghcnd
   pyseasters/pyseasters.constants
   pyseasters/pyseasters.utils
   pyseasters/pyseasters.data_curation

.. note::
    This page contains auto-generated API reference documentation created with
    `sphinx-autodoc2 <https://github.com/chrisjsewell/sphinx-autodoc2>`_.
"""
