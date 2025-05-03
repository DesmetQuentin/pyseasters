# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import logging
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

rtd_version = os.environ.get("READTHEDOCS_VERSION_NAME", "")
if rtd_version == "dev":
    switcher_version = "dev"
elif rtd_version == "stable":
    switcher_version = "v1.x"
else:
    switcher_version = "v1.x"
log.info("Set switcher version to %s", switcher_version)


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pyseasters"
copyright = "2025, Quentin Desmet"
author = "Quentin Desmet"
release = "1.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
    "sphinx_design",
    "sphinxcontrib.bibtex",
    # "myst_parser",
    # "autodoc2",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for napoleon ----------------------------------------------------
napoleon_google_docstring = False
napoleon_numpy_docstring = True


# -- Options for autodoc
autodoc_typehints = "description"
autodoc_typehints_description_target = "all"


# -- Options for bibtex
bibtex_bibfiles = ["_static/references.bib"]
bibtex_default_style = "plain"
bibtex_reference_style = "author_year"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_context = {
    "display_github": False,
    "display_version": False,  # disables the Read the Docs default version dropdown
}

html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "navbar_end": ["version-switcher", "navbar-icon-links", "theme-switcher"],
    "github_url": "https://github.com/DesmetQuentin/pyseasters",
    "icon_links": [],
    "switcher": {
        "json_url": "https://pyseasters.readthedocs.io/en/dev/_static/versions.json",
        "version_match": switcher_version,
    },
    "logo": {
        "text": "PySEASTERS",
    },
}
# html_extra_path = ["versions.json"]

html_static_path = ["_static"]
html_css_files = [
    "custom.css",
]
html_sidebars = {
    "database/*": ["sidebar-nav-bs"],
    "user_guide/*": ["sidebar-nav-bs"],
    "api/*": ["sidebar-nav-bs"],
    "faq/*": ["sidebar-nav-bs"],
    "development/*": ["sidebar-nav-bs"],
    "install*": [],
}

# violet: #8045e5
# blue: #0a7d91

"""
# -- Options for myst-parser -------------------------------------------------
myst_enable_extensions = ["colon_fence"]
"""

'''
# -- Options for autodoc2 ----------------------------------------------------
autodoc2_packages = ["../pyseasters"]
# autodoc2_packages = [{"path": "../pyseasters", "auto_mode": False}]  # disable auto api
# autodoc2_render_plugin = "myst"  # produce apidocs file in myst
autodoc2_hidden_objects = ["dunder", "private", "inherited"]
# autodoc2_module_all_regexes = [r"my_package\..*"]  # only include what's in __all__  # noqa: W605
autodoc2_hidden_regexes = [
    "pyseasters.*log",
    "pyseasters.cli.*main",
    "pyseasters.constants.countries.COUNTRIES",
    "pyseasters.constants.pathconfig.paths",
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
'''
