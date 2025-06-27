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
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for napoleon ----------------------------------------------------
napoleon_google_docstring = False
napoleon_numpy_docstring = True


# -- Options for autodoc
autodoc_typehints = "description"
autodoc_typehints_description_target = "all"
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}
add_module_names = False
autosummary_generate = [
    "api/pyseasters.constants.countries.rst",
    "api/pyseasters.constants.pathconfig.rst",
    "api/pyseasters.data_curation.download.main.rst",
    "api/pyseasters.data_curation.preprocess.ghcnd_data.rst",
    "api/pyseasters.data_curation.preprocess.ghcnd_metadata.rst",
    "api/pyseasters.data_curation.preprocess.ghcnh_metadata.rst",
    "api/pyseasters.data_curation.preprocess.ghcnh.rst",
    "api/pyseasters.data_curation.preprocess.gsdr.rst",
    "api/pyseasters.gauge_data_loaders.rst",
    "api/pyseasters.ghcnd.data_loaders.rst",
    "api/pyseasters.ghcnd.metadata_loaders.rst",
    "api/pyseasters.ghcnh.data_loaders.rst",
    "api/pyseasters.ghcnh.metadata_loaders.rst",
    "api/pyseasters.gsdr.data_loaders.rst",
    "api/pyseasters.gsdr.metadata_loaders.rst",
    "api/pyseasters.utils.units.rst",
]


# -- Options for bibtex
bibtex_bibfiles = ["_static/references.bib"]
bibtex_default_style = "plain"
bibtex_reference_style = "author_year"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

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

rst_prolog = """
.. |rarr| unicode:: U+2192
"""
