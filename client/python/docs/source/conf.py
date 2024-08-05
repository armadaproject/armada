# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))


# -- Project information -----------------------------------------------------

project = "python-armadaclient"
copyright = "2022 Armada Project"
author = "jay@gr-oss.io"


# -- General configuration ---------------------------------------------------

# Jekyll is the style of markdown used by github pages; using
# sphinx_jekyll_builder here allows us to generate docs as
# markdown files.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx_jekyll_builder",
    "sphinx_toolbox.more_autodoc.autonamedtuple",
]

# This setting puts information about typing in the description section instead
# of in the function signature directly. This makes rendered content look much
# better in our template that renders the generated markdown into HTML.
autodoc_typehints = "description"


# This setting makes sure that all types are fully documented with their correct paths
# i.e instead of just "Permissions" we get "armada_client.permissions.Permissions"
autodoc_typehints_format = "fully-qualified"


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"
