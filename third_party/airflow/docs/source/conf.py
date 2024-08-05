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
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'python-armadaairflowoperator'
copyright = '2022 Armada Project'
author = 'armada@armadaproject.io'


# -- General configuration ---------------------------------------------------

# Jekyll is the style of markdown used by github pages; using
# sphinx_jekyll_builder here allows us to generate docs as
# markdown files.
extensions = ['sphinx.ext.autodoc', 'sphinx_jekyll_builder']

# This setting puts information about typing in the description section instead
# of in the function signature directly. This makes rendered content look much
# better in our template that renders the generated markdown into HTML.
autodoc_typehints = 'description'

# Add any paths that contain templates here, relative to this directory.
templates_path = []

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []
