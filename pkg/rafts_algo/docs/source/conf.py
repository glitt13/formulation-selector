# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'rafts_algo'
copyright = '2026, NOAA-OWP'
author = 'NOAA-OWP'
release = '0.1.0'

import os
import sys
# Tell Sphinx where to find the rafts_algo python module
# Adjust the path depending on if your code is in a 'src' layout or directly in 'rafts_algo'
sys.path.insert(0, os.path.abspath('../../rafts_algo'))

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon', # Supports Google and NumPy style docstrings
    'sphinx.ext.viewcode', # Adds links to highlighted source code
]

html_theme = 'sphinx_rtd_theme'

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
