import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

project = 'ScraperLib'
copyright = '2025, Seu Nome'
author = 'Seu Nome'
release = '0.1.0'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.todo',
    'sphinx.ext.autosummary',
    'sphinxarg.ext',
]

templates_path = ['_templates']
exclude_patterns = []
html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']