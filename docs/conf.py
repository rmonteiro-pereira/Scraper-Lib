import os
import sys

# The package lives at src/scraper_lib, so 'src' is what belongs on sys.path.
# This lets `sphinx-build` work from a checkout without an install; when the
# package IS installed (as in CI) the import resolves either way.
sys.path.insert(0, os.path.abspath('../src'))

project = 'ScraperLib'
copyright = '2025, Rodrigo Monteiro Pereira'
author = 'Rodrigo Monteiro Pereira'

from scraper_lib import __version__ as release  # noqa: E402

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
# No docs/_static directory is committed; listing it only produced a warning
# on every build.
html_static_path = []

# Adicione ou modifique estas opções do tema
# The version switcher used to point at https://example.com/versions.json, a
# placeholder that 404s on every build and rendered a dead control on the
# published site. There is only one published version of these docs.
html_theme_options = {
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
}