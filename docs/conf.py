import os
import sys
# Adiciona o diretório raiz do projeto (um nível acima de 'docs') ao sys.path
sys.path.insert(0, os.path.abspath('..'))
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

# Adicione ou modifique estas opções do tema
html_theme_options = {
    "navbar_end": ["version-switcher", "theme-switcher", "navbar-icon-links"],
    "switcher": {
        "json_url": "https://example.com/versions.json",
        "version_match": "latest",
    },
}