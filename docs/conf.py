import os
import sys
# Adiciona o diretório raiz do projeto (um nível acima de 'docs') ao sys.path
sys.path.insert(0, os.path.abspath('..'))

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
    "switcher": {
        "json_url": "", # Deixe vazio para desabilitar
        "version_match": "", # Deixe vazio para desabilitar
    },
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links", "theme-switcher"],
    # Adicione outras opções do pydata-sphinx-theme se necessário
}