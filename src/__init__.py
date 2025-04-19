# filepath: src/scraper_lib/__init__.py
"""
ScraperLib: A library for parallel web scraping and downloading.
"""
__version__ = "0.1.0" # Define version here

# Make the main class and components easily importable
from .ScraperLib import ScraperLib
from .CustomLogger import CustomLogger
from .DownloadState import DownloadState

__all__ = [
    'ScraperLib',
    'CustomLogger',
    'DownloadState',
    '__version__',
]