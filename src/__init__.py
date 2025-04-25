# filepath: src/scraper_lib/__init__.py
"""
ScraperLib: A library for parallel web scraping and downloading.
"""
__version__ = "0.2.295" # Define version here

# Make the main class and components easily importable
from .ScraperLib import ScraperLib, DownloadState
from .CustomLogger import CustomLogger
 

__all__ = [
    'ScraperLib',
    'CustomLogger',
    'DownloadState',
    '__version__',
]