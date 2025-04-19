Getting Started
================

Installation
--------------

Command Line Interface
------------------------


.. code-block:: bash

   git clone https://github.com/seuusuario/Scraper-Lib.git
   cd Scraper-Lib
   pip install -r requirements.txt

Quick Example
-------------

.. code-block:: python

   from scraper_lib import ScraperLib

   scraper = ScraperLib(
       base_url="https://example.com/data",
       file_patterns=[".csv", ".zip"],
       download_dir="data"
   )
   scraper.run()