Command Line Interface
======================

Usage
------
.. code-block:: text
   
   scraper [OPTIONS] [URL]

Main Options
-------------
--patterns TEXT           File patterns to match [multiple allowed]
--output PATH             Output directory [default: ./downloads]
--max-workers INTEGER     Parallel workers [default: 4]
--resume / --no-resume    Resume previous session [default: resume]

Examples
---------
Download all CSV files:
.. code-block:: bash
   scraper --url https://data.example.com --patterns .csv

Limit to 10 files:
.. code-block:: bash
   scraper --url https://data.example.com --max-files 10