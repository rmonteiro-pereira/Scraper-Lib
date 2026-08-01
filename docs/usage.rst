Usage
=====

Command Line Interface
----------------------

Installing the package provides the ``scraper`` command. See :doc:`cli` for the
full option list.

.. code-block:: bash

   scraper --url <URL> --patterns .csv .zip --dir data --max-files 10

The module forms are equivalent:

.. code-block:: bash

   python -m scraper_lib --url <URL> --patterns .csv .zip --dir data --max-files 10
   python -m scraper_lib.cli --url <URL> --patterns .csv .zip --dir data --max-files 10

Programmatic Usage
------------------

.. code-block:: python

   from scraper_lib import ScraperLib

   scraper = ScraperLib(
       base_url="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
       file_patterns=[".csv", ".parquet", ".zip"],
       download_dir="tlc_data",
       state_file="state/download_state.json",
       log_file="logs/process_log.log",
       output_dir="output",
       incremental=True,
       max_files=30,
       max_concurrent=16,
       dataset_name="TLC DATA",
       max_old_logs=10,
       max_old_runs=10,
   )
   scraper.run()
