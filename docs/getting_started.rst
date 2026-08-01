Getting Started
================

Requirements
--------------

Python 3.12. The pinned ``ray`` release only publishes ``cp312`` wheels, so the
repository ships a ``.python-version`` file and ``uv`` picks the right
interpreter for you.

Installation
--------------

From a clone, with `uv <https://docs.astral.sh/uv/>`_:

.. code-block:: bash

   git clone https://github.com/rmonteiro-pereira/Scraper-Lib.git
   cd Scraper-Lib
   uv sync
   uv pip install .

Or with pip, in editable mode:

.. code-block:: bash

   pip install -e .

Quick Example
-------------

.. code-block:: python

   from scraper_lib import ScraperLib

   scraper = ScraperLib(
       base_url="https://example.com/data",
       file_patterns=[".csv", ".zip"],
       download_dir="data",
   )
   scraper.run()

The same run from the command line:

.. code-block:: bash

   scraper --url https://example.com/data --patterns .csv .zip --dir data
