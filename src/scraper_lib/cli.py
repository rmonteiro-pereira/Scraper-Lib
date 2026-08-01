"""Console entry point for ScraperLib.

Every documented way of starting the CLI lands here:

* ``scraper --url ...``             -- the ``console_scripts`` entry point
* ``python -m scraper_lib ...``     -- via :mod:`scraper_lib.__main__`
* ``python -m scraper_lib.cli ...`` -- via the ``__main__`` guard below

The flags themselves are defined once, in
:meth:`scraper_lib.ScraperLib.build_arg_parser`, which is also what
``docs/cli.rst`` renders. There is no second copy to drift.
"""

import argparse

from .ScraperLib import ScraperLib


def get_parser() -> argparse.ArgumentParser:
    """Return the CLI parser.

    Used by the ``.. argparse::`` directive in ``docs/cli.rst`` so the published
    page is generated from the real parser instead of being retyped by hand.
    """
    return ScraperLib.build_arg_parser()


def main(argv=None) -> None:
    """Parse ``argv`` (default ``sys.argv[1:]``) and run a download session."""
    ScraperLib.cli(argv)


if __name__ == "__main__":
    main()
