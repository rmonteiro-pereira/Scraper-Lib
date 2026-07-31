# Contributing

Thanks for taking a look. This is a small library; the bar is "does it work, is it tested,
and is it clear a year from now."

## Setup

Requires **Python 3.12+** and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/rmonteiro-pereira/Scraper-Lib.git
cd Scraper-Lib
uv sync --all-groups     # runtime + dev + docs dependencies
```

`uv.lock` is committed, so `uv sync` reproduces the exact resolved dependency set. Please do
not commit a re-resolved lock file unless changing a dependency is the point of the change.

## Running the tests

```bash
uv run pytest -q
```

Add a test with any behaviour change. Cover the failure path, not only the happy path — this
library's interesting behaviour is in retries, partial downloads and resumed state, and those
are exactly the paths that break silently.

## Linting

```bash
uv run ruff check .
uv run ruff format --check .
```

## Docs

```bash
cd docs && uv run sphinx-build -b html . _build/html
```

`docs/_build/` is generated output and is **not** committed — the `Deploy Documentation`
workflow rebuilds it from source and publishes to GitHub Pages.

## Pull requests

- Branch from the default branch; do not commit directly to it.
- Use [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`,
  `docs:`, `chore:`, `test:`, `refactor:`.
- Explain **why** in the commit body, not just what. The diff already says what.
- Keep one concern per PR.
- Make sure `pytest` and `ruff` pass before opening it.

## Versioning

Versions are managed by `bumpversion` via `.bumpversion.cfg`; do not hand-edit the version in
`pyproject.toml`.
