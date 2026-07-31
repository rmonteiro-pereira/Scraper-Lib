# Changelog

All notable changes to this project are documented here.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning is [semantic](https://semver.org/).

## A note on the tag history

Tags up to `v0.2.336` are artifacts of an automated patch-bump that ran on **every push**;
releases from `v0.3.0` onward are intentional.

That automation had a defect worth recording rather than quietly deleting. The bump job ran on
every push to `master` and pushed its own commit back using a GitHub App token — and app-token
pushes *do* start workflows, unlike `GITHUB_TOKEN`. So each bump triggered another run, which
bumped again, roughly once every 95 seconds. It reached `0.2.336` and only stopped because one
push happened to fail with a transient network error.

Two consequences are permanent and are left in place deliberately:

- **336 git tags**, kept because they are the provenance trail for the versions published to
  PyPI. Deleting them would orphan every published artifact from its source commit, which
  costs real traceability to buy tidiness.
- **~330 PyPI versions**, which cannot be unpublished.

Fixed in [#23](https://github.com/rmonteiro-pereira/Scraper-Lib/pull/23): CI now lints, tests
and builds docs but does not release; releasing is a manual `workflow_dispatch` of
`release.yml`, which bumps, tags, creates a GitHub Release and publishes.

---

## [0.3.0] — 2026-07-31

The first intentional release. No library code changed in this version — it marks the point
where the release process became deliberate.

### Added
- `LICENSE` (MIT). The README had advertised an MIT badge while the repository carried no
  licence file, so the badge was a claim with no artifact behind it.
- `SECURITY.md` — private vulnerability reporting, and a scope section covering what actually
  matters for a downloader: path traversal from crafted remote filenames, redirect handling
  and state-file corruption.
- `CONTRIBUTING.md` — setup, test and lint commands, and the note that `docs/_build/` is
  generated rather than committed.
- `CHANGELOG.md` — this file.
- **GitHub Releases.** `bump2version` only ever wrote a git tag; a Release is a separate API
  object and nothing in the pipeline created one. That step now exists.

### Fixed
- **The self-triggering version-bump loop** described above.
- Repository URL placeholders: `yourusername` in `README.md` and `seuusuario` in
  `docs/getting_started.rst` — the second is the Portuguese variant of the same template
  default, which a search for the English form alone does not find.
- Install instructions referenced `pip install -r requirements.txt` and Poetry; neither exists
  here. The project uses `uv` with a committed `uv.lock`.
- `pyproject.toml` still carried the default `description = "Add your description here"`.

### Changed
- `docs/_build/` is no longer tracked — 63 files of generated Sphinx output, including a
  1.6 MB `fontawesome.js.map`. The *Deploy Documentation* workflow rebuilds it from source, so
  the committed copy was redundant.
- CI permissions reduced to `contents: read`; nothing in the integration workflow writes to the
  repository any more.

### Known issues
- **PyPI publishing is currently failing.** Trusted publishing binds to the workflow
  *filename*, and the publisher is still registered against `ci-cd.yml` while the job now runs
  from `release.yml`. Adding a trusted publisher for `release.yml` in the PyPI project settings
  fixes it; no code change is involved.
- CI jobs carry `if: github.ref == 'refs/heads/master'`, so they do not run on pull requests.

---

## [0.2.x] — 2025 to 2026-07-31

Automated patch bumps, one per push. See the note above; individual versions in this range do
not correspond to deliberate releases.
