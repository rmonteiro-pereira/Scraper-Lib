"""Every documented way of starting the CLI must actually start it.

The old check was ``assert hasattr(ScraperLib, "cli")``, which stayed green
while all four documented invocations raised ModuleNotFoundError: ``cli()`` was
a bare ``@staticmethod`` with no ``__main__.py``, no ``if __name__ ==
"__main__"`` and no ``console_scripts`` entry point.

These tests run the commands the README and the docs tell a reader to run.
"""

import os
import shutil
import subprocess
import sys

import pytest


def _find_console_script():
    """Locate the `scraper` console script.

    `shutil.which` alone is not enough: pytest is often launched as
    `<venv>/bin/python -m pytest`, which does not put the venv's script
    directory on PATH, so the entry point would be silently skipped.
    """
    script_dir = os.path.dirname(sys.executable)
    for name in ("scraper", "scraper.exe"):
        candidate = os.path.join(script_dir, name)
        if os.path.exists(candidate):
            return candidate
    return shutil.which("scraper")


def _run(argv):
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=180,
    )


@pytest.mark.parametrize(
    "module",
    [
        "scraper_lib",       # README.md -- `python -m scraper_lib --help`
        "scraper_lib.cli",   # README.md / docs/usage.rst -- `python -m scraper_lib.cli`
    ],
)
def test_module_invocation_shows_help(module):
    proc = _run([sys.executable, "-m", module, "--help"])
    assert proc.returncode == 0, (
        f"`python -m {module} --help` exited {proc.returncode}\n"
        f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    assert "--url" in proc.stdout
    assert "--max-concurrent" in proc.stdout


def test_console_script_is_installed_and_runs():
    """docs/cli.rst documents a `scraper` command; it must exist."""
    executable = _find_console_script()
    if executable is None:
        pytest.skip("package not installed into the environment (no console script)")
    proc = _run([executable, "--help"])
    assert proc.returncode == 0, (
        f"`scraper --help` exited {proc.returncode}\n"
        f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    assert "--url" in proc.stdout


def test_missing_required_argument_is_reported():
    """A real argparse run, not just an importable attribute."""
    proc = _run([sys.executable, "-m", "scraper_lib"])
    assert proc.returncode != 0
    assert "--url" in proc.stderr


def test_documented_flags_exist_and_invented_ones_do_not():
    """docs/cli.rst used to document --output, --max-workers and --resume.

    None of them were ever in the argparse definition. This pins the real flag
    names so the docs cannot drift back.
    """
    proc = _run([sys.executable, "-m", "scraper_lib", "--help"])
    assert proc.returncode == 0
    help_text = proc.stdout
    for real_flag in ("--url", "--patterns", "--dir", "--max-concurrent",
                      "--incremental", "--max-files", "--chunk-size"):
        assert real_flag in help_text, f"{real_flag} disappeared from the CLI"
    for invented_flag in ("--max-workers", "--resume"):
        assert invented_flag not in help_text, (
            f"{invented_flag!r} now exists in the CLI -- update docs/cli.rst, "
            f"which was rewritten precisely because it documented this flag "
            f"while argparse did not define it"
        )
