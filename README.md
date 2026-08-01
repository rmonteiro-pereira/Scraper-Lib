# ScraperLib

![Python](https://img.shields.io/badge/Python-3.12%2B-blue)
![Ray](https://img.shields.io/badge/Ray-Parallel-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

[📚 **Documentation**](https://rmonteiro-pereira.github.io/Scraper-Lib/)

<pre>
<span style="color:#FFD700;">   _____                                 _      _ _     </span>
<span style="color:#00BFFF;">  / ____|                               | |    (_) |    </span>
<span style="color:#32CD32;"> | (___   ___ _ __ __ _ _ __   ___ _ __ | |     _| |__  </span>
<span style="color:#FFA500;">  \___ \ / __| '__/ _` | '_ \ / _ \ '__|| |    | | '_ \ </span>
<span style="color:#FF69B4;">  ____) | (__| | | (_| | |_) |  __/ |   | |____| | |_) |</span>
<span style="color:#FF6347;"> |_____/ \___|_|  \__,_| .__/ \___|_|   |______|_|_.__/ </span>
<span style="color:#CCCCCC;">                      | |                               </span>
<span style="color:#CCCCCC;">                      |_|                               </span>

<span style="color:#00FF00;">==============================================================</span>                                  
<span style="color:#FFD700;">         Starting download of ScraperLib</span>
<span style="color:#00FF00;">==============================================================</span>                                  
</pre>

---

## ✨ Features

- **Parallel Downloads:** Uses Ray to download multiple files simultaneously, maximizing bandwidth and efficiency.
- **403 Avoidance:** Rotates user-agents, sets referer headers, and uses session management to avoid being blocked.
- **Incremental Mode:** Optionally skip files already downloaded.
- **Robust State Management:** Tracks completed, failed, and skipped downloads with atomic file operations.
- **Progress Visualization:** Uses tqdm for beautiful progress bars.
- **Comprehensive Reporting:** Generates JSON reports and visualizations (if matplotlib is installed) of download delays and errors.
- **Colorful Console Output:** Uses colorama for clear, color-coded logs.
- **Dual Logging:** Terminal shows only relevant events (e.g., `[DONE]` for successful downloads), while the log file contains all attempts, retries, and errors for full traceability.
- **Highly Configurable CLI:** All parameters (parallelism, chunk size, retry/backoff, output dirs, etc.) can be set via command line.

---

## 📦 Installation

Requires **Python 3.12**. The pinned `ray` release publishes `cp312` wheels only,
so the repository ships a `.python-version` and `uv` selects the right
interpreter for you — on 3.13 the install fails with
`ray==2.44.1 ... doesn't have a source distribution or wheel for the current platform`.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/rmonteiro-pereira/Scraper-Lib.git
   cd Scraper-Lib
   ```

2. **Install dependencies** — the project uses [uv](https://docs.astral.sh/uv/)
   and ships a committed `uv.lock`, so this reproduces the exact resolved set:
   ```bash
   uv sync
   uv pip install .
   ```
   Or with pip, installing the package itself in editable mode:
   ```bash
   pip install -e .
   ```

   *Main dependencies:*
   - `ray`
   - `requests`
   - `tqdm`
   - `colorama`
   - `beautifulsoup4`
   - `matplotlib`
   - `numpy`
   - `portalocker`

---

## 🚀 Usage

### CLI

Installing the package puts a `scraper` command on your `PATH`:

```bash
scraper --url <URL> --patterns .csv .zip --dir data --max-files 10
```

The module forms are equivalent:

```bash
python -m scraper_lib --url <URL> --patterns .csv .zip --dir data --max-files 10
python -m scraper_lib.cli --url <URL> --patterns .csv .zip --dir data --max-files 10
```

**Main CLI options:**
- `--url`: Base URL to scrape for files.
- `--patterns`: List of file patterns to match (e.g. .csv .zip).
- `--dir`: Download directory.
- `--incremental`: Enable incremental download state.
- `--max-files`: Limit number of files to download.
- `--max-concurrent`: Max parallel downloads.
- `--chunk-size`: Chunk size for downloads (e.g. 1gb, 10mb, 8 bytes).
- `--initial-delay`: Initial delay between retries (seconds).
- `--max-delay`: Maximum delay between retries (seconds).
- `--max-retries`: Maximum number of download retries.
- `--state-file`: Path for download state file.
- `--log-file`: Path for main log file.
- `--report-prefix`: Prefix for report files.
- `--headers`: Path to JSON file with custom headers.
- `--user-agents`: Path to text file with custom user agents (one per line).
- `--disable-logging`: Disable all logging for production pipelines.
- `--disable-terminal-logging`: Disable terminal logging.
- `--dataset-name`: Dataset name for banner.
- `--disable-progress-bar`: Disable tqdm progress bar.
- `--output-dir`: Directory for report PNGs and JSON.
- `--max-old-logs`: Max old log files to keep (default: 25, None disables rotation).
- `--max-old-runs`: Max old report/png runs to keep (default: 25, None disables rotation).

See all options with:
```bash
scraper --help
```

The full, generated option reference lives at
[the CLI page of the docs](https://rmonteiro-pereira.github.io/Scraper-Lib/cli.html),
rendered by Sphinx from the real `argparse` definition.

### Programmatic Usage

```python
from scraper_lib import ScraperLib

scraper = ScraperLib(
    base_url="https://example.com/data",
    file_patterns=[".csv", ".parquet", ".zip"],
    download_dir="data",
    incremental=True,
    max_files=2,
    max_concurrent=16,
    chunk_size="10mb",
    initial_delay=1.0,
    max_delay=60.0,
    max_retries=5,
    dataset_name="MY DATASET",
)
scraper.run()
```

---

## 🛡️ Anti-Blocking Protocols

- **User-Agent Rotation:** Randomizes the user-agent on **every** request, so a retry
  after a 403 never replays the identity that was just blocked. Held to it by
  `tests/test_user_agent_rotation.py`, which stands up a server that answers 403 to
  the first five requests and asserts the server saw more than one distinct
  user-agent.
- **Referer Header:** Sets a realistic referer to mimic browser behavior.
- **Session Management:** Uses a new HTTP session for each attempt.
- **Exponential Backoff:** Waits longer between retries to avoid rate-limiting.

---

## 📊 Reporting

After execution, a summary is printed to the console and a detailed report is saved as a JSON file. If `matplotlib` is installed, visualizations of download delays are also generated.

---

## 🧪 Testing

To run all tests:

```bash
pytest tests
```

---

## 📝 Project Structure
```
.
├── src/
│   └── scraper_lib/            # The installed package
│       ├── __init__.py         # Public API: ScraperLib, DownloadState, CustomLogger
│       ├── ScraperLib.py       # Main library; also defines the DownloadState class
│       ├── CustomLogger.py     # Custom logger
│       ├── cli.py              # Entry point behind the `scraper` command
│       └── __main__.py         # Makes `python -m scraper_lib` work
├── tests/                      # Unit tests
├── docs/                       # Sphinx sources for the published documentation
├── example.py                  # Example usage (runnable from root)
├── example2.py                 # Minimal example
├── pyproject.toml              # Project metadata and dependencies
├── uv.lock                     # Locked, reproducible dependency set
└── .python-version             # Pins CPython 3.12 (ray ships cp312 wheels only)
```

Output directories are **not** part of the repository — they are created at
runtime, next to wherever you run the scraper, and their names come from the
`--dir`, `--output-dir`, `--log-file` and `--state-file` options:

```
data/                           # Downloaded files          (--dir)
<output-dir>/reports/           # Download reports (JSON)    (--output-dir)
<output-dir>/pngs/              # Delay analysis PNGs        (--output-dir)
logs/                           # Log files                  (--log-file)
state/                          # Download state             (--state-file)
```

---

## 🤝 Contributing

Pull requests and suggestions are welcome! Please open an issue or submit a PR.

---

## 📄 License

This project is licensed under the MIT License.

---

## 📬 Contact

Questions or suggestions? Open an issue or contact [rmonteiropereira1@gmail.com](mailto:rmonteiropereira1@gmail.com).

---

*Happy data hunting with ScraperLib! 🚀*