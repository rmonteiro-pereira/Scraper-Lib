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

Requires **Python 3.12+**.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/rmonteiro-pereira/Scraper-Lib.git
   cd Scraper-Lib
   ```

2. **Install dependencies** — the project uses [uv](https://docs.astral.sh/uv/)
   and ships a committed `uv.lock`, so this reproduces the exact resolved set:
   ```bash
   uv sync
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

```bash
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
python -m scraper_lib --help
```

### Programmatic Usage

```python
from ScraperLib import ScraperLib

scraper = ScraperLib(
    base_url: str = "https://example.com/data",
    file_patterns: List[str] = [".csv", ".parquet", ".zip"],
    download_dir: str = "data",
    incremental: bool = True,
    max_files: Optional[int] = 2,
    max_concurrent: Optional[int] = 16,
    chunk_size: Union[str, int] = "10mb",
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    max_retries: int = 5,
    dataset_name: Optional[str] = "MY DATASET"
)
scraper.run()
```

---

## 🛡️ Anti-Blocking Protocols

- **User-Agent Rotation:** Randomizes user-agent strings on each request and after 403 errors.
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
│   ├── __init__.py             # Makes src a package
│   ├── scraper_lib.py          # Main library
│   ├── DownloadState.py        # Download state management
│   ├── CustomLogger.py         # Custom logger
├── example.py                  # Example usage (runnable from root)
├── pyproject.toml              # Project metadata and dependencies
├── uv.lock                     # Locked, reproducible dependency set
├── output/
│   ├── pngs/                   # Download delay analysis PNGs
│   └── reports/                # Download reports (JSON)
├── data/                       # Downloaded files
├── logs/                       # Log files
├── state/                      # Download state (auto-generated)
├── tests/                      # Unit tests
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