import requests
import logging
from bs4 import BeautifulSoup
import os
os.environ['RAY_DEDUP_LOGS'] = "0"
os.environ["RAY_SILENT_MODE"] = "1"

from tqdm import tqdm
from urllib.parse import urljoin
import time
import random
import sys
import json
from datetime import datetime
import matplotlib.pyplot as plt 
import hashlib
import numpy as np
import ray
from collections import deque
import portalocker
from colorama import init, Fore, Style
import re


BANNER = rf"""
{Fore.CYAN}{Style.BRIGHT}
   _____                                  _      _ _     
  / ____|                                | |    (_) |    
 | (___   ___ _ __ __ _ _ __   ___ _ __  | |     _| |__  
  \___ \ / __| '__/ _` | '_ \ / _ \ '__| | |    | | '_ \ 
  ____) | (__| | | (_| | |_) |  __/ |    | |____| | |_) |
 |_____/ \___|_|  \__,_| .__/ \___|_|    |______|_|_.__/ 
                       | |                               
                       |_|                               
{Style.RESET_ALL}
{Fore.YELLOW}{'='*60}
{Fore.GREEN}{'Scraper Lib'.center(60)}
{Fore.YELLOW}{'='*60}{Fore.RESET}
"""


# Initialize log buffers at the top of the file
LOG_BUFFER_TERMINAL = []
LOG_BUFFER_FILE = []
LOG_FILE_BUFFER = None


def redraw_logs():
    # Clear the terminal (Windows and ANSI terminals)
    if os.name == 'nt':
        os.system('cls')
    else:
        sys.stdout.write('\033[2J\033[H')
        sys.stdout.flush()
    # Always print the banner first
    print(BANNER)
    for line in LOG_BUFFER_TERMINAL:
        print(line)
    pass

def append_log_to_file(line, filename=None):
    if filename is None:
        filename = LOG_FILE_BUFFER
    if filename is None:
        filename = "console_log.txt"
    with open(filename, "a", encoding="utf-8") as f:
        f.write(strip_ansi(line) + "\n")

def log_info(msg):
    line = f"{Fore.CYAN}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line)
    redraw_logs()

def log_warning(msg):
    line = f"{Fore.YELLOW}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line)
    redraw_logs()

def log_error(msg):
    line = f"{Fore.RED}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line)
    redraw_logs()

def log_section(title):
    line = f"\n{Fore.CYAN}{'='*6} {title} {'='*6}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line)
    redraw_logs()

def log_success(msg):
    # Se for um [DONE], limpe os [TRY] e [FAIL] desse arquivo do buffer do terminal
    if msg.startswith("[DONE] Downloaded"):
        filename = msg.split("Downloaded ")[1].split(" ")[0]
        LOG_BUFFER_TERMINAL[:] = [
            line for line in LOG_BUFFER_TERMINAL
            if filename not in strip_ansi(line) or (
                not strip_ansi(line).startswith("[TRY]") and not strip_ansi(line).startswith("[FAIL]")
            )
        ]
    line = f"{Fore.GREEN}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line)
    redraw_logs()

# Initialize colorama
init(autoreset=True)


# Add file handler to log all output to a file with improved formatting (no ANSI codes)
class PlainFormatter(logging.Formatter):
    COLORS = {
        'INFO': '✔',
        'WARNING': '⚠',
        'ERROR': '✖',
        'CRITICAL': '‼',
        'DEBUG': '•'
    }
    def format(self, record):
        # Remove ANSI codes and add emoji/symbols
        msg = super().format(record)
        for code in ['\033[31m', '\033[32m', '\033[33m', '\033[36m', '\033[39m', '\033[0m', '\033[1m']:
            msg = msg.replace(code, '')
        symbol = self.COLORS.get(record.levelname, '')
        if symbol and not msg.startswith(symbol):
            msg = f"{symbol} {msg}"
        return msg

log_file = "taxi_extraction.log"
file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
file_handler.setLevel(logging.INFO)
file_formatter = PlainFormatter("[%(asctime)s] %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
logging.getLogger().addHandler(file_handler)

# Configurations
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DOWNLOAD_DIR = "tlc_data"
INCREMENTAL = False


# Default headers and user agents (used unless overridden)
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

def get_headers(headers=None):
    """Return headers to be used for requests."""
    return headers.copy() if headers else DEFAULT_HEADERS.copy()

def get_user_agents(user_agents=None):
    """Return user agents list to be used for requests."""
    return user_agents if user_agents else DEFAULT_USER_AGENTS

# LOG_FILE_BUFFER will be set per run, not global
LOG_FILE_BUFFER = None

class DownloadState:
    def __init__(self, state_file="download_state.json", incremental=True):
        self.state_file = state_file
        self._cache = {}  # Local cache to reduce disk access
        if incremental:
            self.load_state()
        else:
            self.generate()

    def generate(self):
        """Initialize a fresh state structure"""
        self._cache = {
            'completed': {},
            'failed': {},
            'delays_success': [],  # Track successful attempt delays
            'delays_failed': [],   # Track failed attempt delays
            'stats': {
                'start_time': datetime.now().isoformat(),
                'last_update': None,
                'total_bytes': 0,
                'delay_stats_success': {  # Statistics for successful delays
                    'min': None,
                    'max': None,
                    'avg': None,
                    'median': None,
                    'percentiles': {}
                },
                'delay_stats_failed': {   # Statistics for failed delays
                    'min': None,
                    'max': None,
                    'avg': None,
                    'median': None,
                    'percentiles': {}
                }
            }
        }
        return self._cache

    def _atomic_file_operation(self, operation):
        """Execute file operations with locking"""
        if not os.path.exists(self.state_file):
            self.generate()
            
        with open(self.state_file, 'a+') as f:
            try:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.seek(0)
                try:
                    content = f.read()
                    self._cache = json.loads(content) if content.strip() else self.generate()
                except json.JSONDecodeError:
                    self._cache = self.generate()
                
                operation()
                
                f.seek(0)
                f.truncate()
                json.dump(self._cache, f, indent=2)
            finally:
                portalocker.unlock(f)

    def add_delay(self, delay, success=True):
        """Record a new delay and update statistics"""
        def _add():
            delay_record = {
                'value': delay,
                'timestamp': datetime.now().isoformat()
            }

            # Store in appropriate list
            key = 'delays_success' if success else 'delays_failed'
            self._cache[key].append(delay_record)

            # Update statistics for the appropriate delay type
            stats_key = f'delay_stats_{"success" if success else "failed"}'
            delays = [d['value'] for d in self._cache[key]]
            
            if delays:
                self._cache['stats'][stats_key] = {
                    'min': min(delays),
                    'max': max(delays),
                    'avg': sum(delays) / len(delays),
                    'median': sorted(delays)[len(delays)//2],
                    'percentiles': {
                        '90th': np.percentile(delays, 90) if len(delays) > 1 else delays[0],
                        '95th': np.percentile(delays, 95) if len(delays) > 1 else delays[0]
                    }
                }

            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_add)

    def load_state(self):
        """Load state from file"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                try:
                    self._cache = json.load(f)
                except json.JSONDecodeError:
                    self.generate()
        else:
            self.generate()

    def save_state(self):
        def _save():
            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_save)

    def get_file_id(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def is_completed(self, url):
        return self.get_file_id(url) in self._cache['completed']

    def add_completed(self, url, filepath, size):
        def _add():
            file_id = self.get_file_id(url)
            self._cache['completed'][file_id] = {
                'url': url,
                'filepath': filepath,
                'size': size,
                'timestamp': datetime.now().isoformat()
            }
            self._cache['stats']['total_bytes'] += size
            self._cache['stats']['last_update'] = datetime.now().isoformat()
            if file_id in self._cache['failed']:
                del self._cache['failed'][file_id]
        self._atomic_file_operation(_add)

    def add_failed(self, url, error):
        def _add():
            file_id = self.get_file_id(url)
            current_retries = self._cache['failed'].get(file_id, {}).get('retries', 0)
            self._cache['failed'][file_id] = {
                'url': url,
                'error': str(error),
                'timestamp': datetime.now().isoformat(),
                'retries': current_retries + 1
            }
            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_add)

    @property
    def state(self):
        return self._cache

def print_banner():
    # No need to print here, handled by redraw_logs
    print(BANNER)
    pass

def strip_ansi(line):
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    return ansi_escape.sub('', line)

def parallel_download_with_ray(file_urls, max_concurrent=8, download_dir="data", state_handler=None):
    random.shuffle(file_urls)
    @ray.remote
    class Queue:
        def __init__(self, items):
            self.items = deque(items)
        def get_next(self):
            if self.items:
                return self.items.popleft()
            return None

    queue = Queue.remote(file_urls)
    active_tasks = []
    results = []
    dynamic_delay = 1.0

    bar_format = f"{Fore.GREEN}{{l_bar}}{Fore.BLUE}{{bar}}{Fore.RESET}{{r_bar}}"

    with tqdm(
        total=len(file_urls),
        desc=f"{Fore.YELLOW}Downloading Files{Fore.RESET}",
        bar_format=bar_format,
        unit="file",
        ncols=100,
        dynamic_ncols=False,
        leave=True,
        position=0,
        ascii=" ░▒█",
        file=sys.stdout,
        colour=None
    ) as pbar:
        tqdm._instances.clear()
        for _ in range(max_concurrent):
            url = ray.get(queue.get_next.remote())
            if url:
                active_tasks.append(download_file_ray.remote(url, download_dir, state_handler))

        while active_tasks:
            ready, active_tasks = ray.wait(active_tasks, timeout=5.0)
            if ready:
                result = ray.get(ready[0])
                # Print logs from the result in the main process
                for level, msg in result.get("logs", []):
                    if level == "info":
                        log_info(msg)
                    elif level == "success":
                        log_success(msg)
                    elif level == "warning":
                        log_warning(msg)
                    elif level == "error":
                        log_error(msg)
                results.append(result)
                pbar.update(1)
                # Use custom log functions for all logs
                if result['status'] == 'success':
                    pass  # Already logged in logs
                elif result['status'] == 'error':
                    pass  # Already logged in logs
                else:
                    log_warning(f"[SKIP] {os.path.basename(result['file'])}")
                if result.get('delay'):
                    dynamic_delay = max(1.0, (dynamic_delay + result['delay']) / 2)
                time.sleep(dynamic_delay * random.uniform(0.9, 1.1))
                next_url = ray.get(queue.get_next.remote())
                if next_url:
                    active_tasks.append(download_file_ray.remote(next_url, download_dir, state_handler))
    return results

@ray.remote
def download_file_ray(file_url, download_dir="data", state_handler=None, headers=None, user_agents=None, base_url=None):
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    INITIAL_DELAY = 1.0  # Initial delay in seconds
    MAX_DELAY = 60.0     # Maximum delay in seconds
    MAX_RETRIES = 5      # Maximum number of retries
    current_delay = INITIAL_DELAY
    temp_filename = None
    logs = []

    def log(msg, level="info"):
        logs.append((level, msg))

    try:
        # Check if file already downloaded
        if state_handler and state_handler.is_completed(file_url):
            log(f"[SKIP] Already downloaded: {file_url}", "info")
            return {"status": "skipped", "file": file_url, "reason": "already_completed", "logs": logs}
        
        log(f"[TRY] Downloading: {file_url}", "info")
        
        # Configure download
        local_filename = os.path.join(download_dir, file_url.split('/')[-1])
        
        # Enhanced headers with referer
        used_headers = get_headers(headers)
        used_user_agents = get_user_agents(user_agents)
        used_headers.update({
            'User-Agent': random.choice(used_user_agents),
            'Referer': base_url if base_url else file_url
        })
        
        # Create unique temp filename
        timestamp = int(time.time())
        temp_filename = f"{local_filename}.{timestamp}.tmp"

        for attempt in range(MAX_RETRIES + 1):  # +1 for the initial attempt
            try:
                # Exponential backoff with jitter
                if attempt > 0:
                    current_delay = min(INITIAL_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
                    current_delay *= random.uniform(0.8, 1.2)  # Add jitter

                # Create a new session for each attempt
                session = requests.Session()
                session.headers.update(used_headers)
                
                # Download with session and timeout
                start_time = time.time()
                with session.get(file_url, stream=True, timeout=(30, 60)) as r:
                    if r.status_code == 403:
                        raise Exception("403 Forbidden")
                    if r.status_code != 200:
                        raise Exception(f"HTTP {r.status_code}")
                    os.makedirs(download_dir, exist_ok=True)
                    with open(temp_filename, 'wb') as f:
                        downloaded = 0
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                    os.replace(temp_filename, local_filename)
                    download_time = time.time() - start_time
                    if state_handler:
                        state_handler.add_completed(file_url, local_filename, downloaded)
                        state_handler.add_delay(download_time, success=True)
                    log(f"[DONE] Downloaded {os.path.basename(local_filename)} "
                        f"({downloaded/1024/1024:.2f} MB) in {download_time:.2f}s", "success")
                    return {
                        "status": "success",
                        "file": local_filename,
                        "size": downloaded,
                        "time": download_time,
                        "speed": downloaded / (download_time + 1e-6),
                        "attempts": attempt + 1,
                        "logs": logs
                    }
            except Exception as e:
                if state_handler:
                    state_handler.add_failed(file_url, str(e))
                    state_handler.add_delay(current_delay, success=False)
                # Combine FAIL and WAIT logs
                if attempt == MAX_RETRIES:
                    log(f"[FAIL] Attempt {attempt + 1} for {file_url}: {str(e)}", "error")
                    raise
                else:
                    log(f"[FAIL] Attempt {attempt + 1} for {file_url}: {str(e)} | Waiting {current_delay:.1f}s before retry", "error")
                    time.sleep(current_delay)
    except Exception as e:
        if state_handler:
            state_handler.add_failed(file_url, str(e))
        log(f"[FAIL] Failed to download {file_url} after {MAX_RETRIES} attempts: {str(e)}", "error")
        return {
            "status": "error",
            "file": file_url,
            "error": str(e),
            "retry_count": state_handler.state['failed'].get(
                state_handler.get_file_id(file_url), {}).get('retries', 0) if state_handler else 0,
            "delay": current_delay,
            "attempts": attempt + 1 if 'attempt' in locals() else 0,
            "logs": logs
        }
    finally:
        if temp_filename and os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
            except Exception:
                pass

def download_file(file_url, download_dir="data", state_handler=None, headers=None, user_agents=None, base_url=None):
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    INITIAL_DELAY = 1.0
    MAX_DELAY = 60.0
    MAX_RETRIES = 5
    current_delay = INITIAL_DELAY
    temp_filename = None

    try:
        # Check if file already downloaded
        if state_handler and state_handler.is_completed(file_url):
            log_info(f"[SKIP] Already downloaded: {file_url}")
            return {"status": "skipped", "file": file_url, "reason": "already_completed"}

        log_info(f"[TRY] Downloading: {file_url}")

        local_filename = os.path.join(download_dir, file_url.split('/')[-1])
        used_headers = get_headers(headers)
        used_user_agents = get_user_agents(user_agents)
        used_headers.update({
            'User-Agent': random.choice(used_user_agents),
            'Referer': base_url if base_url else file_url
        })

        timestamp = int(time.time())
        temp_filename = f"{local_filename}.{timestamp}.tmp"

        for attempt in range(MAX_RETRIES + 1):
            try:
                # Exponential backoff with jitter
                if attempt > 0:
                    current_delay = min(INITIAL_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
                    current_delay *= random.uniform(0.8, 1.2)
                    log_info(f"[WAIT] Attempt {attempt + 1}: Waiting {current_delay:.1f}s")
                    time.sleep(current_delay)

                # Create a new session for each attempt
                session = requests.Session()
                session.headers.update(used_headers)

                start_time = time.time()
                with session.get(file_url, stream=True, timeout=(30, 60)) as r:
                    if r.status_code == 403:
                        raise Exception("403 Forbidden - Potential blocking detected")
                    r.raise_for_status()

                    if state_handler:
                        state_handler.add_delay(current_delay, success=True)

                    if r.url != file_url:
                        log_info(f"[REDIR] {file_url} → {r.url}")

                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0

                    if os.path.isfile(temp_filename):
                        os.remove(temp_filename)

                    # Download file without individual progress bar
                    with open(temp_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            downloaded += len(chunk)

                    if total_size > 0 and downloaded != total_size:
                        raise Exception(f"Size mismatch: expected {total_size}, got {downloaded}")

                if os.path.exists(local_filename):
                    os.remove(local_filename)

                os.rename(temp_filename, local_filename)
                download_time = time.time() - start_time

                if state_handler:
                    state_handler.add_completed(file_url, local_filename, downloaded)

                log_success(f"[DONE] Downloaded {os.path.basename(local_filename)} "
                            f"({downloaded/1024/1024:.2f} MB) in {download_time:.2f}s")

                return {
                    "status": "success",
                    "file": local_filename,
                    "size": downloaded,
                    "time": download_time,
                    "speed": downloaded / (download_time + 1e-6),
                    "attempts": attempt + 1
                }

            except Exception as e:
                if state_handler:
                    state_handler.add_delay(current_delay, success=False)

                if "403" in str(e):
                    # Rotate user agent for 403 errors
                    used_headers['User-Agent'] = random.choice(used_user_agents)
                    log_warning("[403] Rotating User-Agent and retrying...")

                if attempt >= MAX_RETRIES:
                    raise

                error_msg = str(e)
                if "SSL" in error_msg:
                    error_msg = "SSL Error - retrying with different parameters"
                elif "Connection" in error_msg:
                    error_msg = "Connection Error - retrying"

                log_warning(f"[RETRY] Attempt {attempt + 1} failed: {error_msg}")

    except Exception as e:
        if state_handler:
            state_handler.add_failed(file_url, e)

        log_error(f"[FAIL] Failed to download {file_url} after {MAX_RETRIES} attempts: {str(e)}")

        return {
            "status": "error",
            "file": file_url,
            "error": str(e),
            "retry_count": state_handler.state['failed'].get(
                state_handler.get_file_id(file_url), {}).get('retries', 0) if state_handler else 0,
            "delay": current_delay,
            "attempts": attempt + 1
        }
    finally:
        if temp_filename and os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
            except Exception as e:
                log_warning(f"[WARN] Could not remove temp file {temp_filename}: {e}")

def generate_report(results, state, report_prefix="download_report"):
    """Generate comprehensive statistics report with visualization"""
    # Get all delay data
    delays_success = state.state.get('delays_success', [])
    delays_failed = state.state.get('delays_failed', [])
    
    # Prepare delay statistics
    success_delays = [d['value'] for d in delays_success]
    failed_delays = [d['value'] for d in delays_failed]
    
    # Calculate statistics for successful delays
    success_stats = {}
    if success_delays:
        success_stats = {
            'min': min(success_delays),
            'max': max(success_delays),
            'avg': sum(success_delays) / len(success_delays),
            'median': sorted(success_delays)[len(success_delays)//2],
            'percentiles': {
                '90th': np.percentile(success_delays, 90) if len(success_delays) > 1 else success_delays[0],
                '95th': np.percentile(success_delays, 95) if len(success_delays) > 1 else success_delays[0]
            }
        }

    # Calculate statistics for failed delays
    failed_stats = {}
    if failed_delays:
        failed_stats = {
            'min': min(failed_delays),
            'max': max(failed_delays),
            'avg': sum(failed_delays) / len(failed_delays),
            'median': sorted(failed_delays)[len(failed_delays)//2],
            'percentiles': {
                '90th': np.percentile(failed_delays, 90) if len(failed_delays) > 1 else failed_delays[0],
                '95th': np.percentile(failed_delays, 95) if len(failed_delays) > 1 else failed_delays[0]
            }
        }

    # --- CORREÇÃO DO CÁLCULO DE DATETIME ---
    stats = state.state['stats']
    start_time_str = stats.get('start_time')
    last_update_str = stats.get('last_update') or datetime.now().isoformat()
    try:
        start_dt = datetime.fromisoformat(start_time_str) if start_time_str else datetime.now()
        last_update_dt = datetime.fromisoformat(last_update_str) if last_update_str else datetime.now()
        elapsed = (last_update_dt - start_dt).total_seconds()
        time_elapsed_str = str(last_update_dt - start_dt)
    except Exception:
        elapsed = 1
        time_elapsed_str = "unknown"
    throughput_gbps = stats['total_bytes'] * 8 / (1024**3) / max(1, elapsed)

    # Create report structure
    report = {
        "summary": {
            "total_files": len(results),
            "success": sum(1 for r in results if r['status'] == 'success'),
            "skipped": sum(1 for r in results if r['status'] == 'skipped'),
            "failed": sum(1 for r in results if r['status'] == 'error'),
            "total_bytes": stats['total_bytes'],
            "throughput_gbps": throughput_gbps,
            "time_elapsed": time_elapsed_str
        },
        "delays": {
            "success": success_stats,
            "failed": failed_stats
        },
        "failed_downloads": [
            {"url": r['file'], "error": r['error'], "retries": r.get('retry_count', 0)}
            for r in results if r['status'] == 'error'
        ]
    }

    # Generate visualizations if matplotlib is available
    try:
        # Create figure for successful delays
        if success_delays:
            plt.figure(figsize=(12, 6))
            
            # Time-series plot
            plt.subplot(1, 2, 1)
            plt.plot([datetime.fromisoformat(d['timestamp']) for d in delays_success], 
                    success_delays, 'b.', alpha=0.5, label='Sucesso')
            plt.title('Evolução dos Delays (Sucessos)')
            plt.xlabel('Tempo')
            plt.ylabel('Delay (segundos)')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Distribution plot
            plt.subplot(1, 2, 2)
            plt.hist(success_delays, bins=20, color='g', alpha=0.7, label='Sucesso')
            plt.title('Distribuição dos Delays (Sucessos)')
            plt.xlabel('Delay (segundos)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            plt.tight_layout()
            success_plot_filename = 'delay_success_analysis.png'
            plt.savefig(success_plot_filename, dpi=300)
            plt.close()
            report['success_delay_plot'] = success_plot_filename

        # Create figure for failed delays
        if failed_delays:
            plt.figure(figsize=(8, 5))
            plt.hist(failed_delays, bins=20, color='r', alpha=0.7, label='Falhas')
            plt.title('Distribuição dos Delays (Falhas)')
            plt.xlabel('Delay (segundos)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            plt.tight_layout()
            failed_plot_filename = 'delay_failed_analysis.png'
            plt.savefig(failed_plot_filename, dpi=300)
            plt.close()
            report['failed_delay_plot'] = failed_plot_filename

    except ImportError:
        log_warning("Matplotlib não disponível - visualizações não geradas")

    # Save JSON report
    report_filename = f"{report_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, 'w') as f:
        json.dump(report, f, indent=2)

    # Log summary to console
    log_section("DOWNLOAD SUMMARY")
    log_success(f"✔ Success: {report['summary']['success']}/{report['summary']['total_files']} ({report['summary']['success']/report['summary']['total_files']:.1%})")
    log_warning(f"↻ Skipped: {report['summary']['skipped']}")
    log_error(f"✖ Failed: {report['summary']['failed']}")
    log_info(f"Total data: {report['summary']['total_bytes']/(1024**3):.2f} GB")
    log_info(f"Throughput: {report['summary']['throughput_gbps']:.2f} Gbps")
    log_info(f"Time elapsed: {report['summary']['time_elapsed']}")

    # Log visualization info
    if 'success_delay_plot' in report:
        log_info(f"Success delay plot saved to: {report['success_delay_plot']}")
    if 'failed_delay_plot' in report:
        log_info(f"Failed delay plot saved to: {report['failed_delay_plot']}")

    log_info(f"Full report saved to: {report_filename}")

def setup_download_dir():
    """Create download directory if it doesn't exist"""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        log_success(f"[DIR] Created directory: {DOWNLOAD_DIR}")
    else:
        log_info(f"[DIR] Directory exists: {DOWNLOAD_DIR}")

def extract_file_links(html_content, base_url, patterns):
    """Extract file download links from the page using given patterns"""
    soup = BeautifulSoup(html_content, 'html.parser')
    file_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower()
        if any(pattern in href for pattern in patterns):
            absolute_url = urljoin(base_url, a_tag['href'])
            file_links.append(absolute_url)
    return file_links

def get_page_content(url, user_agents=None, headers=None):
    """Get HTML content of the page with better header handling"""
    headers = headers.copy() if headers else {}
    if user_agents:
        headers['User-Agent'] = random.choice(user_agents)
    session = requests.Session()
    session.headers.update(headers)
    try:
        response = session.get(url, timeout=(10, 30))
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        log_error(f"[ERROR] Error accessing page: {e}")
        return None

def scraper(
    base_url,
    file_patterns,
    download_dir="data",
    incremental=False,
    max_files=None,
    max_concurrent=None,
    headers=None,
    user_agents=None,
    state_file="download_state.json",
    log_file="taxi_extraction.log",
    console_log_file="console_log.txt",
    report_prefix="download_report"
):
    global LOG_FILE_BUFFER
    LOG_FILE_BUFFER = console_log_file

    # Use internal defaults unless overridden
    if headers is None:
        headers = DEFAULT_HEADERS
    if user_agents is None:
        user_agents = DEFAULT_USER_AGENTS

    # Set up logging file handler with the provided log_file path
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    file_handler.setLevel(logging.INFO)
    file_formatter = PlainFormatter("[%(asctime)s] %(levellevelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    logging.getLogger().addHandler(file_handler)

    print_banner()
    ray_cpu = min(max_concurrent, os.cpu_count())
    # ---- INÍCIO DO RAY ----
    ray.shutdown()
    ray.init(
        num_cpus=ray_cpu,
        include_dashboard=False,
        logging_level=logging.ERROR,
        ignore_reinit_error=True,
    )
    # Agora é seguro acessar recursos do Ray
    log_info(f"Ray version: {ray.__version__}")
    log_info(f"Available CPUs: {ray.available_resources()['CPU']}")
    log_info(f"Using CPUs: {ray_cpu}")

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        log_success(f"[DIR] Created directory: {download_dir}")
    else:
        log_info(f"[DIR] Directory exists: {download_dir}")
    log_section("PHASE 1: Collecting file links")
    html_content = get_page_content(base_url, user_agents, headers)
    if not html_content:
        log_error("Failed to access main page. Check connection and URL.")
        ray.shutdown()
        return
    file_links = extract_file_links(html_content, base_url, file_patterns)
    if max_files:
        file_links = file_links[:max_files]
    if not file_links:
        log_error("No valid links found. Check extraction pattern.")
        ray.shutdown()
        return
    log_success(f"Found {len(file_links)} files to download")
    log_section("PHASE 2: Parallel downloads with Ray")
    start_time = time.time()
    if max_concurrent is None:
        max_concurrent = min(16, int(ray.available_resources()['CPU'] * 1.5))
    log_info(f"Using {max_concurrent} parallel workers")
    state_handler = DownloadState(state_file=state_file, incremental=incremental)
    results = parallel_download_with_ray(file_links, max_concurrent, download_dir, state_handler)
    log_section("PHASE 3: Results analysis")
    success = sum(1 for r in results if r['status'] == 'success')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    errors = sum(1 for r in results if r['status'] == 'error')
    total_size_gb = sum(
        r['size'] for r in results 
        if r['status'] == 'success' and 'size' in r
    ) / (1024**3)
    total_time = time.time() - start_time
    log_section("FINAL SUMMARY")
    log_success(f"✔ Downloaded: {success}")
    log_warning(f"↻ Skipped: {skipped}")
    log_error(f"✖ Errors: {errors}")
    log_info(f"Total data: {total_size_gb:.2f} GB")
    log_info(f"Total time: {total_time:.2f} seconds")
    log_info(f"Throughput: {total_size_gb/(total_time/60):.2f} GB/min")
    generate_report(results, state_handler, report_prefix=report_prefix)
    ray.shutdown()

def cli():
    import argparse
    parser = argparse.ArgumentParser(description="Generic parallel file downloader")
    parser.add_argument("--url", required=True, help="Base URL to scrape for files")
    parser.add_argument("--patterns", nargs="+", required=True, help="List of file patterns to match (e.g. .csv .zip)")
    parser.add_argument("--dir", default="data", help="Download directory")
    parser.add_argument("--incremental", action="store_true", help="Enable incremental download state")
    parser.add_argument("--max-files", type=int, default=None, help="Limit number of files to download")
    parser.add_argument("--max-concurrent", type=int, default=None, help="Max parallel downloads")
    parser.add_argument("--state-file", default="download_state.json", help="Path for download state file")
    parser.add_argument("--log-file", default="taxi_extraction.log", help="Path for main log file")
    parser.add_argument("--console-log-file", default="console_log.txt", help="Path for console log file")
    parser.add_argument("--report-prefix", default="download_report", help="Prefix for report files")
    parser.add_argument("--headers", type=str, default=None, help="Path to JSON file with custom headers")
    parser.add_argument("--user-agents", type=str, default=None, help="Path to text file with custom user agents (one per line)")
    args = parser.parse_args()

    # Optionally load custom headers and user agents from files
    headers = None
    user_agents = None
    if args.headers:
        with open(args.headers, "r", encoding="utf-8") as f:
            headers = json.load(f)
    if args.user_agents:
        with open(args.user_agents, "r", encoding="utf-8") as f:
            user_agents = [line.strip() for line in f if line.strip()]

    log_info("Starting Ray cluster...")
    ray.shutdown()
    ray.init(
        num_cpus=min(16, os.cpu_count()),
        include_dashboard=False,
        logging_level=logging.ERROR,
        ignore_reinit_error=True,
    )
    try:
        scraper(
            base_url=args.url,
            file_patterns=args.patterns,
            download_dir=args.dir,
            incremental=args.incremental,
            max_files=args.max_files,
            max_concurrent=args.max_concurrent,
            headers=headers,
            user_agents=user_agents,
            state_file=args.state_file,
            log_file=args.log_file,
            console_log_file=args.console_log_file,
            report_prefix=args.report_prefix,
        )
    finally:
        ray.shutdown()
        log_info("Ray cluster shutdown")