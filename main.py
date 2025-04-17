import requests
import logging
from bs4 import BeautifulSoup
import os
os.environ['RAY_DEDUP_LOGS'] = "0"
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

# Initialize colorama
init(autoreset=True)

# Configure logging for aesthetic output
logging.basicConfig(
    level=logging.INFO,
    format=f"{Fore.LIGHTBLACK_EX}[%(asctime)s]{Fore.RESET} {Fore.CYAN}%(levelname)s{Fore.RESET} {Fore.WHITE}%(message)s{Fore.RESET}",
    datefmt='%H:%M:%S'
)

# Add file handler to log all output to a file
log_file = "taxi_extraction.log"
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
logging.getLogger().addHandler(file_handler)

# Configurations
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DOWNLOAD_DIR = "tlc_data"
INCREMENTAL = False

# Enhanced headers with referer and more realistic user agents
HEADERS = {
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

USER_AGENTS = [
    # Chrome (Windows/Mac/Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Firefox (Windows/Mac/Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/120.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

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
    banner = f"""
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
    print(banner)

def log_info(msg):
    logging.info(f"{Fore.CYAN}{msg}{Fore.RESET}")

def log_success(msg):
    logging.info(f"{Fore.GREEN}{msg}{Fore.RESET}")

def log_warning(msg):
    logging.warning(f"{Fore.YELLOW}{msg}{Fore.RESET}")

def log_error(msg):
    logging.error(f"{Fore.RED}{msg}{Fore.RESET}")

def log_section(title):
    print(f"\n{Fore.CYAN}{'='*6} {title} {'='*6}{Fore.RESET}")

def parallel_download_with_ray(file_urls, max_concurrent=8, download_dir="data", state_handler=None):
    """Distributed queue version to avoid duplication"""
    random.shuffle(file_urls)
    @ray.remote
    class Queue:
        def __init__(self, items):
            self.items = deque(items)
        def get_next(self):
            if self.items:
                return self.items.popleft()
            return None
        def get_size(self):
            return len(self.items)

    queue = Queue.remote(file_urls)
    active_tasks = []
    results = []
    dynamic_delay = 1.0

    # Fixed progress bar (disable dynamic postfix to avoid reprinting)
    bar_format = f"{Fore.GREEN}{{l_bar}}{Fore.BLUE}{{bar}}{Fore.RESET}{{r_bar}}"

    # Pin the progress bar to the top of the output
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
        file=sys.stdout,  # Ensure it always writes to the top
        colour=None
    ) as pbar:
        tqdm._instances.clear()  # Ensure only one bar is active and at the top
        for _ in range(max_concurrent):
            url = ray.get(queue.get_next.remote())
            if url:
                active_tasks.append(download_file_ray.remote(url, download_dir, state_handler))

        while active_tasks:
            ready, active_tasks = ray.wait(active_tasks, timeout=5.0)
            if ready:
                result = ray.get(ready[0])
                results.append(result)
                pbar.update(1)
                # Print status only once per file, not as postfix, and do NOT refresh the bar
                if result['status'] == 'success':
                    print(f"{Fore.GREEN}[DONE]{Fore.RESET} Downloaded {Fore.BLUE}{os.path.basename(result['file'])}{Fore.RESET} "
                          f"({result.get('size', 0)/1024/1024:.2f} MB) in {result.get('time', 0):.2f}s")
                elif result['status'] == 'error':
                    print(f"{Fore.RED}[FAIL]{Fore.RESET} {os.path.basename(result['file'])} - {result.get('error', '')}")
                else:
                    print(f"{Fore.YELLOW}[SKIP]{Fore.RESET} {os.path.basename(result['file'])}")
                if result.get('delay'):
                    dynamic_delay = max(1.0, (dynamic_delay + result['delay']) / 2)
                time.sleep(dynamic_delay * random.uniform(0.9, 1.1))
                next_url = ray.get(queue.get_next.remote())
                if next_url:
                    active_tasks.append(download_file_ray.remote(next_url, download_dir, state_handler))
    return results

@ray.remote
def download_file_ray(file_url, download_dir="data", state_handler=None):
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    INITIAL_DELAY = 1.0  # Initial delay in seconds
    MAX_DELAY = 60.0     # Maximum delay in seconds
    MAX_RETRIES = 5      # Maximum number of retries
    current_delay = INITIAL_DELAY
    temp_filename = None
    
    try:
        # Check if file already downloaded
        if state_handler and state_handler.is_completed(file_url):
            print(f"{Fore.CYAN}[SKIP]{Fore.RESET} Already downloaded: {Fore.BLUE}{file_url}{Fore.RESET}")
            return {"status": "skipped", "file": file_url, "reason": "already_completed"}
        
        print(f"{Fore.YELLOW}[TRY]{Fore.RESET} Downloading: {Fore.BLUE}{file_url}{Fore.RESET}")
        
        # Configure download
        local_filename = os.path.join(download_dir, file_url.split('/')[-1])
        
        # Enhanced headers with referer
        headers = HEADERS.copy()
        headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': BASE_URL
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
                    print(f"{Fore.MAGENTA}[WAIT]{Fore.RESET} Attempt {attempt + 1}: Waiting {Fore.GREEN}{current_delay:.1f}s{Fore.RESET}")
                    time.sleep(current_delay)

                # Create a new session for each attempt
                session = requests.Session()
                session.headers.update(headers)
                
                # Download with session and timeout
                start_time = time.time()
                with session.get(file_url, stream=True, timeout=(30, 60)) as r:
                    if r.status_code == 403:
                        raise Exception("403 Forbidden - Potential blocking detected")
                    r.raise_for_status()
                    
                    if state_handler:
                        state_handler.add_delay(current_delay, success=True)
                    
                    if r.url != file_url:
                        print(f"{Fore.CYAN}[REDIR]{Fore.RESET} {file_url} → {r.url}")

                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0

                    # Remove any existing temp file
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

                # Remove existing file if it exists before rename
                if os.path.exists(local_filename):
                    os.remove(local_filename)
                
                # Commit download
                os.rename(temp_filename, local_filename)
                download_time = time.time() - start_time

                # Update state
                if state_handler:
                    state_handler.add_completed(file_url, local_filename, downloaded)

                print(f"{Fore.GREEN}[DONE]{Fore.RESET} Downloaded {Fore.BLUE}{os.path.basename(local_filename)}{Fore.RESET} "
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
                    headers['User-Agent'] = random.choice(USER_AGENTS)
                    print(f"{Fore.RED}[403]{Fore.RESET} Rotating User-Agent and retrying...")
                
                if attempt >= MAX_RETRIES:
                    raise
                
                error_msg = str(e)
                if "SSL" in error_msg:
                    error_msg = "SSL Error - retrying with different parameters"
                elif "Connection" in error_msg:
                    error_msg = "Connection Error - retrying"
                
                print(f"{Fore.YELLOW}[RETRY]{Fore.RESET} Attempt {attempt + 1} failed: {Fore.RED}{error_msg}{Fore.RESET}")

    except Exception as e:
        # Record failure
        if state_handler:
            state_handler.add_failed(file_url, e)
        
        print(f"{Fore.RED}[FAIL]{Fore.RESET} Failed to download {Fore.BLUE}{file_url}{Fore.RESET} after {MAX_RETRIES} attempts: "
              f"{Fore.RED}{str(e)}{Fore.RESET}")
        
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
                print(f"{Fore.YELLOW}[WARN]{Fore.RESET} Could not remove temp file {temp_filename}: {e}")

def download_file(file_url, download_dir="data", state_handler=None):
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    INITIAL_DELAY = 1.0
    MAX_DELAY = 60.0
    MAX_RETRIES = 5
    current_delay = INITIAL_DELAY
    temp_filename = None

    try:
        # Check if file already downloaded
        if state_handler and state_handler.is_completed(file_url):
            print(f"{Fore.CYAN}[SKIP]{Fore.RESET} Already downloaded: {Fore.BLUE}{file_url}{Fore.RESET}")
            return {"status": "skipped", "file": file_url, "reason": "already_completed"}

        print(f"{Fore.YELLOW}[TRY]{Fore.RESET} Downloading: {Fore.BLUE}{file_url}{Fore.RESET}")

        local_filename = os.path.join(download_dir, file_url.split('/')[-1])
        headers = HEADERS.copy()
        headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': BASE_URL
        })

        timestamp = int(time.time())
        temp_filename = f"{local_filename}.{timestamp}.tmp"

        for attempt in range(MAX_RETRIES + 1):
            try:
                # Exponential backoff with jitter
                if attempt > 0:
                    current_delay = min(INITIAL_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
                    current_delay *= random.uniform(0.8, 1.2)
                    print(f"{Fore.MAGENTA}[WAIT]{Fore.RESET} Attempt {attempt + 1}: Waiting {Fore.GREEN}{current_delay:.1f}s{Fore.RESET}")
                    time.sleep(current_delay)

                # Create a new session for each attempt
                session = requests.Session()
                session.headers.update(headers)

                start_time = time.time()
                with session.get(file_url, stream=True, timeout=(30, 60)) as r:
                    if r.status_code == 403:
                        raise Exception("403 Forbidden - Potential blocking detected")
                    r.raise_for_status()

                    if state_handler:
                        state_handler.add_delay(current_delay, success=True)

                    if r.url != file_url:
                        print(f"{Fore.CYAN}[REDIR]{Fore.RESET} {file_url} → {r.url}")

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

                print(f"{Fore.GREEN}[DONE]{Fore.RESET} Downloaded {Fore.BLUE}{os.path.basename(local_filename)}{Fore.RESET} "
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
                    headers['User-Agent'] = random.choice(USER_AGENTS)
                    print(f"{Fore.RED}[403]{Fore.RESET} Rotating User-Agent and retrying...")

                if attempt >= MAX_RETRIES:
                    raise

                error_msg = str(e)
                if "SSL" in error_msg:
                    error_msg = "SSL Error - retrying with different parameters"
                elif "Connection" in error_msg:
                    error_msg = "Connection Error - retrying"

                print(f"{Fore.YELLOW}[RETRY]{Fore.RESET} Attempt {attempt + 1} failed: {Fore.RED}{error_msg}{Fore.RESET}")

    except Exception as e:
        if state_handler:
            state_handler.add_failed(file_url, e)

        print(f"{Fore.RED}[FAIL]{Fore.RESET} Failed to download {Fore.BLUE}{file_url}{Fore.RESET} after {MAX_RETRIES} attempts: "
              f"{Fore.RED}{str(e)}{Fore.RESET}")

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
                print(f"{Fore.YELLOW}[WARN]{Fore.RESET} Could not remove temp file {temp_filename}: {e}")

def generate_report(results, state):
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

    # Create report structure
    report = {
        "summary": {
            "total_files": len(results),
            "success": sum(1 for r in results if r['status'] == 'success'),
            "skipped": sum(1 for r in results if r['status'] == 'skipped'),
            "failed": sum(1 for r in results if r['status'] == 'error'),
            "total_bytes": state.state['stats']['total_bytes'],
            "throughput_gbps": state.state['stats']['total_bytes'] * 8 / (1024**3) / max(
                1, (datetime.fromisoformat(state.state['stats']['last_update']) - 
                datetime.fromisoformat(state.state['stats']['start_time'])).total_seconds()),
            "time_elapsed": str(datetime.fromisoformat(state.state['stats']['last_update']) - 
                              datetime.fromisoformat(state.state['stats']['start_time']))
        },
        "delays": {
            "success": success_stats,
            "failed": failed_stats
        },
        "failed_downloads": [
            {"url": r['file'], "error": r['error'], "retries": r.get('retry_count', 0)}
            for r in results if r['status'] == 'error'
        ][:100]
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
        print("Matplotlib não disponível - visualizações não geradas")

    # Save JSON report
    report_filename = f"download_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary to console
    print(f"\n{Fore.CYAN}=== {Style.BRIGHT}DOWNLOAD SUMMARY{Style.NORMAL} ===")
    print(f"{Fore.GREEN}✔ Success: {report['summary']['success']}/{report['summary']['total_files']} ({report['summary']['success']/report['summary']['total_files']:.1%})")
    print(f"{Fore.YELLOW}↻ Skipped: {report['summary']['skipped']}")
    print(f"{Fore.RED}✖ Failed: {report['summary']['failed']}")
    print(f"{Fore.BLUE}Total data: {report['summary']['total_bytes']/(1024**3):.2f} GB")
    print(f"Throughput: {report['summary']['throughput_gbps']:.2f} Gbps")
    print(f"Time elapsed: {report['summary']['time_elapsed']}{Fore.RESET}")

    # Print visualization info
    if 'success_delay_plot' in report:
        print(f"\n{Fore.CYAN}Success delay plot saved to: {report['success_delay_plot']}{Fore.RESET}")
    if 'failed_delay_plot' in report:
        print(f"{Fore.CYAN}Failed delay plot saved to: {report['failed_delay_plot']}{Fore.RESET}")

    print(f"\n{Fore.CYAN}Full report saved to: {report_filename}{Fore.RESET}")

def setup_download_dir():
    """Create download directory if it doesn't exist"""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"{Fore.GREEN}[DIR]{Fore.RESET} Created directory: {Fore.BLUE}{DOWNLOAD_DIR}{Fore.RESET}")
    else:
        print(f"{Fore.CYAN}[DIR]{Fore.RESET} Directory exists: {Fore.BLUE}{DOWNLOAD_DIR}{Fore.RESET}")

def get_page_content(url):
    """Get HTML content of the page with better header handling"""
    headers = HEADERS.copy()
    headers['User-Agent'] = random.choice(USER_AGENTS)
    
    session = requests.Session()
    session.headers.update(headers)
    
    try:
        response = session.get(url, timeout=(10, 30))
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"{Fore.RED}[ERROR]{Fore.RESET} Error accessing page: {e}")
        return None

def extract_file_links(html_content):
    """Extract file download links from the page"""
    soup = BeautifulSoup(html_content, 'html.parser')
    file_links = []
    
    # Search for links containing data file patterns
    patterns = ['.parquet', '.csv', '.zip', '.gz', 'trip data']
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower()
        if any(pattern in href for pattern in patterns):
            absolute_url = urljoin(BASE_URL, a_tag['href'])
            file_links.append(absolute_url)
    
    return file_links

def main():
    print_banner()
    log_info(f"Ray version: {ray.__version__}")
    log_info(f"Available CPUs: {ray.available_resources()['CPU']}")
    setup_download_dir()
    log_section("PHASE 1: Collecting file links")
    html_content = get_page_content(BASE_URL)
    if not html_content:
        log_error("Failed to access main page. Check connection and URL.")
        return
    file_links = extract_file_links(html_content)
    if not file_links:
        log_error("No valid links found. Check extraction pattern.")
        return
    log_success(f"Found {len(file_links)} files to download")
    log_section("PHASE 2: Parallel downloads with Ray")
    start_time = time.time()
    max_concurrent = min(8, int(ray.available_resources()['CPU'] * 1.5))
    log_info(f"Using {max_concurrent} parallel workers")
    state_handler = DownloadState(incremental=INCREMENTAL)
    results = parallel_download_with_ray(file_links, max_concurrent, "tlc_data", state_handler)
    log_section("PHASE 3: Results analysis")
    success = sum(1 for r in results if r['status'] == 'success')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    errors = sum(1 for r in results if r['status'] == 'error')
    total_size_gb = sum(
        r['size'] for r in results 
        if r['status'] == 'success' and 'size' in r
    ) / (1024**3)
    total_time = time.time() - start_time
    print(f"\n{Fore.CYAN}=== FINAL SUMMARY ===")
    print(f"{Fore.GREEN}✔ Downloaded: {success}")
    print(f"{Fore.YELLOW}↻ Skipped: {skipped}")
    print(f"{Fore.RED}✖ Errors: {errors}")
    print(f"{Fore.BLUE}Total data: {total_size_gb:.2f} GB")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Throughput: {total_size_gb/(total_time/60):.2f} GB/min{Fore.RESET}")
    generate_report(results, state_handler)

if __name__ == "__main__":
    log_info("Starting Ray cluster...")
    ray.shutdown()
    ray.init(
        num_cpus=min(8, os.cpu_count()),
        include_dashboard=False,
        logging_level=logging.WARNING,
        ignore_reinit_error=True
    )
    try:
        main()
    finally:
        ray.shutdown()
        log_info("Ray cluster shutdown")