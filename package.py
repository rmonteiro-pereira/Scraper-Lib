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
import matplotlib.pyplot as plt 
import ray
from collections import deque
from colorama import init, Fore, Style
from CustomLogger import CustomLogger
from state import DownloadState
import numpy as np
from datetime import datetime

def parallel_download_with_ray(
    file_urls, headers, user_agents, max_concurrent=8, download_dir="data", state_handler=None,
    logger=None, disable_progress_bar=False  # <-- NEW ARG
):
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

    progress_bar = tqdm(
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
        colour=None,
        disable=disable_progress_bar  # <-- Disable if requested
    )

    with progress_bar as pbar:
        tqdm._instances.clear()
        for _ in range(max_concurrent):
            url = ray.get(queue.get_next.remote())
            if url:
                active_tasks.append(download_file_ray.remote(url, headers, user_agents, download_dir, state_handler))

        while active_tasks:
            ready, active_tasks = ray.wait(active_tasks, timeout=5.0)
            if ready:
                result = ray.get(ready[0])
                for level, msg in result.get("logs", []):
                    if logger:
                        if level == "info":
                            logger.info(msg)
                        elif level == "success":
                            logger.success(msg)
                        elif level == "warning":
                            logger.warning(msg)
                        elif level == "error":
                            logger.error(msg)
                results.append(result)
                pbar.update(1)
                if result.get('delay'):
                    dynamic_delay = max(1.0, (dynamic_delay + result['delay']) / 2)
                time.sleep(dynamic_delay * random.uniform(0.9, 1.1))
                next_url = ray.get(queue.get_next.remote())
                if next_url:
                    active_tasks.append(download_file_ray.remote(next_url, headers, user_agents, download_dir, state_handler))
    return results

@ray.remote
def download_file_ray(file_url, headers, user_agents, download_dir="data", state_handler=None, base_url=None):
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
        if state_handler and state_handler.is_completed(file_url):
            log(f"[SKIP] Already downloaded: {file_url}", "info")
            return {"status": "skipped", "file": file_url, "reason": "already_completed", "logs": logs}
        
        log(f"[TRY] Downloading: {file_url}", "info")
        local_filename = os.path.join(download_dir, file_url.split('/')[-1])
        used_headers = headers
        used_user_agents = user_agents
        used_headers.update({
            'User-Agent': random.choice(used_user_agents),
            'Referer': base_url if base_url else file_url
        })
        timestamp = int(time.time())
        temp_filename = f"{local_filename}.{timestamp}.tmp"

        for attempt in range(MAX_RETRIES + 1):
            try:
                if attempt > 0:
                    current_delay = min(INITIAL_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
                    current_delay *= random.uniform(0.8, 1.2)
                session = requests.Session()
                session.headers.update(used_headers)
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

def generate_report(results, state, report_prefix="download_report", logger=None, output_dir="."):
    # Ensure output directory exists
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    delays_success = state.state.get('delays_success', [])
    delays_failed = state.state.get('delays_failed', [])
    success_delays = [d['value'] for d in delays_success]
    failed_delays = [d['value'] for d in delays_failed]
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
    try:
        if success_delays:
            plt.figure(figsize=(12, 6))
            plt.subplot(1, 2, 1)
            plt.plot([datetime.fromisoformat(d['timestamp']) for d in delays_success], 
                    success_delays, 'b.', alpha=0.5, label='Sucesso')
            plt.title('Evolução dos Delays (Sucessos)')
            plt.xlabel('Tempo')
            plt.ylabel('Delay (segundos)')
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.subplot(1, 2, 2)
            plt.hist(success_delays, bins=20, color='g', alpha=0.7, label='Sucesso')
            plt.title('Distribuição dos Delays (Sucessos)')
            plt.xlabel('Delay (segundos)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.tight_layout()
            success_plot_filename = os.path.join(output_dir, 'delay_success_analysis.png')
            plt.savefig(success_plot_filename, dpi=300)
            plt.close()
            report['success_delay_plot'] = success_plot_filename
        if failed_delays:
            plt.figure(figsize=(8, 5))
            plt.hist(failed_delays, bins=20, color='r', alpha=0.7, label='Falhas')
            plt.title('Distribuição dos Delays (Falhas)')
            plt.xlabel('Delay (segundos)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.tight_layout()
            failed_plot_filename = os.path.join(output_dir, 'delay_failed_analysis.png')
            plt.savefig(failed_plot_filename, dpi=300)
            plt.close()
            report['failed_delay_plot'] = failed_plot_filename
    except ImportError:
        if logger:
            logger.warning("Matplotlib não disponível - visualizações não geradas")
    report_filename = os.path.join(
        output_dir, f"{report_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(report_filename, 'w') as f:
        json.dump(report, f, indent=2)
    if logger:
        logger.section("FINAL SUMMARY")
        logger.success(f"✔ Success: {report['summary']['success']}/{report['summary']['total_files']} ({report['summary']['success']/report['summary']['total_files']:.1%})")
        logger.warning(f"↻ Skipped: {report['summary']['skipped']}")
        logger.error(f"✖ Failed: {report['summary']['failed']}")
        logger.info(f"Total data: {report['summary']['total_bytes']/(1024**3):.2f} GB")
        logger.info(f"Throughput: {report['summary']['throughput_gbps']:.2f} Gbps")
        logger.info(f"Time elapsed: {report['summary']['time_elapsed']}")
        if 'success_delay_plot' in report:
            logger.info(f"Success delay plot saved to: {report['success_delay_plot']}")
        if 'failed_delay_plot' in report:
            logger.info(f"Failed delay plot saved to: {report['failed_delay_plot']}")
        logger.info(f"Full report saved to: {report_filename}")

def extract_file_links(html_content, base_url, patterns):
    soup = BeautifulSoup(html_content, 'html.parser')
    file_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].lower()
        if any(pattern in href for pattern in patterns):
            absolute_url = urljoin(base_url, a_tag['href'])
            file_links.append(absolute_url)
    return file_links

def get_page_content(url, user_agents, headers, logger=None):
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
        if logger:
            logger.error(f"[ERROR] Error accessing page: {e}")
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
    state_file="./state/download_state.json",
    log_file="./logs/taxi_extraction.log",
    console_log_file="./logs/console_log.log",
    report_prefix="download_report",
    disable_logging=False,               
    dataset_name=None,                   
    disable_progress_bar=False,          
    output_dir="./output",                      # <-- NEW ARG
):
    init(autoreset=True)
    yellow_title = "Starting download" if not dataset_name else f"Starting download of {dataset_name}"
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
{Fore.GREEN}{yellow_title.center(60)}
{Fore.YELLOW}{'='*60}{Fore.RESET}
"""
    logger = None
    if not disable_logging:
        # Ensure log file directories exist
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        if console_log_file:
            os.makedirs(os.path.dirname(console_log_file), exist_ok=True)
        logger = CustomLogger(banner=BANNER, console_log_file_path=console_log_file, file_log_path=log_file)
        print(BANNER)

    # Ensure output_dir exists (for report PNGs/JSON)
    if output_dir:
        output_dir_path = os.path.abspath(output_dir)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path, exist_ok=True)
    # Ensure state file directory exists
    if state_file:
        state_dir = os.path.dirname(state_file)
        if state_dir and not os.path.exists(state_dir):
            os.makedirs(state_dir, exist_ok=True)

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
    if headers is None:
        headers = DEFAULT_HEADERS
    if user_agents is None:
        user_agents = DEFAULT_USER_AGENTS

    ray_cpu = min(max_concurrent, os.cpu_count())
    ray.shutdown()
    ray.init(
        num_cpus=ray_cpu,
        include_dashboard=False,
        logging_level=logging.ERROR,
        ignore_reinit_error=True,
    )

    if logger: 
        logger.info(f"Ray version: {ray.__version__}")
    if logger: 
        logger.info(f"Available CPUs: {ray.available_resources()['CPU']}")
    if logger: 
        logger.info(f"Using CPUs: {ray_cpu}")

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        if logger: 
            logger.success(f"[DIR] Created directory: {download_dir}")
    else:
        if logger: 
            logger.info(f"[DIR] Directory exists: {download_dir}")
    if logger: 
        logger.section("PHASE 1: Collecting file links")
    html_content = get_page_content(base_url, user_agents, headers, logger=logger)
    if not html_content:
        if logger: 
            logger.error("Failed to access main page. Check connection and URL.")
        ray.shutdown()
        return
    file_links = extract_file_links(html_content, base_url, file_patterns)
    if max_files:
        file_links = file_links[:max_files]
    if not file_links:
        if logger: 
            logger.error("No valid links found. Check extraction pattern.")
        ray.shutdown()
        return
    if logger: 
        logger.success(f"Found {len(file_links)} files to download")
    if logger: 
        logger.section("PHASE 2: Parallel downloads with Ray")
    if max_concurrent is None:
        max_concurrent = min(16, int(ray.available_resources()['CPU'] * 1.5))
    if logger: 
        logger.info(f"Using {max_concurrent} parallel workers")
    
    state_handler = DownloadState(state_file=state_file, incremental=incremental)
    results = parallel_download_with_ray(
        file_links, headers, user_agents, max_concurrent, download_dir, state_handler,
        logger=logger,
        disable_progress_bar=disable_progress_bar  # <-- Pass to function
    )
    if logger: 
        logger.section("PHASE 3: Results analysis")
    generate_report(
        results, state_handler, 
        report_prefix=report_prefix, 
        logger=logger,
        output_dir=output_dir         # <-- Pass to generate_report
    )
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
    parser.add_argument("--console-log-file", default="console_log.log", help="Path for console log file")
    parser.add_argument("--report-prefix", default="download_report", help="Prefix for report files")
    parser.add_argument("--headers", type=str, default=None, help="Path to JSON file with custom headers")
    parser.add_argument("--user-agents", type=str, default=None, help="Path to text file with custom user agents (one per line)")
    parser.add_argument("--disable-logging", action="store_true", help="Disable all logging for production pipelines")  # <-- NEW ARG
    parser.add_argument("--dataset-name", type=str, default=None, help="Dataset name for banner")  # <-- NEW ARG
    parser.add_argument("--disable-progress-bar", action="store_true", help="Disable tqdm progress bar")  # <-- NEW ARG
    parser.add_argument("--output-dir", type=str, default=".", help="Directory for report PNGs and JSON")  # <-- NEW ARG
    args = parser.parse_args()

    headers = None
    user_agents = None
    if args.headers:
        with open(args.headers, "r", encoding="utf-8") as f:
            headers = json.load(f)
    if args.user_agents:
        with open(args.user_agents, "r", encoding="utf-8") as f:
            user_agents = [line.strip() for line in f if line.strip()]

    if not args.disable_logging:
        # Ensure log file directories exist
        if args.log_file:
            os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
        if args.console_log_file:
            os.makedirs(os.path.dirname(args.console_log_file), exist_ok=True)
        logger = CustomLogger(console_log_file_path=args.console_log_file, file_log_path=args.log_file)
        logger.info("Starting Ray cluster...")
    else:
        logger = None

    ray.shutdown()
    ray.init(
        num_cpus=min(args.max_concurrent, os.cpu_count()),
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
            disable_logging=args.disable_logging,
            dataset_name=args.dataset_name,
            disable_progress_bar=args.disable_progress_bar,
            output_dir=args.output_dir      # <-- Pass to scraper
        )
    finally:
        ray.shutdown()
        if logger:
            logger.info("Ray cluster shutdown")