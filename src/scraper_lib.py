import requests
import time
import logging
from bs4 import BeautifulSoup
import os
from typing import Any, Dict, List, Optional, Union
os.environ['RAY_DEDUP_LOGS'] = "0"
os.environ["RAY_SILENT_MODE"] = "1"
from tqdm import tqdm
from urllib.parse import urljoin
import random
import sys
import json
import matplotlib.pyplot as plt 
import ray
from collections import deque
from colorama import init, Fore, Style
from CustomLogger import CustomLogger
try:
    from DownloadState import DownloadState
    print("foi normal")
except (ModuleNotFoundError,ImportError):
    try:
        from .DownloadState import DownloadState
        print("foi com o relativo (.)")
    except (ModuleNotFoundError,ImportError):
        print("foi com src")
        from src.DownloadState import DownloadState
import numpy as np
from datetime import datetime
import gc
import shutil



class ScraperLib:
    """
    Library for parallel file extraction and download, report generation, and log/result rotation.

    Allows downloading files from a web page in parallel using Ray,
    managing download state, generating reports and charts, and rotating old logs and reports.
    """

    def __init__(
        self,
        base_url: str,
        file_patterns: List[str],
        download_dir: str = "data",
        incremental: bool = False,
        max_files: Optional[int] = None,
        max_concurrent: Optional[int] = None,
        headers: Optional[Dict[str, Any]] = None,
        user_agents: Optional[List[str]] = None,
        state_file: str = "./state/download_state.json",
        log_file: str = "./logs/process_log.log",
        report_prefix: str = "download_report",
        disable_logging: bool = False,
        disable_terminal_logging: bool = False,
        dataset_name: Optional[str] = None,
        disable_progress_bar: bool = False,
        output_dir: str = "./output",
        max_old_logs: int = 25,
        max_old_runs: int = 25,
        ray_instance: Optional[Any] = None,
        chunk_size: Union[int, str] = 5 * 1024 * 1024,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        max_retries: int = 5,
    ) -> None:
        """
        Initialize ScraperLib with the provided options.

        Args:
            base_url (str): Base URL to extract files from.
            file_patterns (list): List of file patterns to download (e.g., [".csv", ".zip"]).
            download_dir (str): Directory to save downloaded files.
            incremental (bool): If True, enables incremental mode (does not download already downloaded files).
            max_files (int): Limits the number of files to download.
            max_concurrent (int): Maximum number of parallel downloads.
            headers (dict): Custom HTTP headers.
            user_agents (list): List of user-agents for rotation.
            state_file (str): Path to the state file.
            log_file (str): Path to the log file.
            report_prefix (str): Prefix for generated reports.
            disable_logging (bool): Disables logging.
            disable_terminal_logging (bool): Disables terminal logging.
            dataset_name (str): Dataset name for display.
            disable_progress_bar (bool): Disables progress bar.
            output_dir (str): Directory for reports and PNGs.
            max_old_logs (int): Maximum number of old logs to keep.
            max_old_runs (int): Maximum number of old reports to keep.
            ray_instance: Already initialized Ray instance (optional).
            chunk_size (int or str): Chunk size in bytes or as a string (e.g. '10mb', '1gb').
            initial_delay (float): Initial delay between retries (seconds).
            max_delay (float): Maximum delay between retries (seconds).
            max_retries (int): Maximum number of download retries.
        """
        self.base_url: str = base_url
        self.file_patterns: List[str] = file_patterns
        self.download_dir: str = download_dir
        self.incremental: bool = incremental
        self.max_files: Optional[int] = max_files
        self.max_concurrent: Optional[int] = max_concurrent
        self.headers: Optional[Dict[str, Any]] = headers
        self.user_agents: Optional[List[str]] = user_agents
        self.state_file: str = state_file
        self.log_file: str = log_file
        self.report_prefix: str = report_prefix
        self.disable_logging: bool = disable_logging
        self.disable_terminal_logging: bool = disable_terminal_logging
        self.dataset_name: Optional[str] = dataset_name
        self.disable_progress_bar: bool = disable_progress_bar
        self.output_dir: str = output_dir
        self.max_old_logs: int = max_old_logs
        self.max_old_runs: int = max_old_runs
        self.ray_instance: Optional[Any] = ray_instance
        self.chunk_size: int = self._parse_chunk_size(chunk_size) if isinstance(chunk_size, str) else chunk_size
        self.initial_delay: float = initial_delay
        self.max_delay: float = max_delay
        self.max_retries: int = max_retries
        self.logger: Optional[CustomLogger] = None

    def _safe_copy2(
        self,
        src: str,
        dst: str,
        max_retries: int = 10,
        delay: float = 0.2,
        logger: Optional[CustomLogger] = None
    ) -> bool:
        """
        Safely copies a file from src to dst, retrying multiple times if the file is in use.

        Args:
            src (str): Source file path.
            dst (str): Destination file path.
            max_retries (int): Maximum number of attempts.
            delay (float): Wait time between attempts.
            logger (CustomLogger): Logger to record failures.

        Returns:
            bool: True if copy succeeded, False otherwise.
        """
        for attempt in range(max_retries):
            try:
                shutil.copy2(src, dst)
                return True
            except PermissionError as e:
                if attempt == max_retries - 1:
                    if logger:
                        logger.error(f"Failed to copy {src} to {dst}: {e}")
                    raise
                time.sleep(delay)
        return False

    def _rotate_and_limit_files(
        self,
        base_path: str,
        ext: str,
        max_old_files: Optional[int]
    ) -> None:
        """
        Rotates and limits old files (e.g., logs, reports, PNGs).

        Args:
            base_path (str): Base file path (without extension).
            ext (str): File extension (e.g., ".json").
            max_old_files (int): Maximum number of old files to keep.
        """
        if not os.path.exists(base_path + ext):
            return
        if max_old_files is None:
            return
        rotated: List[str] = []
        for fname in os.listdir(os.path.dirname(base_path)):
            if (
                fname.startswith(os.path.basename(base_path))
                and fname.endswith(ext)
                and fname != os.path.basename(base_path + ext)
                and len(fname) > len(os.path.basename(base_path + ext)) + 1
            ):
                rotated.append(fname)
        rotated_full: List[str] = [os.path.join(os.path.dirname(base_path), f) for f in rotated]
        if len(rotated_full) >= max_old_files:
            rotated_full.sort(key=os.path.getmtime)
            for oldfile in rotated_full[:len(rotated_full) - max_old_files + 1]:
                try:
                    os.remove(oldfile)
                except Exception:
                    pass
        if os.path.getsize(base_path + ext) > 0:
            ts: str = datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated_name: str = f"{base_path}.{ts}{ext}"
            gc.collect()
            try:
                os.rename(base_path + ext, rotated_name)
            except PermissionError:
                time.sleep(0.2)
                gc.collect()
                os.rename(base_path + ext, rotated_name)

    def _extract_file_links(
        self,
        html_content: str,
        base_url: str,
        patterns: List[str]
    ) -> List[str]:
        """
        Extracts file links from HTML that match the provided patterns.

        Args:
            html_content (str): HTML content of the page.
            base_url (str): Base URL to resolve relative links.
            patterns (list): List of file patterns.

        Returns:
            list: List of absolute URLs of found files.
        """
        soup: BeautifulSoup = BeautifulSoup(html_content, 'html.parser')
        file_links: List[str] = []
        for a_tag in soup.find_all('a', href=True):
            href: str = a_tag['href'].lower()
            if any(pattern in href for pattern in patterns):
                absolute_url: str = urljoin(base_url, a_tag['href'])
                file_links.append(absolute_url)
        return file_links

    def _get_page_content(
        self,
        url: str,
        user_agents: Optional[List[str]],
        headers: Optional[Dict[str, Any]],
        logger: Optional[CustomLogger] = None
    ) -> Optional[str]:
        """
        Downloads the HTML content of the base page.

        Args:
            url (str): Page URL.
            user_agents (list): List of user-agents.
            headers (dict): HTTP headers.
            logger (CustomLogger): Logger to record errors.

        Returns:
            str: HTML content of the page, or None on error.
        """
        headers = headers.copy() if headers else {}
        if user_agents:
            headers['User-Agent'] = random.choice(user_agents)
        session: requests.Session = requests.Session()
        session.headers.update(headers)
        try:
            response: requests.Response = session.get(url, timeout=(10, 30))
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            if logger:
                logger.error(f"[ERROR] Error accessing page: {e}")
            return None

    def _generate_report(
        self,
        results: List[Dict[str, Any]],
        state: DownloadState,
        report_prefix: Optional[str] = None,
        logger: Optional[CustomLogger] = None,
        output_dir: Optional[str] = None,
        max_old_runs: Optional[int] = None
    ) -> None:
        """
        Generates a JSON report and analysis PNGs of downloads, with file rotation.

        Args:
            results (list): List of download results.
            state (DownloadState): Download state.
            report_prefix (str): Report prefix.
            logger (CustomLogger): Logger to record info.
            output_dir (str): Output directory for reports.
            max_old_runs (int): Maximum number of old reports to keep.
        """
        if report_prefix is None:
            report_prefix = self.report_prefix
        if logger is None:
            logger = self.logger
        if output_dir is None:
            output_dir = self.output_dir
        if max_old_runs is None:
            max_old_runs = self.max_old_runs

        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        report_dir = os.path.join(output_dir, "reports")
        png_dir = os.path.join(output_dir, "pngs")
        os.makedirs(report_dir, exist_ok=True)
        os.makedirs(png_dir, exist_ok=True)

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
                success_plot_filename = os.path.join(png_dir, 'delay_success_analysis.png')
                plt.savefig(success_plot_filename, dpi=300)
                plt.close('all')
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
                failed_plot_filename = os.path.join(png_dir, 'delay_failed_analysis.png')
                plt.savefig(failed_plot_filename, dpi=300)
                plt.close('all')
                report['failed_delay_plot'] = failed_plot_filename
        except ImportError:
            if logger:
                logger.warning("Matplotlib não disponível - visualizações não geradas")
        report_base = os.path.join(report_dir, report_prefix)
        base_json = report_base + ".json"
        os.makedirs(os.path.dirname(base_json), exist_ok=True)
        with open(base_json, 'w') as f:
            json.dump(report, f, indent=2)
        self._rotate_and_limit_files(report_base, ".json", max_old_runs)
        report_filename = os.path.join(
            report_dir, f"{report_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        if os.path.exists(base_json):
            shutil.copy2(base_json, report_filename)
        else:
            if logger:
                logger.error(f"Base report file not found for copy: {base_json}")
        # PNGs
        if 'success_delay_plot' in report:
            png_base = os.path.splitext(os.path.join(png_dir, 'delay_success_analysis.png'))[0]
            base_png = png_base + ".png"
            if os.path.abspath(report['success_delay_plot']) != os.path.abspath(base_png):
                self._safe_copy2(report['success_delay_plot'], base_png, logger=logger)
            self._rotate_and_limit_files(png_base, ".png", max_old_runs)
            ts_png = os.path.join(png_dir, f"delay_success_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            if os.path.exists(base_png):
                self._safe_copy2(base_png, ts_png, logger=logger)
            else:
                if logger:
                    logger.error(f"Base PNG not found for copy: {base_png}")
        if 'failed_delay_plot' in report:
            png_base = os.path.splitext(os.path.join(png_dir, 'delay_failed_analysis.png'))[0]
            base_png = png_base + ".png"
            if os.path.abspath(report['failed_delay_plot']) != os.path.abspath(base_png):
                self._safe_copy2(report['failed_delay_plot'], base_png, logger=logger)
            self._rotate_and_limit_files(png_base, ".png", max_old_runs)
            ts_png = os.path.join(png_dir, f"delay_failed_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            if os.path.exists(base_png):
                self._safe_copy2(base_png, ts_png, logger=logger)
            else:
                if logger:
                    logger.error(f"Base PNG not found for copy: {base_png}")
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

    def _parallel_download_with_ray(
        self,
        file_urls: List[str],
        headers: Dict[str, Any],
        user_agents: List[str],
        max_concurrent: int = 8,
        download_dir: str = "data",
        state_handler: Optional[DownloadState] = None,
        logger: Optional[CustomLogger] = None,
        disable_progress_bar: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Performs parallel download of files using Ray.

        Args:
            file_urls (list): List of file URLs to download.
            headers (dict): HTTP headers.
            user_agents (list): List of user-agents.
            max_concurrent (int): Maximum number of parallel downloads.
            download_dir (str): Destination directory.
            state_handler (DownloadState): Download state handler.
            logger (CustomLogger): Logger to record progress.
            disable_progress_bar (bool): Disables progress bar.

        Returns:
            list: List of download results.
        """
        chunk_size = self.chunk_size
        initial_delay = self.initial_delay
        max_delay = self.max_delay
        max_retries = self.max_retries

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
            disable=disable_progress_bar
        )

        with progress_bar as pbar:
            tqdm._instances.clear()
            for _ in range(max_concurrent):
                url = ray.get(queue.get_next.remote())
                if url:
                    active_tasks.append(
                        self._download_file_ray.remote(
                            url, headers, user_agents, download_dir, state_handler,
                            chunk_size, initial_delay, max_delay, max_retries
                        )
                    )

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
                        active_tasks.append(
                            self._download_file_ray.remote(
                                next_url, headers, user_agents, download_dir, state_handler,
                                chunk_size, initial_delay, max_delay, max_retries
                            )
                        )
        return results

    @staticmethod
    @ray.remote
    def _download_file_ray(
        file_url: str,
        headers: Dict[str, Any],
        user_agents: List[str],
        download_dir: str = "data",
        state_handler: Optional[DownloadState] = None,
        chunk_size: int = 5 * 1024 * 1024,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        max_retries: int = 5,
        base_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Ray remote function for downloading a file.

        Args:
            file_url (str): File URL.
            headers (dict): HTTP headers.
            user_agents (list): List of user-agents.
            download_dir (str): Destination directory.
            state_handler (DownloadState): Download state handler.
            chunk_size (int): Chunk size in bytes for downloads.
            initial_delay (float): Initial delay between retries (seconds).
            max_delay (float): Maximum delay between retries (seconds).
            max_retries (int): Maximum number of download retries.
            base_url (str): Base URL for referer.

        Returns:
            dict: Download result (status, file, logs, etc).
        """
        CHUNK_SIZE = chunk_size
        INITIAL_DELAY = initial_delay
        MAX_DELAY = max_delay
        MAX_RETRIES = max_retries
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

    def run(self) -> None:
        """
        Runs the main scraping, download, and report generation flow.

        This method initializes the logger, prepares directories, collects links, performs parallel downloads,
        and generates reports and charts at the end of the process.
        """
        init(autoreset=True)
        yellow_title = "Starting download" if not self.dataset_name else f"Starting download of {self.dataset_name}"
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
        if not self.disable_logging:
            if self.log_file:
                os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            logger = CustomLogger(
                banner=BANNER,
                log_file_path=self.log_file,
                max_old_logs=self.max_old_logs
            )
            logger.disable_terminal_logging = self.disable_terminal_logging
            if not self.disable_terminal_logging:
                print(BANNER)
        self.logger = logger

        if self.output_dir:
            output_dir_path = os.path.abspath(self.output_dir)
            if not os.path.exists(output_dir_path):
                os.makedirs(output_dir_path, exist_ok=True)
        if self.state_file:
            state_dir = os.path.dirname(self.state_file)
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
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15; rv:109.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        ]
        if self.headers is None:
            self.headers = DEFAULT_HEADERS
        if self.user_agents is None:
            self.user_agents = DEFAULT_USER_AGENTS

        ray_created = False
        if self.ray_instance is None:
            ray_cpu = min(self.max_concurrent, os.cpu_count())
            ray.shutdown()
            ray.init(
                num_cpus=ray_cpu,
                include_dashboard=False,
                logging_level=logging.ERROR,
                ignore_reinit_error=True,
            )
            ray_created = True

        if logger:
            logger.info(f"Ray version: {ray.__version__}")
            logger.info(f"Available CPUs: {ray.available_resources()['CPU']}")
            logger.info(f"Using CPUs: {min(self.max_concurrent, os.cpu_count())}")

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            if logger:
                logger.success(f"[DIR] Created directory: {self.download_dir}")
        else:
            if logger:
                logger.info(f"[DIR] Directory exists: {self.download_dir}")
        if logger:
            logger.section("PHASE 1: Collecting file links")
        html_content = self._get_page_content(self.base_url, self.user_agents, self.headers, logger=logger)
        if not html_content:
            if logger:
                logger.error("Failed to access main page. Check connection and URL.")
            if ray_created:
                ray.shutdown()
            return
        file_links = self._extract_file_links(html_content, self.base_url, self.file_patterns)
        if self.max_files:
            file_links = file_links[:self.max_files]
        if not file_links:
            if logger:
                logger.error("No valid links found. Check extraction pattern.")
            if ray_created:
                ray.shutdown()
            return
        if logger:
            logger.success(f"Found {len(file_links)} files to download")
        if logger:
            logger.section("PHASE 2: Parallel downloads with Ray")
        max_concurrent = self.max_concurrent
        if max_concurrent is None:
            max_concurrent = min(16, int(ray.available_resources()['CPU'] * 1.5))
        if logger:
            logger.info(f"Using {max_concurrent} parallel workers")

        state_handler = DownloadState(state_file=self.state_file, incremental=self.incremental)
        results = self._parallel_download_with_ray(
            file_links, self.headers, self.user_agents, max_concurrent, self.download_dir, state_handler,
            logger=logger,
            disable_progress_bar=self.disable_progress_bar
        )
        if logger:
            logger.section("PHASE 3: Results analysis")
        self._generate_report(
            results, state_handler,
            report_prefix=self.report_prefix,
            logger=logger,
            output_dir=self.output_dir,
            max_old_runs=self.max_old_runs
        )
        if ray_created:
            ray.shutdown()

    @staticmethod
    def cli() -> None:
        """
        Command-line interface to run ScraperLib.

        Reads arguments from the terminal, instantiates ScraperLib, and runs the process.

        Usage example:
            python -m scraper_lib.cli --url <URL> --patterns .csv .zip --dir data --max-files 10

        Parameters:
            --url: Base URL to scrape for files.
            --patterns: List of file patterns to match (e.g. .csv .zip).
            --dir: Download directory.
            --incremental: Enable incremental download state.
            --max-files: Limit number of files to download.
            --max-concurrent: Max parallel downloads.
            --chunk-size: Chunk size for downloads (e.g. 1gb, 10mb, 8 bytes).
            --initial-delay: Initial delay between retries (seconds).
            --max-delay: Maximum delay between retries (seconds).
            --max-retries: Maximum number of download retries.
            --state-file: Path for download state file.
            --log-file: Path for main log file.
            --report-prefix: Prefix for report files.
            --headers: Path to JSON file with custom headers.
            --user-agents: Path to text file with custom user agents (one per line).
            --disable-logging: Disable all logging for production pipelines.
            --disable-terminal-logging: Disable terminal logging.
            --dataset-name: Dataset name for banner.
            --disable-progress-bar: Disable tqdm progress bar.
            --output-dir: Directory for report PNGs and JSON.
            --max-old-logs: Max old log files to keep (default: 25, None disables rotation).
            --max-old-runs: Max old report/png runs to keep (default: 25, None disables rotation).
        """
        import argparse
        parser = argparse.ArgumentParser(
            description="Generic parallel file downloader",
            epilog="""
Arguments:
  --url                Base URL to scrape for files.
  --patterns           List of file patterns to match (e.g. .csv .zip).
  --dir                Download directory.
  --incremental        Enable incremental download state.
  --max-files          Limit number of files to download.
  --max-concurrent     Max parallel downloads.
  --chunk-size         Chunk size for downloads (e.g. 1gb, 10mb, 8 bytes).
  --initial-delay      Initial delay between retries (seconds).
  --max-delay          Maximum delay between retries (seconds).
  --max-retries        Maximum number of download retries.
  --state-file         Path for download state file.
  --log-file           Path for main log file.
  --report-prefix      Prefix for report files.
  --headers            Path to JSON file with custom headers.
  --user-agents        Path to text file with custom user agents (one per line).
  --disable-logging    Disable all logging for production pipelines.
  --disable-terminal-logging Disable terminal logging.
  --dataset-name       Dataset name for banner.
  --disable-progress-bar Disable tqdm progress bar.
  --output-dir         Directory for report PNGs and JSON.
  --max-old-logs       Max old log files to keep (default: 25, None disables rotation).
  --max-old-runs       Max old report/png runs to keep (default: 25, None disables rotation).
"""
        )
        parser.add_argument("--url", required=True, help="Base URL to scrape for files")
        parser.add_argument("--patterns", nargs="+", required=True, help="List of file patterns to match (e.g. .csv .zip)")
        parser.add_argument("--dir", default="data", help="Download directory")
        parser.add_argument("--incremental", action="store_true", help="Enable incremental download state")
        parser.add_argument("--max-files", type=int, default=None, help="Limit number of files to download")
        parser.add_argument("--max-concurrent", type=int, default=None, help="Max parallel downloads")
        parser.add_argument("--state-file", default="download_state.json", help="Path for download state file")
        parser.add_argument("--log-file", default="taxi_extraction.log", help="Path for main log file")
        parser.add_argument("--report-prefix", default="download_report", help="Prefix for report files")
        parser.add_argument("--headers", type=str, default=None, help="Path to JSON file with custom headers")
        parser.add_argument("--user-agents", type=str, default=None, help="Path to text file with custom user agents (one per line)")
        parser.add_argument("--disable-logging", action="store_true", help="Disable all logging for production pipelines")
        parser.add_argument("--disable-terminal-logging", action="store_true", help="Disable terminal logging")
        parser.add_argument("--dataset-name", type=str, default=None, help="Dataset name for banner")
        parser.add_argument("--disable-progress-bar", action="store_true", help="Disable tqdm progress bar")
        parser.add_argument("--output-dir", type=str, default=".", help="Directory for report PNGs and JSON")
        parser.add_argument("--max-old-logs", type=int, default=25, nargs='?', help="Max old log files to keep (default: 25, None disables rotation)")
        parser.add_argument("--max-old-runs", type=int, default=25, nargs='?', help="Max old report/png runs to keep (default: 25, None disables rotation)")
        parser.add_argument("--chunk-size", type=str, default="5mb", help="Chunk size for downloads (e.g. 1gb, 10mb, 8 bytes)")
        parser.add_argument("--initial-delay", type=float, default=1.0, help="Initial delay between retries (seconds)")
        parser.add_argument("--max-delay", type=float, default=60.0, help="Maximum delay between retries (seconds)")
        parser.add_argument("--max-retries", type=int, default=5, help="Maximum number of download retries")
        args = parser.parse_args()

        headers = None
        user_agents = None
        if args.headers:
            with open(args.headers, "r", encoding="utf-8") as f:
                headers = json.load(f)
        if args.user_agents:
            with open(args.user_agents, "r", encoding="utf-8") as f:
                user_agents = [line.strip() for line in f if line.strip()]

        chunk_size = ScraperLib._parse_chunk_size(args.chunk_size)

        ray.shutdown()
        ray.init(
            num_cpus=min(args.max_concurrent, os.cpu_count()),
            include_dashboard=False,
            logging_level=logging.ERROR,
            ignore_reinit_error=True,
        )
        try:
            scraper = ScraperLib(
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
                report_prefix=args.report_prefix,
                disable_logging=args.disable_logging,
                disable_terminal_logging=args.disable_terminal_logging,
                dataset_name=args.dataset_name,
                disable_progress_bar=args.disable_progress_bar,
                output_dir=args.output_dir,
                max_old_logs=args.max_old_logs,
                max_old_runs=args.max_old_runs,
                chunk_size=chunk_size,
                initial_delay=args.initial_delay,
                max_delay=args.max_delay,
                max_retries=args.max_retries
            )
            scraper.run()
        finally:
            ray.shutdown()

    def _parse_chunk_size(size_str: str) -> int:
        """
        Parse a human-readable chunk size string (e.g., '1gb', '10mb', '8 bytes') into an integer number of bytes.

        Args:
            size_str (str): Chunk size as a string.

        Returns:
            int: Chunk size in bytes.

        Raises:
            ValueError: If the string cannot be parsed.
        """
        size_str = size_str.strip().lower().replace(" ", "")
        units = {
            "b": 1,
            "bytes": 1,
            "kb": 1024,
            "mb": 1024 ** 2,
            "gb": 1024 ** 3,
            "tb": 1024 ** 4,
        }
        for unit in sorted(units, key=len, reverse=True):
            if size_str.endswith(unit):
                num = size_str[: -len(unit)]
                try:
                    return int(float(num) * units[unit])
                except Exception:
                    raise ValueError(f"Invalid chunk size: {size_str}")
        try:
            return int(size_str)
        except Exception:
            raise ValueError(f"Invalid chunk size: {size_str}")