from datetime import datetime
import os
import portalocker
import numpy as np
import json
import hashlib
from typing import Any, Callable, Dict

class DownloadState:
    """
    Manages the persistent state of downloads, including completed, failed, and delay statistics.

    This class provides atomic file operations for safe concurrent access, tracks download progress,
    and maintains statistics for reporting and incremental downloads.

    Args:
        state_file (str): Path to the state file (JSON).
        incremental (bool): If True, loads existing state; otherwise, starts fresh.
    """

    def __init__(self, state_file: str = "download_state.json", incremental: bool = True) -> None:
        """
        Initialize the DownloadState.

        Args:
            state_file (str): Path to the state file (JSON).
            incremental (bool): If True, loads existing state; otherwise, starts fresh.
        """
        self.state_file: str = state_file
        self._cache: Dict[str, Any] = {}  # Local cache to reduce disk access
        # Garante que o diretório existe
        state_dir = os.path.dirname(os.path.abspath(self.state_file))
        if state_dir and not os.path.exists(state_dir):
            os.makedirs(state_dir, exist_ok=True)
        if incremental:
            self.load_state()
        else:
            self.generate()

    def generate(self) -> Dict[str, Any]:
        """
        Initialize a fresh state structure and salva no disco.

        Returns:
            dict: The initialized state dictionary.
        """
        self._cache = {
            'completed': {},
            'failed': {},
            'delays_success': [],
            'delays_failed': [],
            'stats': {
                'start_time': datetime.now().isoformat(),
                'last_update': None,
                'total_bytes': 0,
                'delay_stats_success': {
                    'min': None,
                    'max': None,
                    'avg': None,
                    'median': None,
                    'percentiles': {}
                },
                'delay_stats_failed': {
                    'min': None,
                    'max': None,
                    'avg': None,
                    'median': None,
                    'percentiles': {}
                }
            }
        }
        # Salva imediatamente o estado inicial
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self._cache, f, indent=2)
        return self._cache

    def _atomic_file_operation(self, operation: Callable[[], None]) -> None:
        """
        Execute file operations with locking for safe concurrent access.

        Args:
            operation (callable): Function to execute atomically.
        """
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

    def add_delay(self, delay: float, success: bool = True) -> None:
        """
        Record a new delay and update statistics.

        Args:
            delay (float): Delay value in seconds.
            success (bool): True if the delay was for a successful download.
        """
        def _add() -> None:
            delay_record = {
                'value': delay,
                'timestamp': datetime.now().isoformat()
            }
            key = 'delays_success' if success else 'delays_failed'
            self._cache[key].append(delay_record)
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

    def load_state(self) -> None:
        """
        Load state from file.
        """
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                try:
                    self._cache = json.load(f)
                except json.JSONDecodeError:
                    self.generate()
        else:
            self.generate()

    def save_state(self) -> None:
        """
        Save the current state to file.
        """
        def _save() -> None:
            self._cache['stats']['last_update'] = datetime.now().isoformat()
        self._atomic_file_operation(_save)

    def get_file_id(self, url: str) -> str:
        """
        Generate a unique file ID for a given URL.

        Args:
            url (str): File URL.

        Returns:
            str: MD5 hash of the URL.
        """
        return hashlib.md5(url.encode()).hexdigest()

    def is_completed(self, url: str) -> bool:
        """
        Check if a file has already been downloaded.

        Args:
            url (str): File URL.

        Returns:
            bool: True if completed, False otherwise.
        """
        return self.get_file_id(url) in self._cache['completed']

    def add_completed(self, url: str, filepath: str, size: int) -> None:
        """
        Mark a file as successfully downloaded.

        Args:
            url (str): File URL.
            filepath (str): Local file path.
            size (int): File size in bytes.
        """
        def _add() -> None:
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

    def add_failed(self, url: str, error: Any) -> None:
        """
        Mark a file as failed to download.

        Args:
            url (str): File URL.
            error (str): Error message.
        """
        def _add() -> None:
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
    def state(self) -> Dict[str, Any]:
        """
        Get the current state dictionary.

        Returns:
            dict: The current state.
        """
        return self._cache