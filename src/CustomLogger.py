import os
import sys
import re
import logging
import gc
from colorama import Fore
from datetime import datetime
from typing import List, Tuple, Optional, Any

__all__ = [
    "CustomLogger",
]

class CustomLogger(logging.Formatter):
    """
    Custom logger for colored terminal and file logging with sectioning and log rotation.

    Args:
        banner (str): Banner to display at the top of logs.
        log_file_path (str): Path to the log file.
        max_old_logs (int): Maximum number of old log files to keep.
    """

    COLORS = {
        'INFO': '✔',
        'WARNING': '⚠',
        'ERROR': '✖',
        'CRITICAL': '‼',
        'DEBUG': '•'
    }

    def __init__(self, banner: str = "", log_file_path: Optional[str] = None, max_old_logs: int = 25) -> None:
        """
        Initialize the CustomLogger.

        Args:
            banner (str): Banner to display at the top of logs.
            log_file_path (str): Path to the log file.
            max_old_logs (int): Maximum number of old log files to keep.
        """
        super().__init__()
        if log_file_path and not log_file_path.endswith(".log"):
            log_file_path += ".log"
        self.path_log_file: Optional[str] = log_file_path
        self.LOG_BUFFER_TERMINAL: List[Any] = []
        self.LOG_BUFFER_FILE: List[Any] = []
        self.BANNER: str = banner
        self.disable_terminal_logging: bool = False
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        if log_file_path:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            if os.path.isfile(log_file_path) and os.path.getsize(log_file_path) > 0:
                base, ext = os.path.splitext(log_file_path)
                rotated: List[str] = []
                for fname in os.listdir(os.path.dirname(log_file_path)):
                    if (
                        fname.startswith(os.path.basename(base))
                        and fname.endswith(ext)
                        and fname != os.path.basename(log_file_path)
                        and len(fname) > len(os.path.basename(log_file_path)) + 1
                    ):
                        rotated.append(fname)
                rotated_full: List[str] = [os.path.join(os.path.dirname(log_file_path), f) for f in rotated]
                if len(rotated_full) >= max_old_logs:
                    rotated_full.sort(key=os.path.getmtime)
                    for oldfile in rotated_full[:len(rotated_full) - max_old_logs + 1]:
                        try:
                            os.remove(oldfile)
                        except Exception:
                            pass
                ts: str = datetime.now().strftime("%Y%m%d_%H%M%S")
                rotated_name: str = f"{base}.{ts}{ext}"
                gc.collect()
                try:
                    os.rename(log_file_path, rotated_name)
                except PermissionError:
                    import time
                    time.sleep(0.2)
                    gc.collect()
                    os.rename(log_file_path, rotated_name)
            with open(log_file_path, "w", encoding="utf-8"):
                pass
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8', mode='a')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(self)
            self.file_handler = file_handler
            logging.getLogger().addHandler(file_handler)

    def strip_ansi(self, line: str) -> str:
        """
        Remove ANSI color codes from a log line.

        Args:
            line (str): Log line.

        Returns:
            str: Cleaned log line.
        """
        ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
        return ansi_escape.sub('', line)

    def redraw_logs(self) -> None:
        """
        Redraw all logs in the terminal, including the banner.
        """
        if getattr(self, "disable_terminal_logging", False):
            return
        if os.name == 'nt':
            os.system('cls')
        else:
            sys.stdout.write('\033[2J\033[H')
            sys.stdout.flush()
        print(self.BANNER)
        for line in self.LOG_BUFFER_TERMINAL:
            print(line[1])

    def append_log_to_file(self, line: str, filename: Optional[str] = None) -> None:
        """
        Append a log line to the log file.

        Args:
            line (str): Log line.
            filename (str): Optional log file path.
        """
        filename = filename or self.path_log_file
        if filename is not None:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "a", encoding="utf-8") as f:
                f.write(self.strip_ansi(line) + "\n")

    def _now(self) -> str:
        """
        Get the current timestamp as a string.

        Returns:
            str: Timestamp in [YYYY-MM-DD HH:MM:SS] format.
        """
        return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

    def info(self, msg: str) -> None:
        """
        Log an info message.

        Args:
            msg (str): Message to log.
        """
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]", "[SKIP]"))
        line = f"{self._now()} {Fore.CYAN}{msg}{Fore.RESET}" if add_time else f"{Fore.CYAN}{msg}{Fore.RESET}"
        item = ("info", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def warning(self, msg: str) -> None:
        """
        Log a warning message.

        Args:
            msg (str): Message to log.
        """
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]"))
        line = f"{self._now()} {Fore.YELLOW}{msg}{Fore.RESET}" if add_time else f"{Fore.YELLOW}{msg}{Fore.RESET}"
        item = ("warning", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def error(self, msg: str) -> None:
        """
        Log an error message.

        Args:
            msg (str): Message to log.
        """
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]"))
        line = f"{self._now()} {Fore.RED}{msg}{Fore.RESET}" if add_time else f"{Fore.RED}{msg}{Fore.RESET}"
        item = ("error", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def section(self, title: str) -> None:
        """
        Log a section title.

        Args:
            title (str): Section title.
        """
        line = f"{Fore.CYAN}{'='*6} {title} {'='*6}{Fore.RESET}"
        item = ("section", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def success(self, msg: str) -> None:
        """
        Log a success message.

        Args:
            msg (str): Message to log.
        """
        if msg.startswith("[DONE] Downloaded"):
            filename = msg.split("Downloaded ")[1].split(" ")[0]
            self.LOG_BUFFER_TERMINAL[:] = [
                line for line in self.LOG_BUFFER_TERMINAL
                if not (
                    (("[TRY]" in self.strip_ansi(line[1]) or "[FAIL]" in self.strip_ansi(line[1]))
                     and filename in self.strip_ansi(line[1]))
                )
            ]
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]"))
        line = f"{self._now()} {Fore.GREEN}{msg}{Fore.RESET}" if add_time else f"{Fore.GREEN}{msg}{Fore.RESET}"
        item = ("success", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def get_buffers(self) -> Tuple[List[Any], List[Any], str]:
        """
        Get the terminal and file log buffers and the banner.

        Returns:
            tuple: (terminal_buffer, file_buffer, banner)
        """
        return self.LOG_BUFFER_TERMINAL, self.LOG_BUFFER_FILE, self.BANNER

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record for file output.

        Args:
            record (logging.LogRecord): Log record.

        Returns:
            str: Formatted log message.
        """
        msg = super().format(record)
        for code in ['\033[31m', '\033[32m', '\033[33m', '\033[36m', '\033[39m', '\033[0m', '\033[1m']:
            msg = msg.replace(code, '')
        symbol = self.COLORS.get(record.levelname, '')
        if symbol and not msg.startswith(symbol):
            msg = f"{symbol} {msg}"
        return msg

    def close(self):
        """Close any open file handles or perform cleanup if needed."""
        if hasattr(self, "log_file") and getattr(self, "log_file", None):
            try:
                self.log_file.close()
            except Exception:
                pass