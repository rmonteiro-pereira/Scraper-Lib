import os
import sys
import re
import logging
import gc
from colorama import Fore
from datetime import datetime

__all__ = [
    "CustomLogger",
]

class CustomLogger(logging.Formatter):
    COLORS = {
        'INFO': '✔',
        'WARNING': '⚠',
        'ERROR': '✖',
        'CRITICAL': '‼',
        'DEBUG': '•'
    }

    def __init__(self, banner="", log_file_path=None, max_old_logs=25):
        super().__init__()
        # Ensure log file ends with .log
        if log_file_path and not log_file_path.endswith(".log"):
            log_file_path += ".log"
        self.path_log_file = log_file_path
        self.LOG_BUFFER_TERMINAL = []
        self.LOG_BUFFER_FILE = []
        self.BANNER = banner
        self.disable_terminal_logging = False  # <-- Add this attribute
        # Remove and close all handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        # Handle old log file rotation
        if log_file_path:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            # Only rotate if file exists and is not empty
            if os.path.isfile(log_file_path) and os.path.getsize(log_file_path) > 0:
                base, ext = os.path.splitext(log_file_path)
                # Match only rotated logs: e.g. process_log.log.20240418_153000
                rotated = []
                for fname in os.listdir(os.path.dirname(log_file_path)):
                    if (
                        fname.startswith(os.path.basename(base))
                        and fname.endswith(ext)
                        and fname != os.path.basename(log_file_path)
                        and len(fname) > len(os.path.basename(log_file_path)) + 1  # must have .TIMESTAMP
                    ):
                        rotated.append(fname)
                rotated_full = [os.path.join(os.path.dirname(log_file_path), f) for f in rotated]
                # Remove oldest if exceeding max_old_logs
                if len(rotated_full) >= max_old_logs:
                    rotated_full.sort(key=os.path.getmtime)
                    for oldfile in rotated_full[:len(rotated_full) - max_old_logs + 1]:
                        try:
                            os.remove(oldfile)
                        except Exception:
                            pass
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                rotated_name = f"{base}.{ts}{ext}"
                gc.collect()
                try:
                    os.rename(log_file_path, rotated_name)
                except PermissionError:
                    import time
                    time.sleep(0.2)
                    gc.collect()
                    os.rename(log_file_path, rotated_name)
            # Clear log file at start (truncate)
            with open(log_file_path, "w", encoding="utf-8"):
                pass
            # Only now add the handler!
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8', mode='a')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(self)
            self.file_handler = file_handler
            logging.getLogger().addHandler(file_handler)

    def strip_ansi(self, line):
        ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
        return ansi_escape.sub('', line)

    def redraw_logs(self):
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

    def append_log_to_file(self, line, filename=None):
        filename = filename or self.path_log_file
        if filename is not None:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "a", encoding="utf-8") as f:
                f.write(self.strip_ansi(line) + "\n")

    def _now(self):
        return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

    def info(self, msg):
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]", "[SKIP]"))
        line = f"{self._now()} {Fore.CYAN}{msg}{Fore.RESET}" if add_time else f"{Fore.CYAN}{msg}{Fore.RESET}"
        item = ("info", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def warning(self, msg):
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]"))
        line = f"{self._now()} {Fore.YELLOW}{msg}{Fore.RESET}" if add_time else f"{Fore.YELLOW}{msg}{Fore.RESET}"
        item = ("warning", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def error(self, msg):
        add_time = any(msg.strip().startswith(tag) for tag in ("[TRY]", "[FAIL]", "[DONE]"))
        line = f"{self._now()} {Fore.RED}{msg}{Fore.RESET}" if add_time else f"{Fore.RED}{msg}{Fore.RESET}"
        item = ("error", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def section(self, title):
        # Section titles do not get a timestamp
        line = f"{Fore.CYAN}{'='*6} {title} {'='*6}{Fore.RESET}"
        item = ("section", line)
        self.LOG_BUFFER_TERMINAL.append(item)
        self.LOG_BUFFER_FILE.append(item)
        self.append_log_to_file(line, self.path_log_file)
        self.redraw_logs()

    def success(self, msg):
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

    def get_buffers(self):
        return self.LOG_BUFFER_TERMINAL, self.LOG_BUFFER_FILE, self.BANNER

    # Formatter override for file logging (removes ANSI and adds emoji/symbols)
    def format(self, record):
        msg = super().format(record)
        for code in ['\033[31m', '\033[32m', '\033[33m', '\033[36m', '\033[39m', '\033[0m', '\033[1m']:
            msg = msg.replace(code, '')
        symbol = self.COLORS.get(record.levelname, '')
        if symbol and not msg.startswith(symbol):
            msg = f"{symbol} {msg}"
        return msg