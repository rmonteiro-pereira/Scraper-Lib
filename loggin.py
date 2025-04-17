import os
import sys
import re
from colorama import  Fore
import logging
__all__ = [
    "strip_ansi",
    "redraw_logs",
    "append_log_to_file",
    "log_info",
    "log_warning",
    "log_error",
    "log_section",
    "log_success",
]


def strip_ansi(line):
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    return ansi_escape.sub('', line)

def redraw_logs(LOG_BUFFER_TERMINAL, BANNER):
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

def append_log_to_file(line, LOG_FILE_BUFFER, filename=None):
    if filename is None:
        filename = LOG_FILE_BUFFER
    if filename is None:
        filename = "console_log.txt"
    with open(filename, "a", encoding="utf-8") as f:
        f.write(strip_ansi(line) + "\n")

def log_info(msg, LOG_BUFFER_TERMINAL, LOG_BUFFER_FILE, LOG_FILE_BUFFER, BANNER):
    line = f"{Fore.CYAN}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line, LOG_FILE_BUFFER)
    redraw_logs(LOG_BUFFER_TERMINAL, BANNER)

def log_warning(msg, LOG_BUFFER_TERMINAL, LOG_BUFFER_FILE, LOG_FILE_BUFFER, BANNER):
    line = f"{Fore.YELLOW}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line, LOG_FILE_BUFFER)
    redraw_logs(LOG_BUFFER_TERMINAL, BANNER)

def log_error(msg, LOG_BUFFER_TERMINAL, LOG_BUFFER_FILE, LOG_FILE_BUFFER, BANNER):
    line = f"{Fore.RED}{msg}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line, LOG_FILE_BUFFER)
    redraw_logs(LOG_BUFFER_TERMINAL, BANNER)

def log_section(title, LOG_BUFFER_TERMINAL, LOG_BUFFER_FILE, LOG_FILE_BUFFER, BANNER):
    line = f"\n{Fore.CYAN}{'='*6} {title} {'='*6}{Fore.RESET}"
    LOG_BUFFER_TERMINAL.append(line)
    LOG_BUFFER_FILE.append(line)
    append_log_to_file(line, LOG_FILE_BUFFER)
    redraw_logs(LOG_BUFFER_TERMINAL, BANNER)

def log_success(msg, LOG_BUFFER_TERMINAL, LOG_BUFFER_FILE, LOG_FILE_BUFFER, BANNER):
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
    append_log_to_file(line, LOG_FILE_BUFFER)
    redraw_logs(LOG_BUFFER_TERMINAL, BANNER)

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