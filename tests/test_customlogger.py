import sys
import os
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from CustomLogger import CustomLogger


def test_logger_creation(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(str(log_file), disable_terminal=True)
    assert logger is not None

def test_logger_log_and_file(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(str(log_file), disable_terminal=True)
    logger.log("info", "Test message")
    logger.log("error", "Error message")
    logger.close()
    with open(log_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert "Test message" in content
    assert "Error message" in content

def test_logger_terminal_output(capsys, tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(str(log_file), disable_terminal=False)
    logger.log("info", "Terminal message")
    logger.close()
    out = capsys.readouterr().out
    assert "Terminal message" in out

def test_logger_disable_terminal(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(str(log_file), disable_terminal=True)
    logger.log("info", "No terminal output")
    logger.close()
    # Should not print to terminal, so nothing to assert here, just coverage

def test_logger_close(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(str(log_file), disable_terminal=True)
    logger.close()
    # Should not raise

def test_logger_log_levels(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(str(log_file), disable_terminal=True)
    logger.log("info", "Info")
    logger.log("warning", "Warning")
    logger.log("error", "Error")
    logger.log("debug", "Debug")
    logger.close()
    with open(log_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert "Info" in content
    assert "Warning" in content
    assert "Error" in content
    assert "Debug" in content