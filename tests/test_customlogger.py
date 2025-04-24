import logging
from CustomLogger import CustomLogger

def test_logger_creation(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file), max_old_logs=2)
    assert logger is not None

def test_logger_log_and_file(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.info("Test info")
    logger.warning("Test warning")
    logger.error("Test error")
    logger.success("[DONE] Downloaded file.csv")
    logger.section("Section Title")
    logger.close()
    with open(log_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert "Test info" in content
    assert "Test warning" in content
    assert "Test error" in content
    assert "Section Title" in content

def test_logger_terminal_output(capsys, tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.info("Terminal info")
    logger.warning("Terminal warning")
    logger.error("Terminal error")
    logger.success("[DONE] Downloaded file.csv")
    logger.section("Terminal Section")
    logger.close()
    out = capsys.readouterr().out
    assert "Terminal info" in out
    assert "Terminal warning" in out
    assert "Terminal error" in out
    assert "Section" in out

def test_logger_strip_ansi():
    logger = CustomLogger()
    ansi_line = "\033[31mRed Text\033[0m"
    clean = logger.strip_ansi(ansi_line)
    assert clean == "Red Text"

def test_logger_append_log_to_file(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.append_log_to_file("Line to file")
    with open(log_file, "r", encoding="utf-8") as f:
        assert "Line to file" in f.read()

def test_logger_get_buffers(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.info("Buffer info")
    terminal, file, banner = logger.get_buffers()
    assert any("Buffer info" in item[1] for item in terminal)
    assert any("Buffer info" in item[1] for item in file)
    assert isinstance(banner, str)

def test_logger_format(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    record = logging.LogRecord(name="test", level=logging.INFO, pathname="", lineno=0, msg="format test", args=(), exc_info=None)
    formatted = logger.format(record)
    assert "format test" in formatted

def test_logger_redraw_logs(tmp_path, capsys):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.info("Redraw info")
    logger.redraw_logs()
    out = capsys.readouterr().out
    assert "Redraw info" in out

def test_logger_disable_terminal_logging(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.disable_terminal_logging = True
    logger.info("Should not print")
    # Should not raise or print

def test_logger_close(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.close()
    # Should not raise

def test_logger_success_removes_try_and_fail(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.info("[TRY] Downloaded file.csv")
    logger.error("[FAIL] Downloaded file.csv")
    logger.success("[DONE] Downloaded file.csv")
    # After success, [TRY] and [FAIL] for the same file should be removed from terminal buffer
    terminal, _, _ = logger.get_buffers()
    assert not any("[TRY]" in logger.strip_ansi(item[1]) for item in terminal)
    assert not any("[FAIL]" in logger.strip_ansi(item[1]) for item in terminal)
    assert any("[DONE]" in logger.strip_ansi(item[1]) for item in terminal)