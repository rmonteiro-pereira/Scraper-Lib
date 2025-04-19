import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import os
from CustomLogger import CustomLogger

def test_logger_file(tmp_path):
    log_file = tmp_path / "test.log"
    logger = CustomLogger(log_file_path=str(log_file))
    logger.info("info test")
    logger.success("success test")
    logger.error("error test")
    logger.warning("warning test")
    logger.section("section test")
    assert os.path.exists(log_file)
    with open(log_file, "r", encoding="utf-8") as f:
        content = f.read()
        assert "info test" in content
        assert "success test" in content
        assert "error test" in content
        assert "warning test" in content