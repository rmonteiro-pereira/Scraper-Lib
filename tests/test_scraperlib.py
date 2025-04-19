import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


import os
import pytest
from scraper_lib import ScraperLib

class DummyState:
    def __init__(self):
        self.state = {
            'delays_success': [],
            'delays_failed': [],
            'stats': {'total_bytes': 0, 'start_time': None, 'last_update': None}
        }

@pytest.fixture
def scraper(tmp_path):
    return ScraperLib(
        base_url="http://example.com",
        file_patterns=[".csv"],
        download_dir=tmp_path / "data",
        output_dir=tmp_path / "output",
        log_file=tmp_path / "logs" / "test.log",
        state_file=tmp_path / "state" / "test_state.json",
        disable_logging=True,
        disable_terminal_logging=True,
        max_concurrent=1,
        max_old_logs=2,
        max_old_runs=2
    )

def test_rotate_and_limit_files(scraper, tmp_path):
    # Cria arquivos fake
    base = tmp_path / "output" / "reports" / "testfile"
    os.makedirs(base.parent, exist_ok=True)
    for i in range(5):
        with open(f"{base}.{20220101+i}.json", "w") as f:
            f.write("test")
    with open(f"{base}.json", "w") as f:
        f.write("test")
    scraper._rotate_and_limit_files(str(base), ".json", 2)
    files = [f for f in os.listdir(base.parent) if f.startswith("testfile")]
    assert len(files) <= 3  # 2 antigos + 1 atual

def test_generate_report_empty(scraper, tmp_path):
    dummy_state = DummyState()
    results = []
    scraper._generate_report(results, dummy_state, output_dir=tmp_path / "output", max_old_runs=2)
    report_dir = tmp_path / "output" / "reports"
    assert any(f.endswith(".json") for f in os.listdir(report_dir))