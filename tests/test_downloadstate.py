import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import os
from DownloadState import DownloadState

def test_state_creation(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=state_file, incremental=False)
    state.save_state()  # Garante que o arquivo será criado
    assert os.path.exists(state_file)
    assert isinstance(state.state, dict)

def test_add_completed_and_failed(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=state_file, incremental=False)
    url = "http://example.com/file.csv"
    state.add_completed(url, "file.csv", 123)
    assert state.is_completed(url)
    state.add_failed(url, "error")
    assert state.state['failed']