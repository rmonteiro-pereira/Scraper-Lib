import os
from ScraperLib import DownloadState

def test_init_and_generate(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    assert os.path.exists(state_file)
    assert isinstance(state.state, dict)
    assert 'completed' in state.state

def test_load_and_save_state(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    url = "http://example.com/file.csv"
    state.add_completed(url, "file.csv", 123)
    state.save_state()
    state2 = DownloadState(state_file=str(state_file), incremental=True)
    file_id = state2.get_file_id(url)
    assert file_id in state2.state['completed']

def test_add_completed_and_is_completed(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    url = "http://example.com/file.csv"
    state.add_completed(url, "file.csv", 123)
    assert state.is_completed(url)
    file_id = state.get_file_id(url)
    assert file_id in state.state['completed']

def test_add_failed(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    url = "http://example.com/file.csv"
    state.add_failed(url, "error msg")
    file_id = state.get_file_id(url)
    assert file_id in state.state['failed']
    assert state.state['failed'][file_id]['error'] == "error msg"

def test_add_delay_success_and_failed(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    state.add_delay(1.5, success=True)
    state.add_delay(2.5, success=False)
    assert state.state['delays_success']
    assert state.state['delays_failed']

def test_get_file_id_is_md5(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    url = "http://example.com/file.csv"
    file_id = state.get_file_id(url)
    import hashlib
    assert file_id == hashlib.md5(url.encode()).hexdigest()

def test_atomic_file_operation(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    called = []
    def op():
        called.append(True)
    state._atomic_file_operation(op)
    assert called

def test_state_property(tmp_path):
    state_file = tmp_path / "state.json"
    state = DownloadState(state_file=str(state_file), incremental=False)
    assert isinstance(state.state, dict)