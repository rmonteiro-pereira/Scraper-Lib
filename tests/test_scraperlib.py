import pytest
from ScraperLib import ScraperLib


@pytest.fixture
def scraper(tmp_path):
    return ScraperLib(
        base_url="https://example.com",
        file_patterns=[".csv"],
        download_dir=str(tmp_path / "data"),
        incremental=False,
        max_files=1,
        max_concurrent=1,
        headers=None,
        user_agents=None,
        state_file=str(tmp_path / "state.json"),
        log_file=str(tmp_path / "log.log"),
        report_prefix="test_report",
        disable_logging=True,
        disable_terminal_logging=True,
        dataset_name="TEST",
        disable_progress_bar=True,
        output_dir=str(tmp_path / "output"),
        max_old_logs=2,
        max_old_runs=2,
        ray_instance=None,
        chunk_size=1024,
        initial_delay=0.1,
        max_delay=1.0,
        max_retries=1,
    )

def test_parse_chunk_size():
    assert ScraperLib._parse_chunk_size("1kb") == 1024
    assert ScraperLib._parse_chunk_size("1mb") == 1024**2
    assert ScraperLib._parse_chunk_size("1gb") == 1024**3
    assert ScraperLib._parse_chunk_size("1024") == 1024
    with pytest.raises(ValueError):
        ScraperLib._parse_chunk_size("invalid")

def test_safe_copy2(tmp_path, scraper):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("hello")
    assert scraper._safe_copy2(str(src), str(dst))
    assert dst.read_text() == "hello"

def test_rotate_and_limit_files(tmp_path, scraper):
    base = tmp_path / "file"
    ext = ".log"
    # Create 5 files
    for i in range(5):
        (tmp_path / f"file.{i}{ext}").write_text("x")
    (tmp_path / f"file{ext}").write_text("latest")
    scraper._rotate_and_limit_files(str(base), ext, 2)
    files = list(tmp_path.glob("file*"))
    # Should keep only 2 old files + the latest
    assert len(files) <= 3

def test_extract_file_links(scraper):
    html = """
    <html>
        <body>
            <a href="file1.csv">file1</a>
            <a href="file2.txt">file2</a>
            <a href="file3.csv">file3</a>
        </body>
    </html>
    """
    links = scraper._extract_file_links(html, "https://example.com", [".csv"])
    assert "https://example.com/file1.csv" in links
    assert "https://example.com/file3.csv" in links
    assert all(link.endswith(".csv") for link in links)

def test_get_page_content(monkeypatch, scraper):
    class DummyResponse:
        def __init__(self, text):
            self.text = text
        def raise_for_status(self): pass
    class DummySession:
        def __init__(self): self.headers = {}
        def get(self, url, timeout): return DummyResponse("ok")
    monkeypatch.setattr("requests.Session", lambda: DummySession())
    content = scraper._get_page_content("http://test", None, None)
    assert content == "ok"

def test_generate_report(tmp_path, scraper):
    # Minimal state and results for coverage
    class DummyState:
        state = {
            "delays_success": [{"value": 1, "timestamp": "2024-01-01T00:00:00"}],
            "delays_failed": [],
            "stats": {"total_bytes": 1024, "start_time": "2024-01-01T00:00:00", "last_update": "2024-01-01T00:01:00"}
        }
    results = [{"status": "success", "file": "f.csv", "logs": []}]
    scraper.output_dir = str(tmp_path)
    scraper._generate_report(results, DummyState(), report_prefix="test", logger=None, output_dir=str(tmp_path), max_old_runs=1)
    report_dir = tmp_path / "reports"
    assert any(f.name.startswith("test") and f.suffix == ".json" for f in report_dir.iterdir())

def test_parallel_download_with_ray_empty(scraper):
    # Should handle empty file list gracefully
    results = scraper._parallel_download_with_ray([])
    assert results == []

def test_init_sets_attributes(scraper):
    assert scraper.base_url == "https://example.com"
    assert scraper.file_patterns == [".csv"]
    assert scraper.max_files == 1

def test_cli_exists():
    assert hasattr(ScraperLib, "cli")