"""Regression test for the headline anti-blocking claim.

README.md advertises: "User-Agent Rotation: Randomizes user-agent strings on
each request and after 403 errors."

That was false. The User-Agent was drawn once, *above* the retry loop in
``ScraperLib._download_file_ray``, so every retry after a 403 replayed the exact
identity the server had just blocked. Measured against a local server: 4
requests, 1 distinct User-Agent.

This test stands a real HTTP server up, answers 403 to the first three requests
for the file, runs the real ``ScraperLib.run()`` against it, and asserts that the
server saw more than one distinct User-Agent. It fails on the pre-fix code.
"""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from scraper_lib import ScraperLib

INDEX_HTML = b'<html><body><a href="/file1.csv">file1</a></body></html>'
PAYLOAD = b"col\n1\n"
# Five rejections force six requests. With the eight shipped user-agents, a
# passing run that drew the same string six times in a row has probability
# 8**-5 (~3e-5), so this is not a flaky assertion.
DENY_FIRST_N = 5


class _Recorder:
    """Shared, thread-safe record of what the server was asked for."""

    def __init__(self, deny_first_n=DENY_FIRST_N):
        self.lock = threading.Lock()
        self.file_user_agents = []
        self.deny_first_n = deny_first_n
        self.denied = 0

    def register(self, user_agent):
        """Record a hit on the file and say whether to answer 403."""
        with self.lock:
            self.file_user_agents.append(user_agent)
            if self.denied < self.deny_first_n:
                self.denied += 1
                return True
            return False


def _make_handler(recorder):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # silence the stderr access log
            pass

        def _send(self, code, body=b"", content_type="text/html"):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def do_GET(self):
            if self.path.startswith("/file1.csv"):
                deny = recorder.register(self.headers.get("User-Agent", "<none>"))
                if deny:
                    self._send(403)
                else:
                    self._send(200, PAYLOAD, "text/csv")
                return
            self._send(200, INDEX_HTML)

    return Handler


@pytest.fixture
def flaky_server():
    """A server that 403s the first `DENY_FIRST_N` requests for the file."""
    recorder = _Recorder()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(recorder))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/", recorder
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_user_agent_rotates_across_retries_after_403(flaky_server, tmp_path):
    base_url, recorder = flaky_server

    scraper = ScraperLib(
        base_url=base_url,
        file_patterns=[".csv"],
        download_dir=str(tmp_path / "data"),
        incremental=False,
        max_files=1,
        max_concurrent=1,
        state_file=str(tmp_path / "state.json"),
        log_file=str(tmp_path / "log.log"),
        output_dir=str(tmp_path / "output"),
        disable_logging=True,
        disable_terminal_logging=True,
        disable_progress_bar=True,
        chunk_size=1024,
        initial_delay=0.01,
        max_delay=0.05,
        max_retries=6,
    )
    scraper.run()

    attempts = recorder.file_user_agents
    assert len(attempts) > DENY_FIRST_N, (
        f"server saw only {len(attempts)} request(s) for the file; expected the "
        f"{DENY_FIRST_N} rejected attempts plus at least one retry"
    )

    distinct = set(attempts)
    assert len(distinct) > 1, (
        f"{len(attempts)} requests were sent with {len(distinct)} distinct "
        f"User-Agent(s). README.md claims the User-Agent is randomized on each "
        f"request and after 403 errors, so retries must not replay the identity "
        f"the server just blocked. Saw: {sorted(distinct)}"
    )
