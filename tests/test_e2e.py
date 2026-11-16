"""Run a real uvicorn HTTP server (no TestClient) and exercise it over HTTP."""
from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

from app.main import create_app


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _ServerThread:
    def __init__(self, db_path: Path) -> None:
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            create_app(str(db_path)),
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.server.started:
                return
            if not self.thread.is_alive():
                raise RuntimeError("uvicorn thread exited before startup")
            time.sleep(0.02)
        raise RuntimeError("server did not start")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=15)
        assert not self.thread.is_alive(), "server did not shut down"


@pytest.fixture()
def server(tmp_path):
    instance = _ServerThread(tmp_path / "e2e.db")
    instance.start()
    try:
        yield instance.base_url
    finally:
        instance.stop()


def test_end_to_end_over_http(server):
    with httpx.Client(base_url=server, timeout=10) as http:
        assert http.get("/docs").status_code == 200
        assert http.get("/openapi.json").status_code == 200

        intake = http.post(
            "/intakes",
            json={
                "source_sample_ids": ["S1"],
                "samples": [
                    {"sample_id": "S1", "batch_nos": ["B1"]},
                    {"sample_id": "S2", "batch_nos": ["B2"]},
                    {"sample_id": "S3", "batch_nos": ["B3"]},
                ],
                "batches": [
                    {
                        "batch_no": "B1",
                        "origin": "collection-lab",
                        "destination": "hub-north",
                        "departed_at": "2026-01-01T08:00:00Z",
                        "arrived_at": "2026-01-01T10:00:00Z",
                    },
                    {
                        "batch_no": "B2",
                        "origin": "hub-north",
                        "destination": "reference-lab",
                        "departed_at": "2026-01-01T20:00:00Z",
                        "arrived_at": "2026-01-01T23:00:00Z",
                    },
                    {
                        "batch_no": "B3",
                        "origin": "reference-lab",
                        "destination": "archive",
                        "departed_at": "2026-01-02T08:00:00Z",
                        "arrived_at": "2026-01-02T10:00:00Z",
                    },
                ],
            },
        )
        assert intake.status_code == 201, intake.text

        result = http.post("/traces", json={})
        assert result.status_code == 201, result.text
        body = result.json()
        assert [a["sample_id"] for a in body["affected_samples"]] == ["S2", "S3"]
        assert (
            body["affected_samples"][0]["quarantine_until"]
            == "2026-01-02T23:00:00Z"
        )
        assert len(body["affected_samples"][1]["path"]) == 3
        trace_id = body["trace_id"]

        # Re-reading the pinned snapshot returns the same rows.
        reread = http.get(f"/traces/{trace_id}")
        assert reread.status_code == 200
        assert reread.json()["affected_samples"] == body["affected_samples"]

        correction = http.post(
            "/batches/B3/corrections",
            json={
                "seen_version": 1,
                "origin": "reference-lab",
                "destination": "cold-archive",
                "departed_at": "2026-01-02T09:00:00Z",
                "arrived_at": "2026-01-02T11:00:00Z",
            },
        )
        assert correction.status_code == 200, correction.text
        assert correction.json()["version"] == 2
        assert correction.json()["invalidated_trace_ids"] == [trace_id]

        stale = http.get(f"/traces/{trace_id}")
        assert stale.status_code == 409
        assert stale.json()["code"] == "snapshot_invalidated"

        fresh = http.post("/traces").json()
        affected = {a["sample_id"]: a for a in fresh["affected_samples"]}
        assert affected["S3"]["path"][-1]["destination"] == "cold-archive"
        assert affected["S3"]["path"][-1]["version"] == 2


def test_end_to_end_errors_are_structured(server):
    with httpx.Client(base_url=server, timeout=10) as http:
        assert http.post("/traces", json={}).status_code == 404
        http.post("/intakes", json={"samples": [{"sample_id": "S1"}]})
        unknown = http.post("/traces", json={"source_sample_ids": ["missing"]})
        assert unknown.status_code == 404
        bad = http.post(
            "/intakes",
            json={
                "samples": [{"sample_id": "S2"}],
                "batches": [
                    {
                        "batch_no": "B1",
                        "origin": "a",
                        "destination": "a",
                        "departed_at": "2026-01-01T00:00:00Z",
                        "arrived_at": "2026-01-01T01:00:00Z",
                    }
                ],
            },
        )
        assert bad.status_code == 422
        assert bad.json()["code"] == "validation_error"
