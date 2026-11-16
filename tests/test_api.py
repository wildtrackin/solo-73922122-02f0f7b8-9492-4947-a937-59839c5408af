"""API surface: docs, payload validation, batch shapes, errors, restart."""
from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from conftest import batch, ingest, trace

from app.main import create_app
from app.store import Store


def test_openapi_and_docs_available(client):
    schema = client.get("/openapi.json")
    assert schema.status_code == 200
    paths = schema.json()["paths"]
    assert "/intakes" in paths
    assert "/traces" in paths
    assert "/batches/{batch_no}/corrections" in paths
    docs = client.get("/docs")
    assert docs.status_code == 200
    assert "text/html" in docs.headers["content-type"]


def test_trace_accepts_empty_body(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "a", "b", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "b", "c", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
        ],
    )
    assert client.post("/traces").status_code == 201
    assert client.post("/traces", json={}).status_code == 201


def test_naive_timestamp_treated_as_utc(client):
    response = client.post(
        "/intakes",
        json={
            "samples": [{"sample_id": "S1", "batch_nos": ["B1"]}],
            "batches": [
                {
                    "batch_no": "B1",
                    "origin": "a",
                    "destination": "b",
                    "departed_at": "2026-01-01T00:00:00",
                    "arrived_at": "2026-01-01T01:00:00",
                }
            ],
        },
    )
    assert response.status_code == 201
    b1 = client.get("/dataset").json()["batches"][0]
    assert b1["departed_at"] == "2026-01-01T00:00:00Z"


def test_reversed_batch_times_rejected(client):
    response = client.post(
        "/intakes",
        json={
            "samples": [{"sample_id": "S1", "batch_nos": ["B1"]}],
            "batches": [
                batch("B1", "a", "b", "2026-01-01T02:00:00Z", "2026-01-01T01:00:00Z")
            ],
        },
    )
    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"


def test_unknown_batch_reference_rejected(client):
    response = client.post(
        "/intakes",
        json={
            "samples": [{"sample_id": "S1", "batch_nos": ["missing"]}],
            "batches": [],
        },
    )
    assert response.status_code == 422
    assert "unknown batch" in response.text


def test_duplicate_sample_rejected(client):
    response = client.post(
        "/intakes",
        json={
            "samples": [
                {"sample_id": "S1", "batch_nos": []},
                {"sample_id": "S1", "batch_nos": []},
            ]
        },
    )
    assert response.status_code == 422


def test_inline_batches_are_supported(client):
    response = client.post(
        "/intakes",
        json={
            "samples": [
                {
                    "sample_id": "S1",
                    "contaminated": True,
                    "batches": [
                        batch(
                            "B1",
                            "a",
                            "b",
                            "2026-01-01T00:00:00Z",
                            "2026-01-01T01:00:00Z",
                        )
                    ],
                },
                {
                    "sample_id": "S2",
                    "batches": [
                        batch(
                            "B1",
                            "a",
                            "b",
                            "2026-01-01T00:00:00Z",
                            "2026-01-01T01:00:00Z",
                        )
                    ],
                },
            ]
        },
    )
    assert response.status_code == 201, response.text
    body = trace(client).json()
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S2"]


def test_contaminated_flag_in_sample_body(client):
    response = client.post(
        "/intakes",
        json={
            "samples": [
                {"sample_id": "S1", "contaminated": True, "batch_nos": ["B1"]},
                {"sample_id": "S2", "batch_nos": ["B2"]},
            ],
            "batches": [
                batch("B1", "a", "b", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
                batch("B2", "b", "c", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
            ],
        },
    )
    assert response.status_code == 201
    assert response.json()["source_sample_ids"] == ["S1"]


def test_dataset_404_before_intake(client):
    assert client.get("/dataset").status_code == 404
    assert client.post("/traces", json={}).status_code == 404


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_data_survives_restart(tmp_path):
    path = str(tmp_path / "persist.db")
    with TestClient(create_app(path)) as c:
        ingest(
            c,
            sources=["S1"],
            samples=[
                {"sample_id": "S1", "batch_nos": ["B1"]},
                {"sample_id": "S2", "batch_nos": ["B2"]},
            ],
            batches=[
                batch("B1", "a", "b", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
                batch("B2", "b", "c", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
            ],
        )
        trace_id = trace(c).json()["trace_id"]

    with TestClient(create_app(path)) as c:
        dataset = c.get("/dataset").json()
        assert len(dataset["batches"]) == 2
        assert c.get(f"/traces/{trace_id}").status_code == 200


def test_pending_trace_from_crash_is_marked_failed(tmp_path):
    path = str(tmp_path / "crash.db")
    store = Store(path)
    store.ingest(
        samples=[{"sample_id": "S1", "contaminated": True, "batch_nos": []}],
        batches=[],
        source_sample_ids=[],
    )
    trace_id, _snapshot, _sources = store.accept_trace(sources=["S1"])
    # Simulate a process dying before publication (startup recovery ignores
    # very recent pending traces, so age this one past the grace period).
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE traces SET accepted_at='2000-01-01T00:00:00.000000+00:00'"
            " WHERE id=?",
            (trace_id,),
        )
    assert store.mark_interrupted() == 1

    with TestClient(create_app(path)) as c:
        row = next(
            t for t in c.get("/traces").json()["traces"] if t["trace_id"] == trace_id
        )
        assert row["status"] == "failed"
        assert row["affected_count"] == 0


def test_sqlite_wal_file_layout(tmp_path):
    path = tmp_path / "wal.db"
    store = Store(str(path))
    store.ingest(
        samples=[{"sample_id": "S1", "contaminated": False, "batch_nos": []}],
        batches=[],
        source_sample_ids=[],
    )
    with sqlite3.connect(path) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
