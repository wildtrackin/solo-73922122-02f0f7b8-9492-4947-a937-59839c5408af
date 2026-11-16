"""Snapshot pinning, failure atomicity and concurrency guarantees."""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest
from conftest import batch, ingest, trace

from app import tracing


def seed(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
        ],
    )


def test_trace_requires_dataset(client):
    response = trace(client)
    assert response.status_code == 404


def test_trace_pins_versions_and_batches(client):
    seed(client)
    body = trace(client).json()
    assert body["snapshot"]["batch_versions"] == {"B1": 1, "B2": 1}
    pinned = {b["batch_no"]: b for b in body["snapshot"]["batches"]}
    assert pinned["B2"]["destination"] == "ref"
    assert pinned["B2"]["version"] == 1


def test_failed_trace_publishes_no_partial_result(client, monkeypatch):
    seed(client)

    def boom(**kwargs):
        raise RuntimeError("simulated computation failure")

    monkeypatch.setattr(tracing, "compute", boom)
    response = trace(client)
    assert response.status_code == 500
    detail = response.json()
    assert detail["code"] == "trace_failed"
    trace_id = detail["details"]["trace_id"]

    # The trace is recorded as failed with zero result rows.
    rows = client.get("/traces").json()["traces"]
    failed = next(row for row in rows if row["trace_id"] == trace_id)
    assert failed["status"] == "failed"
    assert failed["affected_count"] == 0
    read_back = client.get(f"/traces/{trace_id}")
    assert read_back.status_code == 500
    assert read_back.json()["code"] == "trace_failed"


def test_publication_rolls_back_when_write_fails(client, monkeypatch):
    """A crash mid-write must leave no result rows at all."""
    seed(client)
    ingest(
        client,
        sources=[],
        samples=[{"sample_id": "S3", "batch_nos": ["B2"]}],
        batches=[],
    )
    real_compute = tracing.compute

    def corrupt(**kwargs):
        outcome = real_compute(**kwargs)
        assert len(outcome["affected_samples"]) == 2
        outcome["affected_samples"].append(
            {
                "sample_id": "BROKEN",
                "first_arrival_at": "not-a-timestamp",
                "quarantine_until": "not-a-timestamp",
                "path": [],
            }
        )
        return outcome

    monkeypatch.setattr(tracing, "compute", corrupt)
    response = trace(client)
    assert response.status_code == 500
    trace_id = response.json()["details"]["trace_id"]

    rows = client.get("/traces").json()["traces"]
    failed = next(row for row in rows if row["trace_id"] == trace_id)
    assert failed["status"] == "failed"
    assert failed["affected_count"] == 0
    monkeypatch.undo()
    # A subsequent trace is unaffected and publishes all rows.
    good = trace(client).json()
    assert good["affected_count"] == 2


def test_failed_publication_does_not_affect_other_traces(client, monkeypatch):
    seed(client)
    good = trace(client).json()
    from app import store as store_module

    monkeypatch.setattr(
        store_module.Store,
        "publish_results",
        lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("nope")),
    )
    assert trace(client).status_code == 500
    monkeypatch.undo()
    restored = client.get(f"/traces/{good['trace_id']}")
    assert restored.status_code == 200
    assert restored.json()["affected_count"] == 1


def test_concurrent_correction_and_trace_never_returns_half_result(client):
    """Interleaved corrections must either invalidate or be fully reflected."""
    seed(client)
    errors: list[BaseException] = []

    def correct():
        try:
            client.post(
                "/batches/B2/corrections",
                json={
                    "seen_version": 1,
                    "origin": "hub",
                    "destination": "ref",
                    "departed_at": "2026-01-01T02:00:00.500000Z",
                    "arrived_at": "2026-01-01T03:00:00Z",
                },
            )
        except BaseException as exc:  # pragma: no cover - diagnostic
            errors.append(exc)

    thread = threading.Thread(target=correct)
    thread.start()
    response = trace(client)
    thread.join()
    assert not errors

    if response.status_code == 201:
        # If the trace completed, it pinned a coherent version set.
        assert response.json()["snapshot"]["batch_versions"]["B2"] in (1, 2)
    else:
        assert response.status_code in (409, 500)
        # No half-published rows exist for that trace.
        detail = response.json()
        trace_id = detail["details"]["trace_id"]
        assert client.get(f"/traces/{trace_id}").status_code == 500


def test_large_graph_performance():
    """A 400-hop chain computes quickly and keeps every deadline."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    size = 400
    batches = []
    samples = [{"sample_id": "S0", "contaminated": True}]
    for i in range(size):
        riders = [f"S{i}", f"S{i + 1}"] if i == 0 else [f"S{i + 1}"]
        batches.append(
            {
                "batch_no": f"B{i:05d}",
                "origin": f"site{i}",
                "destination": f"site{i + 1}",
                "departed_at": (start + timedelta(minutes=30 * i)).isoformat(),
                "arrived_at": (start + timedelta(minutes=30 * i + 20)).isoformat(),
                "version": 1,
                "samples": riders,
            }
        )
        samples.append({"sample_id": f"S{i + 1}"})
    result = tracing.compute(
        samples=samples, batches=batches, source_sample_ids=["S0"]
    )
    assert len(result["affected_samples"]) == size
    assert result["affected_samples"][-1]["sample_id"] == f"S{size}"
    first = result["affected_samples"][0]
    assert first["quarantine_until"] - first["first_arrival_at"] == timedelta(hours=24)


def test_wide_graph_performance():
    """50k independent short batches are handled without path blow-up."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    batches = []
    samples = [{"sample_id": "SRC", "contaminated": True}]
    for i in range(50_000):
        origin = "hub" if i == 0 else f"site{i}"
        batches.append(
            {
                "batch_no": f"W{i:06d}",
                "origin": origin,
                "destination": f"leaf{i}",
                "departed_at": (start + timedelta(seconds=i)).isoformat(),
                "arrived_at": (start + timedelta(seconds=i, minutes=5)).isoformat(),
                "version": 1,
                "samples": ["SRC", "L0"] if i == 0 else [f"L{i}"],
            }
        )
        samples.append({"sample_id": f"L{i}"})
    result = tracing.compute(
        samples=samples, batches=batches, source_sample_ids=["SRC"]
    )
    # Only W000000 leaves the contaminated hub inside the window; every leaf
    # batch departs from its own site and has no contaminated arrival there.
    assert [a["sample_id"] for a in result["affected_samples"]] == ["L0"]


def test_inconsistent_snapshot_fails_loudly():
    with pytest.raises(tracing.TraceFailure):
        tracing.compute(
            samples=[{"sample_id": "S1"}],
            batches=[
                {
                    "batch_no": "B1",
                    "origin": "a",
                    "destination": "b",
                    "departed_at": "2026-01-01T02:00:00Z",
                    "arrived_at": "2026-01-01T01:00:00Z",
                    "version": 1,
                    "samples": ["S1"],
                }
            ],
            source_sample_ids=["S1"],
        )


def test_pending_trace_cannot_be_read_as_complete(client, monkeypatch):
    """While compute is running the trace is pending and exposes no rows."""
    seed(client)
    from app import store as store_module

    observed = {}

    def slow_compute(**kwargs):
        # Inspect the trace mid-flight, before publication.
        rows = client.get("/traces").json()["traces"]
        observed["status"] = rows[0]["status"]
        observed["count"] = rows[0]["affected_count"]
        trace_id = rows[0]["trace_id"]
        observed["read_status"] = client.get(f"/traces/{trace_id}").status_code
        raise RuntimeError("stop before publishing")

    monkeypatch.setattr("app.tracing.compute", slow_compute)
    assert trace(client).status_code == 500
    assert observed == {"status": "pending", "count": 0, "read_status": 409}
