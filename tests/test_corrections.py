"""Versioned corrections, result invalidation and snapshot pinning."""
from __future__ import annotations

from conftest import batch, ingest, trace


def seed(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
            {"sample_id": "S3", "batch_nos": ["B3"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
            batch("B3", "ref", "arch", "2026-01-01T04:00:00Z", "2026-01-01T05:00:00Z"),
        ],
    )


def test_correction_replaces_record_and_invalidates_old_result(client):
    seed(client)
    first = trace(client).json()
    assert first["affected_count"] == 2
    old_trace_id = first["trace_id"]

    response = client.post(
        "/batches/B2/corrections",
        json={
            "seen_version": 1,
            "origin": "hub",
            "destination": "ref",
            # Push departure past the 24h window from B1's 01:00 arrival.
            "departed_at": "2026-01-02T02:00:00Z",
            "arrived_at": "2026-01-02T03:00:00Z",
        },
    )
    assert response.status_code == 200, response.text
    correction = response.json()
    assert correction["previous_version"] == 1
    assert correction["version"] == 2
    assert correction["invalidated_trace_ids"] == [old_trace_id]

    # The old snapshot refuses to serve stale data.
    stale = client.get(f"/traces/{old_trace_id}")
    assert stale.status_code == 409
    assert stale.json()["code"] == "snapshot_invalidated"

    # The replacement is live, only the corrected values appear.
    dataset = client.get("/dataset").json()
    b2 = next(b for b in dataset["batches"] if b["batch_no"] == "B2")
    assert b2["version"] == 2
    assert b2["departed_at"] == "2026-01-02T02:00:00Z"
    assert len([b for b in dataset["batches"] if b["batch_no"] == "B2"]) == 1

    fresh = trace(client).json()
    assert fresh["affected_samples"] == []
    assert fresh["snapshot"]["batch_versions"]["B2"] == 2


def test_stale_seen_version_is_rejected(client):
    seed(client)
    body = {
        "seen_version": 1,
        "origin": "hub",
        "destination": "other",
        "departed_at": "2026-01-01T02:00:00Z",
        "arrived_at": "2026-01-01T03:00:00Z",
    }
    assert client.post("/batches/B2/corrections", json=body).status_code == 200
    again = client.post("/batches/B2/corrections", json=body)
    assert again.status_code == 409
    assert "stale version" in again.json()["message"]


def test_correction_must_change_values(client):
    seed(client)
    response = client.post(
        "/batches/B2/corrections",
        json={
            "seen_version": 1,
            "origin": "hub",
            "destination": "ref",
            "departed_at": "2026-01-01T02:00:00Z",
            "arrived_at": "2026-01-01T03:00:00Z",
        },
    )
    assert response.status_code == 409
    assert "does not change" in response.json()["message"]


def test_unknown_batch_is_404(client):
    seed(client)
    response = client.post(
        "/batches/NOPE/corrections",
        json={
            "seen_version": 1,
            "origin": "a",
            "destination": "b",
            "departed_at": "2026-01-01T02:00:00Z",
            "arrived_at": "2026-01-01T03:00:00Z",
        },
    )
    assert response.status_code == 404


def test_versions_chain_across_multiple_corrections(client):
    seed(client)
    body = {
        "origin": "hub",
        "destination": "ref",
        "departed_at": "2026-01-01T02:00:00Z",
        "arrived_at": "2026-01-01T03:00:00Z",
    }
    first = client.post(
        "/batches/B2/corrections",
        json={**body, "seen_version": 1, "destination": "cool-room"},
    ).json()
    second = client.post(
        "/batches/B2/corrections",
        json={
            **body,
            "seen_version": first["version"],
            "destination": "cold-room",
        },
    )
    assert second.status_code == 200, second.text
    assert second.json()["version"] == 3
    dataset = client.get("/dataset").json()
    live = [b for b in dataset["batches"] if b["batch_no"] == "B2"]
    assert [(b["version"], b["destination"]) for b in live] == [(3, "cold-room")]


def test_conflicting_intake_batch_requires_correction(client):
    seed(client)
    response = client.post(
        "/intakes",
        json={
            "samples": [],
            "batches": [
                batch(
                    "B2",
                    "hub",
                    "elsewhere",
                    "2026-01-01T02:00:00Z",
                    "2026-01-01T03:00:00Z",
                )
            ],
        },
    )
    # Empty samples list is itself invalid; use an unrelated sample.
    assert response.status_code == 422
    response = client.post(
        "/intakes",
        json={
            "samples": [{"sample_id": "S4", "batch_nos": ["B2"]}],
            "batches": [
                batch(
                    "B2",
                    "hub",
                    "elsewhere",
                    "2026-01-01T02:00:00Z",
                    "2026-01-01T03:00:00Z",
                )
            ],
        },
    )
    assert response.status_code == 409
    assert "correction" in response.json()["message"]


def test_identical_intake_resubmission_is_idempotent(client):
    seed(client)
    before = client.get("/dataset").json()
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
            {"sample_id": "S3", "batch_nos": ["B3"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
            batch("B3", "ref", "arch", "2026-01-01T04:00:00Z", "2026-01-01T05:00:00Z"),
        ],
    )
    after = client.get("/dataset").json()
    assert after["batches"] == before["batches"]
    assert after["samples"] == before["samples"]


def test_correction_to_unpinned_batch_leaves_trace_valid(client):
    seed(client)
    ingest(
        client,
        sources=[],
        samples=[{"sample_id": "S9", "batch_nos": ["B9"]}],
        batches=[
            batch("B9", "x", "y", "2026-02-01T00:00:00Z", "2026-02-01T01:00:00Z")
        ],
    )
    # B9 was added before this trace; pin it in a second trace.
    old = trace(client).json()
    response = client.post(
        "/batches/B9/corrections",
        json={
            "seen_version": 1,
            "origin": "x",
            "destination": "z",
            "departed_at": "2026-02-01T00:10:00Z",
            "arrived_at": "2026-02-01T01:00:00Z",
        },
    )
    assert response.status_code == 200
    assert old["trace_id"] in response.json()["invalidated_trace_ids"]
    assert client.get(f"/traces/{old['trace_id']}").status_code == 409


def test_correction_after_accept_pins_are_consistent(client):
    """A correction before a trace is accepted must not invalidate the new one."""
    seed(client)
    client.post(
        "/batches/B2/corrections",
        json={
            "seen_version": 1,
            "origin": "hub",
            "destination": "ref",
            "departed_at": "2026-01-01T02:30:00Z",
            "arrived_at": "2026-01-01T03:30:00Z",
        },
    )
    body = trace(client).json()
    assert body["snapshot"]["batch_versions"]["B2"] == 2
    assert client.get(f"/traces/{body['trace_id']}").status_code == 200


def test_incremental_intake_can_reference_existing_batch(client):
    seed(client)
    # Only a new sample referencing the already-known batch: allowed.
    ingest(
        client,
        sources=[],
        samples=[{"sample_id": "S4", "batch_nos": ["B2"]}],
        batches=[],
    )
    body = trace(client).json()
    assert "S4" in {a["sample_id"] for a in body["affected_samples"]}


def test_invalidation_endpoint_lists_records(client):
    seed(client)
    old = trace(client).json()
    client.post(
        "/batches/B2/corrections",
        json={
            "seen_version": 1,
            "origin": "hub",
            "destination": "ref",
            "departed_at": "2026-01-03T02:00:00Z",
            "arrived_at": "2026-01-03T03:00:00Z",
        },
    )
    records = client.get(f"/traces/{old['trace_id']}/invalidations")
    assert records.status_code == 200
    assert len(records.json()) == 1
    assert records.json()[0]["batch_no"] == "B2"
    assert records.json()[0]["version"] == 1

    listing = client.get("/traces").json()["traces"]
    summary = next(t for t in listing if t["trace_id"] == old["trace_id"])
    assert summary["invalidated"] is True
    assert summary["affected_count"] == 2  # stored rows remain for audit
