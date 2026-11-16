"""Transfer-graph semantics: 24h window, first arrival, path, deadlines."""
from __future__ import annotations

from conftest import batch, ingest, trace


def test_direct_chain_within_window(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
            {"sample_id": "S3", "batch_nos": ["B3"]},
            {"sample_id": "S4", "batch_nos": []},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T10:00:00Z", "2026-01-01T12:00:00Z"),
            batch("B3", "ref", "arch", "2026-01-02T08:00:00Z", "2026-01-02T09:00:00Z"),
        ],
    )
    response = trace(client)
    assert response.status_code == 201, response.text
    body = response.json()
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S2", "S3"]
    assert body["affected_samples"][0]["first_arrival_at"] == "2026-01-01T12:00:00Z"
    assert body["affected_samples"][0]["quarantine_until"] == "2026-01-02T12:00:00Z"
    # Path is a complete chain, starting at the source-carrying batch.
    assert [leg["batch_no"] for leg in body["affected_samples"][1]["path"]] == [
        "B1",
        "B2",
        "B3",
    ]


def test_departure_exactly_24h_later_is_contaminated(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-02T01:00:00Z", "2026-01-02T02:00:00Z"),
        ],
    )
    body = trace(client).json()
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S2"]


def test_departure_after_window_is_not_contaminated(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch(
                "B2",
                "hub",
                "ref",
                "2026-01-02T01:00:00.000001Z",
                "2026-01-02T02:00:00Z",
            ),
        ],
    )
    body = trace(client).json()
    assert body["affected_samples"] == []
    assert body["has_sources"] is True


def test_departure_before_contamination_arrival_is_not_contaminated(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T05:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T04:00:00Z", "2026-01-01T06:00:00Z"),
        ],
    )
    assert trace(client).json()["affected_samples"] == []


def test_earliest_arrival_wins_with_shortest_path_tiebreak(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
            {"sample_id": "S3", "batch_nos": ["B4"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T02:00:00Z", "2026-01-01T05:00:00Z"),
            batch("B3", "hub", "mid", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
            batch("B4", "mid", "ref", "2026-01-01T03:30:00Z", "2026-01-01T04:00:00Z"),
        ],
    )
    body = trace(client).json()
    by_id = {a["sample_id"]: a for a in body["affected_samples"]}
    assert by_id["S2"]["first_arrival_at"] == "2026-01-01T05:00:00Z"
    assert by_id["S2"]["quarantine_until"] == "2026-01-02T05:00:00Z"
    # S3 arrives earlier via B1 -> B3 -> B4.
    assert by_id["S3"]["first_arrival_at"] == "2026-01-01T04:00:00Z"
    assert [leg["batch_no"] for leg in by_id["S3"]["path"]] == ["B1", "B3", "B4"]
    # Result ordering: quarantine deadline, then sample id.
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S3", "S2"]


def test_source_samples_are_not_reported_as_affected(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B1"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),
        ],
    )
    body = trace(client).json()
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S2"]
    assert body["source_sample_ids"] == ["S1"]


def test_no_sources_yields_empty_result(client):
    ingest(
        client,
        sources=[],
        samples=[{"sample_id": "S1", "batch_nos": ["B1"]}],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z")
        ],
    )
    body = trace(client).json()
    assert body["status"] == "complete"
    assert body["has_sources"] is False
    assert body["affected_samples"] == []


def test_source_override_on_trace(client):
    ingest(
        client,
        sources=[],
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
    body = trace(client, source_sample_ids=["S1"]).json()
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S2", "S3"]

    response = trace(client, source_sample_ids=["NOPE"])
    assert response.status_code == 404


def test_cycle_in_values_does_not_loop_forever(client):
    # Batches that depart before the contaminated arrival do not propagate;
    # a backwards-time loop must not be traversed.
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "a", "b", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "b", "a", "2026-01-02T00:00:00Z", "2026-01-02T01:00:00Z"),
        ],
    )
    body = trace(client).json()
    assert [a["sample_id"] for a in body["affected_samples"]] == ["S2"]
    assert len(body["affected_samples"][0]["path"]) == 2


def test_timestamps_normalised_to_utc(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T08:00:00+08:00", "2026-01-01T10:00:00+08:00"),
            batch("B2", "hub", "ref", "2026-01-01T03:00:00Z", "2026-01-01T04:00:00Z"),
        ],
    )
    body = trace(client).json()
    affected = body["affected_samples"][0]
    assert affected["first_arrival_at"] == "2026-01-01T04:00:00Z"
    assert affected["path"][0]["departed_at"] == "2026-01-01T00:00:00Z"
