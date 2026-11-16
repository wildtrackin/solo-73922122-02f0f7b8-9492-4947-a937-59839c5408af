from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import create_app  # noqa: E402


def batch(no, origin, dest, dep, arr):
    return {
        "batch_no": no,
        "origin": origin,
        "destination": dest,
        "departed_at": dep,
        "arrived_at": arr,
    }


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as test_client:
        yield test_client


def ingest(client, *, sources, samples, batches):
    response = client.post(
        "/intakes",
        json={
            "source_sample_ids": sources,
            "samples": samples,
            "batches": batches,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def trace(client, **kwargs):
    return client.post("/traces", json=kwargs)
