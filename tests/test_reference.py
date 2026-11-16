"""Randomised comparison against a brute-force reference implementation."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from app import tracing

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def brute_force(samples, batches, sources):
    """Direct O(n^2) scan mirroring the documented selection rule.

    Eligible upstream chains are those arriving in ``[departed - 24h,
    departed]``; the chosen one has the shortest path, then the earliest
    discovery order (``(arrived_at, batch_no)`` processing).
    """
    source_set = set(sources)
    legs = []
    for row in batches:
        legs.append(
            {
                "batch_no": row["batch_no"],
                "origin": row["origin"],
                "destination": row["destination"],
                "departed_at": datetime.fromisoformat(row["departed_at"]),
                "arrived_at": datetime.fromisoformat(row["arrived_at"]),
                "version": 1,
                "samples": tuple(row["samples"]),
            }
        )
    legs.sort(key=lambda l: (l["arrived_at"], l["batch_no"]))

    chains: list[dict] = []
    first: dict[str, dict] = {}
    seq = 0
    for leg in legs:
        upstream = None
        if not source_set.intersection(leg["samples"]):
            candidates = [
                chain
                for chain in chains
                if chain["destination"] == leg["origin"]
                and leg["departed_at"] - timedelta(hours=24)
                <= chain["arrived_at"]
                <= leg["departed_at"]
            ]
            if not candidates:
                continue
            upstream = min(candidates, key=lambda c: (c["depth"], c["seq"]))
        chain = {
            "leg": leg,
            "prev": upstream,
            "depth": 1 if upstream is None else upstream["depth"] + 1,
            "arrived_at": leg["arrived_at"],
            "destination": leg["destination"],
            "seq": seq,
            "path": (
                (leg,)
                if upstream is None
                else upstream["path"] + (leg,)
            ),
        }
        seq += 1
        chains.append(chain)
        for sample_id in leg["samples"]:
            if sample_id in source_set:
                continue
            current = first.get(sample_id)
            if current is None or (
                chain["arrived_at"],
                chain["depth"],
                chain["seq"],
            ) < (current["arrived_at"], current["depth"], current["seq"]):
                first[sample_id] = chain

    out = []
    for sample_id in sorted(
        first, key=lambda sid: (first[sid]["arrived_at"], sid)
    ):
        chain = first[sample_id]
        out.append(
            (
                sample_id,
                chain["arrived_at"].isoformat(),
                tuple(leg["batch_no"] for leg in chain["path"]),
            )
        )
    return out


def make_case(rng: random.Random):
    sites = ["a", "b", "c", "d"]
    count = rng.randint(2, 9)
    batches = []
    for i in range(count):
        origin = rng.choice(sites)
        destination = rng.choice([s for s in sites if s != origin])
        start_min = rng.randint(0, 60 * 40)
        duration = rng.randint(10, 600)
        departed = BASE + timedelta(minutes=start_min)
        arrived = departed + timedelta(minutes=duration)
        batches.append(
            {
                "batch_no": f"B{i:02d}",
                "origin": origin,
                "destination": destination,
                "departed_at": departed.isoformat(),
                "arrived_at": arrived.isoformat(),
                "version": 1,
                "samples": [f"S{rng.choice(range(4))}"],
            }
        )
    samples = [{"sample_id": f"S{i}"} for i in range(4)]
    sources = rng.sample([f"S{i}" for i in range(4)], rng.randint(1, 2))
    return samples, batches, sources


def test_randomised_against_bruteforce():
    rng = random.Random(20261116)
    for case in range(400):
        samples, batches, sources = make_case(rng)
        expected = brute_force(samples, batches, sources)
        result = tracing.compute(
            samples=samples, batches=batches, source_sample_ids=sources
        )
        actual = [
            (
                item["sample_id"],
                item["first_arrival_at"].isoformat(),
                tuple(leg["batch_no"] for leg in item["path"]),
            )
            for item in result["affected_samples"]
        ]
        assert actual == expected, f"case {case} mismatch:\n{batches}\n{sources}"


def test_every_path_is_time_respecting():
    rng = random.Random(7)
    for _ in range(200):
        samples, batches, sources = make_case(rng)
        result = tracing.compute(
            samples=samples, batches=batches, source_sample_ids=sources
        )
        by_no = {b["batch_no"]: b for b in batches}
        for item in result["affected_samples"]:
            legs = item["path"]
            first = legs[0]
            assert set(by_no[first["batch_no"]]["samples"]) & set(sources)
            for prev, nxt in zip(legs, legs[1:]):
                assert prev["destination"] == nxt["origin"]
                gap = nxt["departed_at"] - prev["arrived_at"]
                assert timedelta(0) <= gap <= timedelta(hours=24)
            assert legs[-1]["arrived_at"] == item["first_arrival_at"]
            assert (
                item["quarantine_until"] - item["first_arrival_at"]
                == timedelta(hours=24)
            )
