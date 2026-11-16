"""Contamination reachability.

Model
-----
Samples ride on transfer batches. A batch has an ``origin``, a ``destination``,
a departure (out-of-storage) time and an arrival (into-storage) time, and may
carry several samples.

A batch is **contaminated** when either

* it carries a sample flagged as a contamination source, or
* it departs from a site between the arrival of a contaminated batch at that
  site and 24 hours after that arrival (both boundaries inclusive).

This is the station-level reading of "后续转运须在上一站入库后 24 小时内离库":
contaminated material arriving at a site makes transfers leaving that same site
within the window contaminated too. A chain therefore requires
``previous.destination == next.origin``.

A sample is **affected** when it rides on at least one contaminated batch
(other than being a source itself). Its ``first_arrival_at`` is the earliest
``arrived_at`` among the contaminated batches it rides, its ``path`` is a full
chain of batches from a source-carrying batch to such a batch, and its
``quarantine_until`` is ``first_arrival_at + 24h``.

Algorithm
---------
Contaminated arrivals strictly precede departures along a chain
(``a.arrived_at <= b.departed_at < b.arrived_at``), so batches are visited once
in ``(arrived_at, batch_no)`` order and the closure is computed in that pass.

For each site the contaminated arrivals are kept in a range-minimum structure
keyed by ``(path length, discovery order)``. A departure at ``t`` queries the
window ``[t - 24h, t]`` and inherits the shortest path among eligible arrivals;
an earlier arrival is *not* assumed to be better, because its window may have
expired. This keeps the worst case at ``O(batches * log batches)`` for
aggregation plus the cost of scanning a site's arrivals within the window.

Determinism: ties are broken by shortest path, then by discovery order (the
``(arrived_at, batch_no)`` sort), so repeated calls on one snapshot return
identical results.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from typing import Any, Iterable

TRANSFER_WINDOW = timedelta(hours=24)


class TraceFailure(Exception):
    """The dataset is inconsistent and a trustworthy result is impossible."""


def _parse(value: str | datetime) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(value)


class _Chain:
    """One contaminated arrival, linked to the arrival it descended from."""

    __slots__ = ("leg", "prev", "depth", "arrived_at", "seq", "_path")

    def __init__(
        self,
        leg: dict[str, Any],
        prev: "_Chain | None",
        depth: int,
        seq: int,
    ) -> None:
        self.leg = leg
        self.prev = prev
        self.depth = depth
        self.arrived_at = leg["arrived_at"]
        self.seq = seq
        self._path: tuple[dict[str, Any], ...] | None = None

    def path(self) -> tuple[dict[str, Any], ...]:
        if self._path is None:
            chain: list[dict[str, Any]] = []
            node: _Chain | None = self
            while node is not None:
                chain.append(node.leg)
                node = node.prev
            chain.reverse()
            self._path = tuple(chain)
        return self._path


def _better(candidate: _Chain | None, current: _Chain | None) -> bool:
    """Shorter path first; then the chain discovered earlier."""
    if candidate is None:
        return False
    if current is None:
        return True
    if candidate.depth != current.depth:
        return candidate.depth < current.depth
    return candidate.seq < current.seq


class _SiteArrivals:
    """Append-only, range-minimum query over arrivals at one site.

    ``times`` is sorted because arrivals are appended in chronological order.
    The segment tree stores the chain with the best ``(depth, seq)`` key.
    """

    def __init__(self) -> None:
        self.times: list[datetime] = []
        self.chains: list[_Chain] = []
        self._size = 1
        self._tree: list[_Chain | None] = [None, None]

    def append(self, chain: _Chain) -> None:
        index = len(self.chains)
        self.chains.append(chain)
        self.times.append(chain.arrived_at)
        if index >= self._size:
            self._grow()
        pos = self._size + index
        self._tree[pos] = chain
        pos //= 2
        while pos:
            left = self._tree[2 * pos]
            right = self._tree[2 * pos + 1]
            self._tree[pos] = right if _better(right, left) else left
            pos //= 2

    def _grow(self) -> None:
        size = self._size * 2
        tree: list[_Chain | None] = [None] * (2 * size)
        tree[size : size + len(self.chains)] = self.chains
        for pos in range(size - 1, 0, -1):
            left = tree[2 * pos]
            right = tree[2 * pos + 1]
            tree[pos] = right if _better(right, left) else left
        self._size = size
        self._tree = tree

    def best(self, departed_at: datetime) -> _Chain | None:
        """Best chain arriving in ``[departed_at - 24h, departed_at]``."""
        end = bisect_right(self.times, departed_at)
        if end == 0:
            return None
        start = bisect_left(
            self.times, departed_at - TRANSFER_WINDOW, 0, end
        )
        if start >= end:
            return None
        best = _range_best(self._tree, self._size, start, end)
        return best


def _range_best(
    tree: list[_Chain | None], size: int, start: int, end: int
) -> _Chain | None:
    """Minimum over leaves ``[start, end)`` in a segment tree."""
    start += size
    end += size
    best: _Chain | None = None
    while start < end:
        if start & 1:
            if _better(tree[start], best):
                best = tree[start]
            start += 1
        if end & 1:
            end -= 1
            if _better(tree[end], best):
                best = tree[end]
        start //= 2
        end //= 2
    return best


def compute(
    *,
    samples: Iterable[dict[str, Any]],
    batches: Iterable[dict[str, Any]],
    source_sample_ids: Iterable[str],
) -> dict[str, Any]:
    """Compute affected samples for one consistent snapshot.

    Args:
        samples: rows with ``sample_id``.
        batches: rows with ``batch_no``, ``origin``, ``destination``,
            ``departed_at``, ``arrived_at``, ``version`` and ``samples``
            (sample ids riding the batch).
        source_sample_ids: contamination sources, already validated.

    Returns:
        ``{"has_sources": bool, "source_sample_ids": [...],
        "affected_samples": [...]}`` sorted by first arrival then sample id.

    Raises:
        TraceFailure: malformed data or unknown source.
    """
    sources = sorted(set(source_sample_ids))
    source_set = set(sources)

    known_samples = {row["sample_id"] for row in samples}
    unknown = sorted(source_set - known_samples)
    if unknown:
        raise TraceFailure(f"unknown source sample(s): {', '.join(unknown)}")

    legs: list[dict[str, Any]] = []
    for row in batches:
        try:
            departed = _parse(row["departed_at"])
            arrived = _parse(row["arrived_at"])
        except (TypeError, ValueError) as exc:
            raise TraceFailure(
                f"batch {row.get('batch_no')!r} has an invalid timestamp"
            ) from exc
        if departed >= arrived:
            raise TraceFailure(
                f"batch {row['batch_no']!r} does not arrive after departure"
            )
        riders = tuple(sorted(row.get("samples", ())))
        legs.append(
            {
                "batch_no": row["batch_no"],
                "origin": row["origin"],
                "destination": row["destination"],
                "departed_at": departed,
                "arrived_at": arrived,
                "version": int(row["version"]),
                "samples": riders,
                "seed": any(sample_id in source_set for sample_id in riders),
            }
        )

    sites: dict[str, _SiteArrivals] = {}
    first_by_sample: dict[str, _Chain] = {}
    seq = 0

    for leg in sorted(legs, key=lambda item: (item["arrived_at"], item["batch_no"])):
        upstream: _Chain | None = None
        if not leg["seed"]:
            site = sites.get(leg["origin"])
            if site is not None:
                upstream = site.best(leg["departed_at"])
        if upstream is None and not leg["seed"]:
            continue

        chain = _Chain(
            leg,
            upstream,
            1 if upstream is None else upstream.depth + 1,
            seq,
        )
        seq += 1

        for sample_id in leg["samples"]:
            if sample_id in source_set:
                continue
            current = first_by_sample.get(sample_id)
            if current is None:
                first_by_sample[sample_id] = chain
            elif chain.arrived_at < current.arrived_at:
                first_by_sample[sample_id] = chain
            elif chain.arrived_at == current.arrived_at and _better(chain, current):
                first_by_sample[sample_id] = chain

        sites.setdefault(leg["destination"], _SiteArrivals()).append(chain)

    affected = []
    for sample_id in sorted(
        first_by_sample, key=lambda sid: (first_by_sample[sid].arrived_at, sid)
    ):
        chain = first_by_sample[sample_id]
        affected.append(
            {
                "sample_id": sample_id,
                "first_arrival_at": chain.arrived_at,
                "quarantine_until": chain.arrived_at + TRANSFER_WINDOW,
                "path": [
                    {
                        "batch_no": leg["batch_no"],
                        "origin": leg["origin"],
                        "destination": leg["destination"],
                        "departed_at": leg["departed_at"],
                        "arrived_at": leg["arrived_at"],
                        "version": leg["version"],
                    }
                    for leg in chain.path()
                ],
            }
        )

    return {
        "has_sources": bool(sources),
        "source_sample_ids": sources,
        "affected_samples": affected,
    }
