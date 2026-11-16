"""FastAPI application for contamination tracing.

Endpoints
---------
``POST /intakes``                  register samples + transfer batches + sources
``GET  /dataset``                   inspect the active dataset
``POST /batches/{batch_no}/corrections``
                                   replace a batch version and invalidate results
``POST /traces``                   run a trace on a pinned snapshot
``GET  /traces``                    list accepted traces
``GET  /traces/{id}``               re-read a published snapshot result
``GET  /healthz``                   readiness probe
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import tracing
from .schemas import (
    AffectedSample,
    BatchCorrection,
    BatchOut,
    CorrectionResponse,
    DatasetResponse,
    ErrorBody,
    IntakeRequest,
    IntakeResponse,
    InvalidationInfo,
    PathLeg,
    SnapshotInfo,
    TraceListResponse,
    TraceRequest,
    TraceResponse,
    TraceSummary,
)
from .store import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    Store,
    parse_iso,
)

DEFAULT_DB = os.environ.get("TRACE_DB_PATH", "./data/trace.db")


def create_app(db_path: str | None = None) -> FastAPI:
    path = db_path or os.environ.get("TRACE_DB_PATH", DEFAULT_DB)
    store = Store(path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.store = store
        store.mark_interrupted()
        yield

    app = FastAPI(
        title="Sample Contamination Trace API",
        version="1.0.0",
        description=(
            "Trace contamination reachability across sample transfer batches.\n\n"
            "* A transfer batch is contaminated when it carries a source sample,"
            " or when it leaves a site within 24 hours of a contaminated batch"
            " arriving there.\n"
            "* A sample is affected when it rides on a contaminated batch. Its"
            " reported path is a full chain from a source-carrying batch,"
            " `first_arrival_at` is the earliest contaminated arrival with the"
            " sample, and `quarantine_until` is that arrival + 24 hours.\n"
            "* Corrections are versioned (`seen_version` optimistic lock),"
            " replace the old record and invalidate snapshots pinned to it.\n"
            "* `POST /traces` pins a snapshot at acceptance time and publishes"
            " results only when complete; failed traces expose no partial rows."
        ),
        lifespan=lifespan,
        openapi_tags=[
            {"name": "intake", "description": "Register samples and batches."},
            {"name": "corrections", "description": "Versioned batch replacement."},
            {"name": "traces", "description": "Contamination trace snapshots."},
        ],
    )
    app.state.store = store

    # ------------------------------------------------------------- error shape

    def error(
        status_code: int, code: str, message: str, details: Any = None
    ) -> JSONResponse:
        body = ErrorBody(code=code, message=message, details=details)
        return JSONResponse(
            status_code=status_code, content=body.model_dump(exclude_none=True)
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return error(
            422,
            "validation_error",
            "request validation failed",
            {"errors": _jsonable_errors(exc.errors())},
        )

    @app.exception_handler(HTTPException)
    async def _http_error_handler(
        request: Request, exc: HTTPException
    ) -> JSONResponse:
        if isinstance(exc.detail, dict) and {"code", "message"} <= set(exc.detail):
            return JSONResponse(
                status_code=exc.status_code, content=exc.detail, headers=exc.headers
            )
        return error(exc.status_code, "http_error", str(exc.detail))

    # ---------------------------------------------------------------- helpers

    def _trace_response(bundle: Any) -> TraceResponse:
        trace = bundle["trace"]
        invalidations = bundle["invalidations"]
        if trace["status"] == "complete" and not invalidations:
            affected = [
                AffectedSample(
                    sample_id=row["sample_id"],
                    first_arrival_at=parse_iso(row["first_arrival"]),
                    quarantine_until=parse_iso(row["quarantine_until"]),
                    path=[PathLeg(**leg) for leg in _load_json(row["path_json"])],
                )
                for row in bundle["results"]
            ]
        else:
            # Failed and invalidated snapshots never expose rows.
            affected = []
        return TraceResponse(
            trace_id=int(trace["id"]),
            status=trace["status"],
            has_sources=bool(_load_json(trace["sources_json"])),
            source_sample_ids=_load_json(trace["sources_json"]),
            affected_samples=affected,
            affected_count=len(affected),
            snapshot=SnapshotInfo(
                intake_id=int(trace["intake_id"]),
                accepted_at=parse_iso(trace["accepted_at"]),
                batch_versions={
                    k: int(v) for k, v in bundle["pinned"].items()
                },
                batches=[BatchOut(**row) for row in bundle["batches"]],
            ),
            error=trace["error"],
        )

    # ----------------------------------------------------------------- routes

    @app.get("/healthz", tags=["intake"], summary="Readiness probe")
    def healthz() -> dict[str, str]:
        store.dataset()  # touches the database
        return {"status": "ok"}

    @app.post(
        "/intakes",
        response_model=IntakeResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["intake"],
        summary="Register samples and transfer batches",
        responses={
            409: {"model": ErrorBody, "description": "Conflicting batch values"},
            422: {"model": ErrorBody, "description": "Invalid payload"},
        },
    )
    def create_intake(payload: IntakeRequest) -> IntakeResponse:
        samples, batches, explicit_sources = payload.normalised()
        try:
            result = store.ingest(
                samples=samples,
                batches=batches,
                source_sample_ids=explicit_sources,
            )
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except InvalidReferenceError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        store.log("intake", {"samples": len(samples), "batches": len(batches)})
        return IntakeResponse(**result)

    @app.get(
        "/dataset",
        response_model=DatasetResponse,
        tags=["intake"],
        summary="Inspect the active dataset",
        responses={404: {"model": ErrorBody, "description": "No dataset yet"}},
    )
    def get_dataset() -> DatasetResponse:
        dataset = store.dataset()
        if dataset is None:
            raise HTTPException(status_code=404, detail="no dataset")
        return DatasetResponse(**dataset)

    @app.post(
        "/batches/{batch_no}/corrections",
        response_model=CorrectionResponse,
        tags=["corrections"],
        summary="Replace a batch version and invalidate dependent results",
        responses={
            404: {"model": ErrorBody, "description": "Unknown batch"},
            409: {"model": ErrorBody, "description": "Stale version or no-op"},
        },
    )
    def correct_batch(
        batch_no: str, payload: BatchCorrection
    ) -> CorrectionResponse:
        try:
            result = store.correct_batch(
                batch_no=batch_no,
                seen_version=payload.seen_version,
                origin=payload.origin,
                destination=payload.destination,
                departed_at=payload.departed_at,
                arrived_at=payload.arrived_at,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        store.log(
            "correction",
            {
                "batch_no": batch_no,
                "previous_version": result["previous_version"],
                "version": result["version"],
                "invalidated": result["invalidated_trace_ids"],
            },
        )
        return CorrectionResponse(**result)

    @app.post(
        "/traces",
        response_model=TraceResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["traces"],
        summary="Compute contamination reachability on a pinned snapshot",
        responses={
            404: {"model": ErrorBody, "description": "No dataset or unknown source"},
            409: {"model": ErrorBody, "description": "Snapshot became stale"},
            500: {"model": ErrorBody, "description": "Trace failed; nothing published"},
        },
    )
    def run_trace(payload: TraceRequest | None = None) -> TraceResponse:
        request = payload or TraceRequest()
        try:
            trace_id, snapshot, sources = store.accept_trace(
                sources=request.source_sample_ids
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except InvalidReferenceError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        try:
            outcome = tracing.compute(
                samples=snapshot["samples"],
                batches=snapshot["batches"],
                source_sample_ids=sources,
            )
            store.publish_results(
                trace_id, outcome["affected_samples"], pinned=snapshot["pinned"]
            )
        except ConflictError as exc:
            # Snapshot changed under us: refusal, not a partial result.
            store.fail_trace(trace_id, str(exc))
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "snapshot_changed",
                    "message": str(exc),
                    "details": {"trace_id": trace_id, "status": "failed"},
                },
            ) from exc
        except Exception as exc:  # noqa: BLE001 - recorded, never half-published
            store.fail_trace(trace_id, f"{type(exc).__name__}: {exc}")
            raise HTTPException(
                status_code=500,
                detail={
                    "code": "trace_failed",
                    "message": f"{type(exc).__name__}: {exc}",
                    "details": {"trace_id": trace_id, "status": "failed"},
                },
            ) from exc

        stored = store.read_trace(trace_id)
        assert stored is not None
        store.log(
            "trace",
            {"trace_id": trace_id, "affected": len(outcome["affected_samples"])},
        )
        return _trace_response(stored)

    @app.get(
        "/traces",
        response_model=TraceListResponse,
        tags=["traces"],
        summary="List accepted traces",
    )
    def list_traces() -> TraceListResponse:
        rows = store.trace_history()
        return TraceListResponse(
            traces=[
                TraceSummary(
                    trace_id=int(row["id"]),
                    status=row["status"],
                    accepted_at=parse_iso(row["accepted_at"]),
                    completed_at=parse_iso(row["completed_at"])
                    if row["completed_at"]
                    else None,
                    source_sample_ids=_load_json(row["sources_json"]),
                    affected_count=int(row["result_count"]),
                    invalidated=int(row["invalidation_count"]) > 0,
                )
                for row in rows
            ]
        )

    @app.get(
        "/traces/{trace_id}",
        response_model=TraceResponse,
        tags=["traces"],
        summary="Read one pinned snapshot result",
        responses={
            404: {"model": ErrorBody, "description": "Unknown trace"},
            409: {"model": ErrorBody, "description": "Snapshot invalidated"},
        },
    )
    def get_trace(trace_id: int) -> TraceResponse:
        bundle = store.read_trace(trace_id)
        if bundle is None:
            raise HTTPException(status_code=404, detail="trace not found")
        trace = bundle["trace"]
        if bundle["invalidations"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "snapshot_invalidated",
                    "message": (
                        "this snapshot was invalidated by a batch correction;"
                        " run a new trace"
                    ),
                    "details": {
                        "trace_id": trace_id,
                        "invalidations": [
                            InvalidationInfo(
                                batch_no=row["batch_no"],
                                version=int(row["version"]),
                                invalidated_at=parse_iso(row["invalidated_at"]),
                                reason=row["reason"],
                            ).model_dump(mode="json")
                            for row in bundle["invalidations"]
                        ],
                    },
                },
            )
        if trace["status"] == "failed":
            raise HTTPException(
                status_code=500,
                detail={
                    "code": "trace_failed",
                    "message": trace["error"] or "trace failed",
                    "details": {"trace_id": trace_id, "status": "failed"},
                },
            )
        if trace["status"] != "complete":
            # A pending trace has no published rows. Returning an empty list
            # with 200 would be indistinguishable from "nothing affected".
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "trace_pending",
                    "message": "trace has not been published yet; retry later",
                    "details": {"trace_id": trace_id, "status": trace["status"]},
                },
            )
        return _trace_response(bundle)

    @app.get(
        "/traces/{trace_id}/invalidations",
        response_model=list[InvalidationInfo],
        tags=["traces"],
        summary="Invalidation records for a trace",
    )
    def get_invalidations(trace_id: int) -> list[InvalidationInfo]:
        if store.trace(trace_id) is None:
            raise HTTPException(status_code=404, detail="trace not found")
        return [
            InvalidationInfo(
                batch_no=row["batch_no"],
                version=int(row["version"]),
                invalidated_at=parse_iso(row["invalidated_at"]),
                reason=row["reason"],
            )
            for row in store.invalidations(trace_id)
        ]

    return app


def _load_json(value: str) -> Any:
    return json.loads(value)


def _jsonable_errors(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Make pydantic errors JSON-safe (ctx may hold exception objects)."""
    cleaned = []
    for item in errors:
        entry = {k: v for k, v in item.items() if k != "ctx"}
        if "ctx" in item:
            entry["ctx"] = {
                k: str(v) if isinstance(v, BaseException) else v
                for k, v in item["ctx"].items()
            }
        entry["loc"] = list(entry.get("loc", ()))
        cleaned.append(entry)
    return cleaned


app = create_app()
