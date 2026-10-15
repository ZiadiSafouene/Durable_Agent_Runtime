from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import EventType, RunStatus
from app.runtime.replay import reconstruct_state, replay_recorded
from app.schemas.api import CreateRunRequest, EventResponse, ReplayResponse, RunResponse
from app.schemas.events import NewEvent
from app.storage.event_store import EventStore
from app.storage.repositories import RunRepository

router = APIRouter()


def _run_response(run, state) -> RunResponse:
    return RunResponse(
        run_id=run.id,
        status=state.status if state.last_sequence else run.status,
        workflow_name=run.workflow_name,
        input=run.input_payload,
        result=state.result or run.result_payload,
        error=state.error or run.error,
        current_step=state.current_step_id,
        completed_steps=state.completed_step_ids,
        last_sequence=state.last_sequence,
        attempt_count=run.attempt_count,
        created_at=run.created_at,
        updated_at=run.updated_at,
    )


@router.post("/runs", response_model=RunResponse, status_code=201)
async def create_run(body: CreateRunRequest, request: Request) -> RunResponse:
    container = request.app.state.container
    async with container.session_factory() as session:
        runs = RunRepository(session)
        run = await runs.create(
            body.workflow_name,
            {"task": body.task, "notify_channel": body.notify_channel},
        )
        await EventStore(session).append(
            run.id,
            [
                NewEvent(
                    event_type=EventType.RUN_CREATED,
                    payload={"workflow_name": body.workflow_name, "input": run.input_payload},
                    correlation_id=run.id,
                )
            ],
        )
        await session.commit()
        state = await reconstruct_state(session, run.id)
        return _run_response(run, state)


@router.get("/runs/{run_id}", response_model=RunResponse)
async def get_run(run_id: str, request: Request) -> RunResponse:
    container = request.app.state.container
    async with container.session_factory() as session:
        run, state = await _load(session, run_id)
        return _run_response(run, state)


@router.get("/runs/{run_id}/events", response_model=list[EventResponse])
async def get_events(run_id: str, request: Request) -> list[EventResponse]:
    container = request.app.state.container
    async with container.session_factory() as session:
        await _load(session, run_id)
        events = await EventStore(session).list_events(run_id)
        return [
            EventResponse(
                id=event.id,
                run_id=event.run_id,
                event_type=event.event_type,
                sequence=event.sequence,
                payload=event.payload,
                schema_version=event.schema_version,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                created_at=event.created_at,
            )
            for event in events
        ]


@router.post("/runs/{run_id}/retry", response_model=RunResponse)
async def retry_run(run_id: str, request: Request) -> RunResponse:
    container = request.app.state.container
    async with container.session_factory() as session:
        run, state = await _load(session, run_id)
        if state.status != RunStatus.FAILED.value:
            raise HTTPException(status_code=409, detail="only failed runs can be retried")
        await EventStore(session).append(
            run.id,
            [NewEvent(event_type=EventType.RUN_RETRY_REQUESTED, payload={"reason": "explicit_retry"})],
        )
        run.status = RunStatus.PENDING.value
        run.error = None
        run.lease_owner = None
        run.lease_expires_at = None
        await session.commit()
        state = await reconstruct_state(session, run.id)
        return _run_response(run, state)


@router.post("/runs/{run_id}/replay", response_model=ReplayResponse)
async def replay_run(run_id: str, request: Request) -> ReplayResponse:
    container = request.app.state.container
    async with container.session_factory() as session:
        await _load(session, run_id)
        before = await _effect_count(session, run_id)
        report = await replay_recorded(session, run_id)
        after = await _effect_count(session, run_id)
        if after != before:
            raise HTTPException(status_code=500, detail="replay executed side effects")
        return report


@router.get("/runs/{run_id}/stream")
async def stream_run(run_id: str, request: Request) -> StreamingResponse:
    container = request.app.state.container
    last_event_id = request.headers.get("last-event-id") or request.query_params.get("last_event_id") or "0"
    try:
        after = int(last_event_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="last_event_id must be an integer sequence") from exc

    async with container.session_factory() as session:
        await _load(session, run_id)

    async def event_publisher():
        current = after
        last_heartbeat = datetime.now(UTC)
        terminal = False
        while True:
            if await request.is_disconnected():
                break
            async with container.session_factory() as session:
                events = await EventStore(session).list_events(run_id, after_sequence=current)
                run = await RunRepository(session).get(run_id)
                state = await reconstruct_state(session, run_id)
            for event in events:
                current = event.sequence
                data = {
                    "id": event.id,
                    "event_type": event.event_type,
                    "sequence": event.sequence,
                    "payload": event.payload,
                    "status": state.status,
                }
                yield f"id: {event.sequence}\nevent: {event.event_type}\ndata: {json.dumps(data, default=str)}\n\n"
            if run is not None and state.status in {
                RunStatus.COMPLETED.value,
                RunStatus.FAILED.value,
                RunStatus.CANCELLED.value,
            }:
                if not events and terminal:
                    break
                terminal = True
                if not events:
                    yield (
                        "event: status\n"
                        f"data: {json.dumps({'status': state.status, 'sequence': state.last_sequence})}\n\n"
                    )
                    break
            now = datetime.now(UTC)
            if (now - last_heartbeat).total_seconds() >= container.settings.sse_heartbeat_seconds:
                yield ": heartbeat\n\n"
                last_heartbeat = now
            await asyncio.sleep(container.settings.sse_poll_interval_seconds)

    return StreamingResponse(
        event_publisher(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


async def _load(session: AsyncSession, run_id: str):
    run = await RunRepository(session).get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    state = await reconstruct_state(session, run_id)
    return run, state


async def _effect_count(session: AsyncSession, run_id: str) -> int:
    from app.storage.repositories import ToolRepository

    return await ToolRepository(session).count_effects(run_id=run_id)
