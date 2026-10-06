"""Saga / workflow orchestrator: the baseline service.

Public contract is README.md. A workflow is a list of steps; each step names a compensation that runs
in reverse order when a later step fails.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class SagaError(Exception):
    code = "internal_error"
    status = 500


class InvalidRequest(SagaError):
    code, status = "invalid_request", 400


class InstanceNotFound(SagaError):
    code, status = "not_found", 404


class InvalidTransition(SagaError):
    code, status = "invalid_transition", 409


WORKFLOWS: dict[str, dict[str, Any]] = {
    "order": {"steps": [
        {"name": "reserve-stock", "compensation": "release-stock"},
        {"name": "charge-payment", "compensation": "refund-payment"},
        {"name": "create-shipment", "compensation": "cancel-shipment"},
    ]},
    "provision": {"steps": [
        {"name": "create-account", "compensation": "delete-account"},
        {"name": "attach-policy", "compensation": "detach-policy"},
    ]},
}

# Sentinel: distinguishes "eventId not carried" (baseline behavior) from an explicit null.
_UNSET = object()

# Longest a step may wait for its event: 24 hours in milliseconds.
MAX_TIMEOUT_MS = 86_400_000


def _now_ms() -> int:
    """Wall clock as Unix epoch milliseconds; the single time source for deadlines."""
    return int(time.time() * 1000)


def validate_workflow(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise InvalidRequest("body must be a JSON object")
    if set(payload) - {"steps"}:
        raise InvalidRequest(f"unknown fields: {sorted(set(payload) - {'steps'})}")
    steps = payload.get("steps")
    if not isinstance(steps, list) or not 1 <= len(steps) <= 20:
        raise InvalidRequest("steps must be an array of 1..20 items")
    cleaned = []
    for index, step in enumerate(steps):
        if not isinstance(step, dict) or set(step) - {"name", "compensation", "retry", "await"}:
            raise InvalidRequest(
                f"steps[{index}] must be an object with name/compensation/retry/await"
            )
        name, compensation = step.get("name"), step.get("compensation")
        if not isinstance(name, str) or not name or len(name) > 100:
            raise InvalidRequest(f"steps[{index}].name must be a non-empty string of at most 100 characters")
        if compensation is not None and (not isinstance(compensation, str) or not compensation):
            raise InvalidRequest(f"steps[{index}].compensation must be a non-empty string when present")
        cleaned_step: dict[str, Any] = {"name": name, "compensation": compensation}
        if "retry" in step:
            cleaned_step["retry"] = _validate_retry(step["retry"], index)
        if "await" in step:
            cleaned_step["await"] = _validate_await(step["await"], index)
        cleaned.append(cleaned_step)
    return {"steps": cleaned}


def _validate_await(await_cfg: Any, index: int) -> dict[str, Any]:
    """A step's await config is {"event": <non-empty string <=100>} plus an optional
    ``timeoutMs`` (integer 1..86400000); anything else is 400."""
    if not isinstance(await_cfg, dict) or set(await_cfg) - {"event", "timeoutMs"}:
        raise InvalidRequest(f"steps[{index}].await must be an object with only event/timeoutMs")
    if "event" not in await_cfg:
        raise InvalidRequest(f"steps[{index}].await.event is required")
    event = await_cfg["event"]
    if not isinstance(event, str) or not event or len(event) > 100:
        raise InvalidRequest(
            f"steps[{index}].await.event must be a non-empty string of at most 100 characters"
        )
    cleaned: dict[str, Any] = {"event": event}
    if "timeoutMs" in await_cfg:
        timeout = await_cfg["timeoutMs"]
        if isinstance(timeout, bool) or not isinstance(timeout, int):
            raise InvalidRequest(f"steps[{index}].await.timeoutMs must be an integer")
        if not 1 <= timeout <= MAX_TIMEOUT_MS:
            raise InvalidRequest(
                f"steps[{index}].await.timeoutMs must be between 1 and {MAX_TIMEOUT_MS}"
            )
        cleaned["timeoutMs"] = timeout
    return cleaned


def _validate_retry(retry: Any, index: int) -> dict[str, Any]:
    """A step's retry policy is exactly {"maxAttempts": <int 2..10>}; anything else is 400."""
    if not isinstance(retry, dict) or set(retry) - {"maxAttempts"}:
        raise InvalidRequest(f"steps[{index}].retry must be an object with only maxAttempts")
    if "maxAttempts" not in retry:
        raise InvalidRequest(f"steps[{index}].retry.maxAttempts is required")
    max_attempts = retry["maxAttempts"]
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
        raise InvalidRequest(f"steps[{index}].retry.maxAttempts must be an integer")
    if not 2 <= max_attempts <= 10:
        raise InvalidRequest(f"steps[{index}].retry.maxAttempts must be between 2 and 10")
    return {"maxAttempts": max_attempts}


def _await_event(step: dict[str, Any]) -> str | None:
    await_cfg = step.get("await")
    return await_cfg["event"] if await_cfg else None


def _wait_fields(step: dict[str, Any], now_ms: int | None) -> tuple[str | None, int | None]:
    """The (waitingFor, deadlineAt) pair for a step being entered.

    A step without await waits for nothing; an await without timeoutMs waits
    forever (deadlineAt null); an await with timeoutMs sets deadlineAt to
    now + timeoutMs, so the same (entry time, definition) always yields the
    same deadline.
    """
    await_cfg = step.get("await")
    if not await_cfg:
        return None, None
    timeout = await_cfg.get("timeoutMs")
    if timeout is None:
        return await_cfg["event"], None
    if now_ms is None:
        now_ms = _now_ms()
    return await_cfg["event"], now_ms + timeout


def initial_state(workflow: dict[str, Any], context: dict[str, Any],
                  now_ms: int | None = None) -> dict[str, Any]:
    first = workflow["steps"][0]
    waiting_for, deadline_at = _wait_fields(first, now_ms)
    return {"status": "running", "step": first["name"], "index": 0, "attempt": 1,
            "completed": [], "compensated": [], "context": dict(context), "failure": None,
            "waitingFor": waiting_for, "deadlineAt": deadline_at}


def apply_outcome(workflow: dict[str, Any], state: dict[str, Any], outcome: str, detail: Any = None,
                  now_ms: int | None = None) -> dict[str, Any]:
    """Pure transition: the same (state, outcome, now) always yields the same next state."""
    if state["status"] != "running":
        raise InvalidTransition(f"instance is {state['status']}, not running")
    if outcome == "timed_out":
        return _apply_timed_out(workflow, state, detail, now_ms)
    if state.get("waitingFor") is not None:
        raise InvalidTransition(f"instance is waiting for signal {state['waitingFor']!r}")
    if outcome not in {"succeeded", "failed"}:
        raise InvalidRequest("outcome must be 'succeeded', 'failed' or 'timed_out'")
    if outcome == "succeeded":
        return _apply_succeeded(workflow, state, now_ms)
    steps = workflow["steps"]
    index = int(state["index"])
    # Instances persisted before the attempt field existed are treated as attempt=1.
    attempt = int(state.get("attempt", 1))
    next_state = json.loads(json.dumps(state))
    next_state["attempt"] = attempt
    next_state["failure"] = {"step": steps[index]["name"], "detail": detail}
    retry = steps[index].get("retry") or {}
    max_attempts = retry.get("maxAttempts")
    if max_attempts is not None and attempt < max_attempts:
        # Retryable failure: stay on the same step, bump the attempt counter,
        # keep completed/compensated untouched. Re-entering the step restarts
        # any wait, so the deadline (if any) is recomputed from now.
        next_state["attempt"] = attempt + 1
        next_state["waitingFor"], next_state["deadlineAt"] = _wait_fields(steps[index], now_ms)
        return next_state
    pending = [s["compensation"] for s in steps[: index + 1] if s.get("compensation")]
    next_state["compensated"] = list(reversed(pending))
    next_state["completed"] = state["completed"]
    next_state["status"] = "compensated"
    next_state["step"] = None
    next_state["waitingFor"] = None
    next_state["deadlineAt"] = None
    return next_state


def _apply_timed_out(workflow: dict[str, Any], state: dict[str, Any], detail: Any,
                     now_ms: int | None) -> dict[str, Any]:
    """Dead-letter a waiting step whose deadline has passed.

    Accepted only while running, on a step that is waiting, with a non-null
    deadlineAt that has been reached. The instance keeps its position (step,
    index, completed, compensated untouched) and becomes terminally
    dead_lettered; the failure records the step, reason "timeout" and detail.
    """
    waiting_for = state.get("waitingFor")
    if waiting_for is None:
        raise InvalidTransition("current step is not waiting for an event")
    deadline = state.get("deadlineAt")
    if deadline is None:
        raise InvalidTransition(f"wait for signal {waiting_for!r} has no timeout")
    if now_ms is None:
        now_ms = _now_ms()
    if now_ms < deadline:
        raise InvalidTransition(f"wait for signal {waiting_for!r} has not reached its deadline")
    steps = workflow["steps"]
    index = int(state["index"])
    next_state = json.loads(json.dumps(state))
    next_state["status"] = "dead_lettered"
    next_state["failure"] = {"step": steps[index]["name"], "reason": "timeout", "detail": detail}
    next_state["waitingFor"] = None
    next_state["deadlineAt"] = None
    return next_state


def _apply_succeeded(workflow: dict[str, Any], state: dict[str, Any],
                     now_ms: int | None = None) -> dict[str, Any]:
    """Complete the current step and advance (shared by succeeded outcomes and signals)."""
    steps = workflow["steps"]
    index = int(state["index"])
    next_state = json.loads(json.dumps(state))
    next_state["completed"] = list(state["completed"]) + [steps[index]["name"]]
    next_state["failure"] = None
    next_state["attempt"] = 1
    if index + 1 >= len(steps):
        next_state["status"] = "completed"
        next_state["step"] = None
        next_state["index"] = index
        next_state["waitingFor"] = None
        next_state["deadlineAt"] = None
        return next_state
    next_state["index"] = index + 1
    next_state["step"] = steps[index + 1]["name"]
    next_state["waitingFor"], next_state["deadlineAt"] = _wait_fields(steps[index + 1], now_ms)
    return next_state


def apply_signal(workflow: dict[str, Any], state: dict[str, Any], event: str, detail: Any = None,
                 now_ms: int | None = None) -> dict[str, Any]:
    """Pure transition for an external signal.

    Only a running instance whose current step awaits *event* accepts it; the signal
    completes the current step exactly like a succeeded outcome and moves to the next
    step, which may itself await another event. Detail participates only in request
    identity (ledger replay), like event detail. A signal that arrives before the
    wait's deadline takes this same success path and clears the deadline.
    """
    if state["status"] != "running":
        raise InvalidTransition(f"instance is {state['status']}, not running")
    waiting_for = state.get("waitingFor")
    if waiting_for is None:
        raise InvalidTransition("current step is not waiting for an event")
    if event != waiting_for:
        raise InvalidTransition(f"instance is waiting for {waiting_for!r}, not {event!r}")
    return _apply_succeeded(workflow, state, now_ms)


def _read_state(raw: str) -> dict[str, Any]:
    """Parse persisted state, backfilling fields old instances never had."""
    state = json.loads(raw)
    # Instances persisted before external-event waits existed simply are not waiting.
    state.setdefault("waitingFor", None)
    # Instances persisted before wait timeouts existed have no deadline.
    state.setdefault("deadlineAt", None)
    return state


class Engine:
    """sqlite-backed instance store; one row per instance, state kept as JSON."""

    def __init__(self, path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("CREATE TABLE IF NOT EXISTS instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        # Idempotent event ledger: one row per (instance, eventId). IF NOT EXISTS keeps
        # files created by older builds readable; old instances simply start with an
        # empty ledger and can accept eventIds on their next advance.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS instance_events ("
            "instance_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "request TEXT NOT NULL, response TEXT NOT NULL, "
            "PRIMARY KEY (instance_id, event_id))"
        )
        # Signal ledger: same idempotency rules as instance_events, separate table so
        # signal eventIds never collide with outcome eventIds. Old files simply start
        # with an empty signal ledger.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS instance_signals ("
            "instance_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "request TEXT NOT NULL, response TEXT NOT NULL, "
            "PRIMARY KEY (instance_id, event_id))"
        )
        # Audit trail: one row per accepted (state-advancing) call, seq strictly
        # increasing per instance from 1. Written in the same transaction as the
        # state update and the ledger insert. IF NOT EXISTS keeps pre-audit files
        # readable; old instances simply start accumulating history from their next
        # successful advance — nothing is backfilled for calls we cannot prove.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS instance_audit ("
            "instance_id TEXT NOT NULL, seq INTEGER NOT NULL, "
            "kind TEXT NOT NULL, event_id TEXT, "
            "request TEXT NOT NULL, response TEXT NOT NULL, "
            "PRIMARY KEY (instance_id, seq))"
        )
        self._db.commit()
        self._workflows: dict[str, dict[str, Any]] = dict(WORKFLOWS)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def define(self, name: str, payload: Any) -> dict[str, Any]:
        if not isinstance(name, str) or not name or len(name) > 100:
            raise InvalidRequest("workflow name must be a non-empty string of at most 100 characters")
        workflow = validate_workflow(payload)
        with self._lock:
            self._workflows[name] = workflow
        return workflow

    def workflow(self, name: str) -> dict[str, Any]:
        with self._lock:
            if name not in self._workflows:
                raise InvalidRequest(f"unknown workflow {name!r}")
            return self._workflows[name]

    def start(self, name: str, context: Any = None, now_ms: int | None = None) -> dict[str, Any]:
        if context is not None and not isinstance(context, dict):
            raise InvalidRequest("context must be a JSON object when present")
        workflow = self.workflow(name)
        instance_id = str(uuid.uuid4())
        state = initial_state(workflow, context or {}, now_ms)
        with self._lock:
            self._db.execute("INSERT INTO instances VALUES (?, ?, ?)", (instance_id, name, json.dumps(state)))
            self._db.commit()
        return {"id": instance_id, "workflow": name, "state": state}

    def advance(self, instance_id: str, outcome: Any, detail: Any = None, event_id: Any = _UNSET,
                now_ms: int | None = None) -> dict[str, Any]:
        """Apply one outcome event atomically.

        Without event_id the behavior is the baseline: every call re-enters the state
        machine, so a terminal instance raises InvalidTransition. With an event_id the
        (instance_id, event_id) pair is idempotent: a repeat with an equal normalized
        request returns the first response verbatim (including its historical state)
        without touching the instance state; a repeat whose outcome/detail differs
        raises InvalidTransition. Nothing is written to the ledger unless the transition
        succeeds. The ``timed_out`` outcome shares all of this; it is accepted only
        for a waiting step whose deadline has passed and ends the instance as
        dead_lettered.
        """
        if event_id is _UNSET:
            # Anonymous call: no ledger, but the audit trail still records the
            # normalized input with a null eventId.
            normalized = self._normalize_request(None, {"outcome": outcome, "detail": detail},
                                                 ("eventId", "outcome", "detail"), anonymous=True)
        else:
            normalized = self._normalize_request(event_id, {"outcome": outcome, "detail": detail},
                                                 ("eventId", "outcome", "detail"))
        return self._commit(instance_id, "instance_events", "event", normalized,
                            lambda workflow, state: apply_outcome(workflow, state, outcome, detail, now_ms))

    def signal(self, instance_id: str, event: Any, detail: Any = None, event_id: Any = _UNSET,
               now_ms: int | None = None) -> dict[str, Any]:
        """Deliver an external signal to a waiting instance, with eventId semantics
        identical to :meth:`advance` (instance-scoped, full-response replay).

        Rejections (bad request, unknown instance, not waiting / mismatched event)
        write no ledger rows.
        """
        if not isinstance(event, str) or not event or len(event) > 100:
            raise InvalidRequest("event must be a non-empty string of at most 100 characters")
        if event_id is _UNSET:
            normalized = self._normalize_request(None, {"event": event, "detail": detail},
                                                 ("eventId", "event", "detail"), anonymous=True)
        else:
            normalized = self._normalize_request(event_id, {"event": event, "detail": detail},
                                                 ("eventId", "event", "detail"))
        return self._commit(instance_id, "instance_signals", "signal", normalized,
                            lambda workflow, state: apply_signal(workflow, state, event, detail, now_ms))

    def _commit(self, instance_id: str, table: str, kind: str, normalized: dict[str, Any],
                transition: Any) -> dict[str, Any]:
        """Shared idempotent commit for outcome events and signals.

        Lookup, ledger replay, state transition, ledger insert and audit append happen
        under one lock and in one transaction; the ledger primary key makes a racing
        duplicate insert fail even if the lock were ever bypassed. Only a call that
        actually advances the state machine appends an audit row: replays return
        before the transition and rejections raise before any write.
        """
        event_id = normalized["eventId"]
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            if event_id is not None:
                ledger = self._db.execute(
                    f"SELECT request, response FROM {table} WHERE instance_id = ? AND event_id = ?",
                    (instance_id, event_id),
                ).fetchone()
                if ledger is not None:
                    stored_request, stored_response = ledger
                    if stored_request != json.dumps(normalized, separators=(",", ":"), sort_keys=True):
                        kind_label = "signal" if table == "instance_signals" else "eventId"
                        raise InvalidTransition(
                            f"{kind_label} {event_id!r} was already submitted for this instance with a different payload"
                        )
                    return json.loads(stored_response)
            workflow = self.workflow(row[0])
            state = _read_state(row[1])
            state = transition(workflow, state)
            response = {"id": instance_id, "workflow": row[0], "state": state}
            self._db.execute("UPDATE instances SET state = ? WHERE id = ?", (json.dumps(state), instance_id))
            if event_id is not None:
                self._db.execute(
                    f"INSERT INTO {table} (instance_id, event_id, request, response) VALUES (?, ?, ?, ?)",
                    (instance_id, event_id,
                     json.dumps(normalized, separators=(",", ":"), sort_keys=True),
                     json.dumps(response)),
                )
            # Audit row for the accepted call, next seq per instance, same transaction.
            self._db.execute(
                "INSERT INTO instance_audit (instance_id, seq, kind, event_id, request, response) "
                "SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ? FROM instance_audit WHERE instance_id = ?",
                (instance_id, kind, event_id,
                 json.dumps(normalized, separators=(",", ":"), sort_keys=True),
                 json.dumps(response), instance_id),
            )
            self._db.commit()
        return response

    @staticmethod
    def _normalize_request(event_id: Any, payload: dict[str, Any], key_order: tuple[str, ...],
                           anonymous: bool = False) -> dict[str, Any]:
        """Validate eventId and produce the canonical request used for replay comparison
        and for the audit trail.

        A missing detail and an explicit null detail are the same value, so the
        normalized form always carries ``"detail": null`` unless a detail was given.
        With ``anonymous=True`` the call carried no eventId at all: validation is
        skipped and the normalized form records ``"eventId": null``.
        """
        if anonymous:
            event_id = None
        elif not isinstance(event_id, str) or not event_id or len(event_id) > 100:
            raise InvalidRequest("eventId must be a non-empty string of at most 100 characters")
        normalized: dict[str, Any] = {"eventId": event_id}
        for key in key_order:
            if key == "eventId":
                continue
            value = payload.get(key)
            normalized[key] = value if value is not None else None
        return normalized

    def get(self, instance_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
        if row is None:
            raise InstanceNotFound(f"no instance {instance_id}")
        return {"id": instance_id, "workflow": row[0], "state": _read_state(row[1])}

    def audit(self, instance_id: str) -> dict[str, Any]:
        """Read-only audit view: current state plus the accepted calls in commit order.

        History rows come back as ``{"seq", "kind", "eventId", "request", "response"}``
        with seq strictly increasing from 1; instances that predate the audit table
        simply have an empty (or short) history — nothing is fabricated.
        """
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            rows = self._db.execute(
                "SELECT seq, kind, event_id, request, response FROM instance_audit "
                "WHERE instance_id = ? ORDER BY seq",
                (instance_id,),
            ).fetchall()
        history = [
            {"seq": seq, "kind": kind, "eventId": event_id,
             "request": json.loads(request), "response": json.loads(response)}
            for seq, kind, event_id, request, response in rows
        ]
        return {"id": instance_id, "workflow": row[0], "state": _read_state(row[1]), "history": history}


def make_handler(engine: Engine) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "saga/0.1"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:
            return

        def _send(self, status: int, body: dict[str, Any]) -> None:
            raw = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _read_json(self) -> Any:
            length = self.headers.get("Content-Length")
            if length is None:
                raise InvalidRequest("Content-Length is required")
            try:
                size = int(length)
            except ValueError as error:
                raise InvalidRequest("Content-Length must be an integer") from error
            if size < 0 or size > 1_048_576:
                raise InvalidRequest("Content-Length must be between 0 and 1 MiB")
            if size == 0:
                return None
            try:
                return json.loads(self.rfile.read(size).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise InvalidRequest("body must be valid UTF-8 JSON") from error

        def _parts(self) -> list[str]:
            return [p for p in self.path.split("?")[0].split("/") if p]

        def do_GET(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if parts == ["v1", "workflows"]:
                    return self._send(200, {"workflows": sorted(engine._workflows)})
                if len(parts) == 3 and parts[:2] == ["v1", "instances"]:
                    return self._send(200, engine.get(parts[2]))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "audit":
                    return self._send(200, engine.audit(parts[2]))
                return self._send(404, {"error": {"code": "not_found"}})
            except SagaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_PUT(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if len(parts) != 3 or parts[:2] != ["v1", "workflows"]:
                    return self._send(404, {"error": {"code": "not_found"}})
                workflow = engine.define(parts[2], self._read_json())
                return self._send(200, {"workflow": parts[2], "steps": workflow["steps"]})
            except SagaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if len(parts) == 4 and parts[:2] == ["v1", "workflows"] and parts[3] == "instances":
                    body = self._read_json() or {}
                    if not isinstance(body, dict) or set(body) - {"context"}:
                        raise InvalidRequest("body must be {\"context\": {...}} when present")
                    return self._send(201, engine.start(parts[2], body.get("context")))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "events":
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"outcome", "detail", "eventId"}:
                        raise InvalidRequest(
                            'body must be {"outcome": "succeeded|failed|timed_out", "detail": ..., "eventId": "..."}'
                        )
                    event_id = body["eventId"] if "eventId" in body else _UNSET
                    return self._send(
                        200, engine.advance(parts[2], body.get("outcome"), body.get("detail"), event_id)
                    )
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "signals":
                    body = self._read_json()
                    if not isinstance(body, dict) or set(body) - {"event", "detail", "eventId"}:
                        raise InvalidRequest(
                            'body must be {"event": "<name>", "detail": ..., "eventId": "..."}'
                        )
                    if "event" not in body:
                        raise InvalidRequest('body must be {"event": "<name>", ...}')
                    event_id = body["eventId"] if "eventId" in body else _UNSET
                    return self._send(
                        200, engine.signal(parts[2], body.get("event"), body.get("detail"), event_id)
                    )
                return self._send(404, {"error": {"code": "not_found"}})
            except SagaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 18897, db: str = ":memory:") -> ThreadingHTTPServer:
    engine = Engine(db)
    httpd = ThreadingHTTPServer((host, port), make_handler(engine))
    httpd.engine = engine  # type: ignore[attr-defined]
    return httpd


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="saga orchestrator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18897)
    parser.add_argument("--db", default="saga.sqlite")
    args = parser.parse_args()
    server = serve(args.host, args.port, args.db)
    print(f"saga service listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
