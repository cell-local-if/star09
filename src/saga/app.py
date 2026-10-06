"""Saga / workflow orchestrator: the baseline service.

Public contract is README.md. A workflow is a list of steps; each step names a compensation that runs
in reverse order when a later step fails.
"""
from __future__ import annotations

import json
import sqlite3
import threading
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
    """A step's await config is exactly {"event": <non-empty string <=100>}."""
    if not isinstance(await_cfg, dict) or set(await_cfg) - {"event"}:
        raise InvalidRequest(f"steps[{index}].await must be an object with only event")
    if "event" not in await_cfg:
        raise InvalidRequest(f"steps[{index}].await.event is required")
    event = await_cfg["event"]
    if not isinstance(event, str) or not event or len(event) > 100:
        raise InvalidRequest(
            f"steps[{index}].await.event must be a non-empty string of at most 100 characters"
        )
    return {"event": event}


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


def initial_state(workflow: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    first = workflow["steps"][0]
    return {"status": "running", "step": first["name"], "index": 0, "attempt": 1,
            "completed": [], "compensated": [], "context": dict(context), "failure": None,
            "waitingFor": _await_event(first)}


def apply_outcome(workflow: dict[str, Any], state: dict[str, Any], outcome: str, detail: Any = None) -> dict[str, Any]:
    """Pure transition: the same (state, outcome) always yields the same next state."""
    if state["status"] != "running":
        raise InvalidTransition(f"instance is {state['status']}, not running")
    if state.get("waitingFor") is not None:
        raise InvalidTransition(f"instance is waiting for signal {state['waitingFor']!r}")
    if outcome not in {"succeeded", "failed"}:
        raise InvalidRequest("outcome must be 'succeeded' or 'failed'")
    if outcome == "succeeded":
        return _apply_succeeded(workflow, state)
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
        # keep completed/compensated untouched.
        next_state["attempt"] = attempt + 1
        next_state["waitingFor"] = _await_event(steps[index])
        return next_state
    pending = [s["compensation"] for s in steps[: index + 1] if s.get("compensation")]
    next_state["compensated"] = list(reversed(pending))
    next_state["completed"] = state["completed"]
    next_state["status"] = "compensated"
    next_state["step"] = None
    next_state["waitingFor"] = None
    return next_state


def _apply_succeeded(workflow: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
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
        return next_state
    next_state["index"] = index + 1
    next_state["step"] = steps[index + 1]["name"]
    next_state["waitingFor"] = _await_event(steps[index + 1])
    return next_state


def apply_signal(workflow: dict[str, Any], state: dict[str, Any], event: str, detail: Any = None) -> dict[str, Any]:
    """Pure transition for an external signal.

    Only a running instance whose current step awaits *event* accepts it; the signal
    completes the current step exactly like a succeeded outcome and moves to the next
    step, which may itself await another event. Detail participates only in request
    identity (ledger replay), like event detail.
    """
    if state["status"] != "running":
        raise InvalidTransition(f"instance is {state['status']}, not running")
    waiting_for = state.get("waitingFor")
    if waiting_for is None:
        raise InvalidTransition("current step is not waiting for an event")
    if event != waiting_for:
        raise InvalidTransition(f"instance is waiting for {waiting_for!r}, not {event!r}")
    return _apply_succeeded(workflow, state)


def _read_state(raw: str) -> dict[str, Any]:
    """Parse persisted state, backfilling fields old instances never had."""
    state = json.loads(raw)
    # Instances persisted before external-event waits existed simply are not waiting.
    state.setdefault("waitingFor", None)
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
        # Instance audit trail: one row per accepted (state-advancing) call, in the
        # deterministic serial order the calls were committed. seq is per-instance and
        # strictly increasing from 1; kind is "event" or "signal" so an eventId string
        # reused across the two ledgers still yields distinct records. Written in the
        # same transaction as the state update and the idempotency ledger. Old files
        # simply start with an empty history; nothing is backfilled for calls that
        # predate the upgrade.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS instance_audit ("
            "instance_id TEXT NOT NULL, seq INTEGER NOT NULL, kind TEXT NOT NULL, "
            "event_id TEXT, request TEXT NOT NULL, response TEXT NOT NULL, "
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

    def start(self, name: str, context: Any = None) -> dict[str, Any]:
        if context is not None and not isinstance(context, dict):
            raise InvalidRequest("context must be a JSON object when present")
        workflow = self.workflow(name)
        instance_id = str(uuid.uuid4())
        state = initial_state(workflow, context or {})
        with self._lock:
            self._db.execute("INSERT INTO instances VALUES (?, ?, ?)", (instance_id, name, json.dumps(state)))
            self._db.commit()
        return {"id": instance_id, "workflow": name, "state": state}

    def advance(self, instance_id: str, outcome: Any, detail: Any = None, event_id: Any = _UNSET) -> dict[str, Any]:
        """Apply one outcome event atomically.

        Without event_id the behavior is the baseline: every call re-enters the state
        machine, so a terminal instance raises InvalidTransition. With an event_id the
        (instance_id, event_id) pair is idempotent: a repeat with an equal normalized
        request returns the first response verbatim (including its historical state)
        without touching the instance state; a repeat whose outcome/detail differs
        raises InvalidTransition. Nothing is written to the ledger unless the transition
        succeeds.
        """
        normalized = None
        if event_id is not _UNSET:
            normalized = self._normalize_request(event_id, {"outcome": outcome, "detail": detail},
                                                 ("eventId", "outcome", "detail"))
            event_id = normalized["eventId"]
        return self._commit(instance_id, "instance_events", "event", event_id, normalized,
                            {"outcome": outcome, "detail": detail},
                            lambda workflow, state: apply_outcome(workflow, state, outcome, detail))

    def signal(self, instance_id: str, event: Any, detail: Any = None, event_id: Any = _UNSET) -> dict[str, Any]:
        """Deliver an external signal to a waiting instance, with eventId semantics
        identical to :meth:`advance` (instance-scoped, full-response replay).

        Rejections (bad request, unknown instance, not waiting / mismatched event)
        write no ledger rows.
        """
        if not isinstance(event, str) or not event or len(event) > 100:
            raise InvalidRequest("event must be a non-empty string of at most 100 characters")
        normalized = None
        if event_id is not _UNSET:
            normalized = self._normalize_request(event_id, {"event": event, "detail": detail},
                                                 ("eventId", "event", "detail"))
            event_id = normalized["eventId"]
        return self._commit(instance_id, "instance_signals", "signal", event_id, normalized,
                            {"event": event, "detail": detail},
                            lambda workflow, state: apply_signal(workflow, state, event, detail))

    def _commit(self, instance_id: str, table: str, kind: str, event_id: Any,
                normalized: dict[str, Any] | None, audit_request: dict[str, Any],
                transition: Any) -> dict[str, Any]:
        """Shared idempotent commit for outcome events and signals.

        Lookup, ledger replay, state transition, ledger insert and audit append happen
        under one lock and in one transaction; the ledger primary key makes a racing
        duplicate insert fail even if the lock were ever bypassed. The audit row is
        appended only when the transition actually advances the state machine — ledger
        replays and every rejection leave no trace — and its seq is the next integer
        after the instance's current maximum, so concurrent accepted calls land in one
        deterministic serial order with contiguous seq values.
        """
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            if normalized is not None:
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
            if normalized is not None:
                self._db.execute(
                    f"INSERT INTO {table} (instance_id, event_id, request, response) VALUES (?, ?, ?, ?)",
                    (instance_id, event_id,
                     json.dumps(normalized, separators=(",", ":"), sort_keys=True),
                     json.dumps(response)),
                )
            seq = self._db.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM instance_audit WHERE instance_id = ?",
                (instance_id,),
            ).fetchone()[0]
            self._db.execute(
                "INSERT INTO instance_audit (instance_id, seq, kind, event_id, request, response)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (instance_id, seq, kind, None if event_id is _UNSET else event_id,
                 json.dumps(audit_request, separators=(",", ":"), sort_keys=True),
                 json.dumps(response)),
            )
            self._db.commit()
        return response

    @staticmethod
    def _normalize_request(event_id: Any, payload: dict[str, Any], key_order: tuple[str, ...]) -> dict[str, Any]:
        """Validate eventId and produce the canonical request used for replay comparison.

        A missing detail and an explicit null detail are the same value, so the
        normalized form always carries ``"detail": null`` unless a detail was given.
        """
        if not isinstance(event_id, str) or not event_id or len(event_id) > 100:
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
        """Read-only history of every accepted (state-advancing) event and signal.

        Records come back in commit order: seq starts at 1 and increases strictly.
        Each record carries the normalized request payload (detail normalized so an
        omitted value and an explicit null both read as null) and the full JSON
        response that processing returned, so past results can be checked without
        re-running the state machine. Replays and rejected calls never appear here.
        """
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            rows = self._db.execute(
                "SELECT seq, kind, event_id, request, response FROM instance_audit"
                " WHERE instance_id = ? ORDER BY seq",
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
                            'body must be {"outcome": "succeeded|failed", "detail": ..., "eventId": "..."}'
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
