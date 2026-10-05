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
        if not isinstance(step, dict) or set(step) - {"name", "compensation"}:
            raise InvalidRequest(f"steps[{index}] must be an object with name/compensation")
        name, compensation = step.get("name"), step.get("compensation")
        if not isinstance(name, str) or not name or len(name) > 100:
            raise InvalidRequest(f"steps[{index}].name must be a non-empty string of at most 100 characters")
        if compensation is not None and (not isinstance(compensation, str) or not compensation):
            raise InvalidRequest(f"steps[{index}].compensation must be a non-empty string when present")
        cleaned.append({"name": name, "compensation": compensation})
    return {"steps": cleaned}


def initial_state(workflow: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return {"status": "running", "step": workflow["steps"][0]["name"], "index": 0,
            "completed": [], "compensated": [], "context": dict(context), "failure": None}


def apply_outcome(workflow: dict[str, Any], state: dict[str, Any], outcome: str, detail: Any = None) -> dict[str, Any]:
    """Pure transition: the same (state, outcome) always yields the same next state."""
    if state["status"] != "running":
        raise InvalidTransition(f"instance is {state['status']}, not running")
    if outcome not in {"succeeded", "failed"}:
        raise InvalidRequest("outcome must be 'succeeded' or 'failed'")
    steps = workflow["steps"]
    index = int(state["index"])
    next_state = json.loads(json.dumps(state))
    if outcome == "failed":
        next_state["failure"] = {"step": steps[index]["name"], "detail": detail}
        pending = [s["compensation"] for s in steps[: index + 1] if s.get("compensation")]
        next_state["compensated"] = list(reversed(pending))
        next_state["completed"] = state["completed"]
        next_state["status"] = "compensated"
        next_state["step"] = None
        return next_state
    completed = list(state["completed"]) + [steps[index]["name"]]
    next_state["completed"] = completed
    if index + 1 >= len(steps):
        next_state["status"] = "completed"
        next_state["step"] = None
        next_state["index"] = index
        return next_state
    next_state["index"] = index + 1
    next_state["step"] = steps[index + 1]["name"]
    return next_state


class Engine:
    """sqlite-backed instance store; one row per instance, state kept as JSON."""

    def __init__(self, path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("CREATE TABLE IF NOT EXISTS instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
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

    def advance(self, instance_id: str, outcome: Any, detail: Any = None) -> dict[str, Any]:
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            workflow = self.workflow(row[0])
            state = apply_outcome(workflow, json.loads(row[1]), outcome, detail)
            self._db.execute("UPDATE instances SET state = ? WHERE id = ?", (json.dumps(state), instance_id))
            self._db.commit()
        return {"id": instance_id, "workflow": row[0], "state": state}

    def get(self, instance_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._db.execute("SELECT workflow, state FROM instances WHERE id = ?", (instance_id,)).fetchone()
        if row is None:
            raise InstanceNotFound(f"no instance {instance_id}")
        return {"id": instance_id, "workflow": row[0], "state": json.loads(row[1])}


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
                    if not isinstance(body, dict) or set(body) - {"outcome", "detail"}:
                        raise InvalidRequest("body must be {\"outcome\": \"succeeded|failed\", \"detail\": ...}")
                    return self._send(200, engine.advance(parts[2], body.get("outcome"), body.get("detail")))
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
