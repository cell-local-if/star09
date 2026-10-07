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
from urllib.parse import parse_qsl


class SagaError(Exception):
    code = "internal_error"
    status = 500


class InvalidRequest(SagaError):
    code, status = "invalid_request", 400


class NotFound(SagaError):
    code, status = "not_found", 404


class InstanceNotFound(NotFound):
    pass


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

# The only values the instance-listing status filter accepts.
LIST_STATUSES = ("running", "completed", "compensated", "dead_lettered")


class _RevisionedResponse(dict):
    """A response dict carrying the instance revision it reflects, for ETag emission.

    It is a plain ``dict`` as far as JSON serialization and equality are concerned
    (so stored/replayed responses compare exactly as before); ``revision`` is extra
    metadata captured in the same locked transaction as the body, never read in a
    separate query afterwards.
    """

    __slots__ = ("revision",)

    def __init__(self, mapping: dict[str, Any], revision: int) -> None:
        super().__init__(mapping)
        self.revision = revision

# If-Match is optional on mutating calls. None means the header was omitted, in
# which case no precondition is added at all; an int is the revision the client
# last observed. Header *syntax* is the HTTP layer's job (parse_if_match).


def parse_if_match(raw: str | None) -> int | None:
    """Parse an optional If-Match header carrying exactly one quoted revision.

    The only accepted shape is a single double-quoted non-negative decimal,
    e.g. ``"1"`` — matching the ETag form emitted on instance responses. Empty
    values, multiple values, weak tags (``W/"1"``), the wildcard ``*`` and
    unquoted numbers are all ``400 invalid_request``. ``None`` (header absent)
    returns ``None``: the call carries no precondition.
    """
    if raw is None:
        return None
    if len(raw) < 2 or not (raw.startswith('"') and raw.endswith('"')):
        raise InvalidRequest("If-Match must be a single quoted instance version, e.g. \"1\"")
    digits = raw[1:-1]
    if not digits or not digits.isascii() or not digits.isdigit():
        raise InvalidRequest("If-Match must quote a decimal version")
    # Canonical decimal only: "01" is not a valid version literal (only "0" may
    # start with 0, and it can never match a revision since those start at 1).
    if len(digits) > 1 and digits[0] == "0":
        raise InvalidRequest("If-Match version must not have leading zeros")
    return int(digits)


def _parse_audit_limit(raw: Any) -> int:
    """Page size for a paged audit query: a decimal integer in 1..100 carried as a
    raw query string, defaulting to 50 when the parameter is absent. Leading zeros,
    empty values, booleans and decimal-point forms are all 400."""
    if raw is None:
        return 50
    if (not isinstance(raw, str) or not raw or not raw.isascii()
            or not raw.isdigit() or (len(raw) > 1 and raw[0] == "0")):
        raise InvalidRequest(
            "limit must be a decimal integer between 1 and 100 without leading zeros"
        )
    size = int(raw)
    if not 1 <= size <= 100:
        raise InvalidRequest("limit must be between 1 and 100")
    return size


def _parse_audit_after_seq(raw: Any) -> int:
    """Audit cursor for a paged query: a non-negative decimal integer carried as a
    raw query string, defaulting to 0 (start from the first record) when absent.
    Leading zeros, empty values, signs and decimal-point forms are all 400."""
    if raw is None:
        return 0
    if (not isinstance(raw, str) or not raw or not raw.isascii()
            or not raw.isdigit() or (len(raw) > 1 and raw[0] == "0")):
        raise InvalidRequest(
            "afterSeq must be a non-negative decimal integer without leading zeros"
        )
    return int(raw)


def _parse_timeline_limit(raw: Any) -> int:
    """Page size for the timeline query: a decimal integer in 1..200 carried as a
    raw query string, defaulting to 50 when the parameter is absent. Leading zeros,
    empty values, booleans and decimal-point forms are all 400."""
    if raw is None:
        return 50
    if (not isinstance(raw, str) or not raw or not raw.isascii()
            or not raw.isdigit() or (len(raw) > 1 and raw[0] == "0")):
        raise InvalidRequest(
            "limit must be a decimal integer between 1 and 200 without leading zeros"
        )
    size = int(raw)
    if not 1 <= size <= 200:
        raise InvalidRequest("limit must be between 1 and 200")
    return size


def _validate_ordering(partition_key: Any, sequence: Any, has_event_id: bool) -> tuple[str | None, int | None]:
    """Validate the optional ``(partitionKey, sequence)`` ordering pair.

    Ordering is opt-in and activates only when *both* fields are carried and the
    call also carries a non-empty eventId. Anything short of that is 400:

    * exactly one of the two fields present, or either present without an
      eventId — ordering cannot ride on an anonymous call;
    * partitionKey other than a 1..100-character non-empty string (null,
      booleans, numbers, arrays, objects, empty or too long);
    * sequence other than a positive JSON integer (booleans rejected
      explicitly, as are floats, strings and 0/negative numbers).

    Returns ``(None, None)`` for a baseline (unordered) call.
    """
    key_present = partition_key is not _UNSET
    seq_present = sequence is not _UNSET
    if not key_present and not seq_present:
        return None, None
    if not has_event_id or not key_present or not seq_present:
        raise InvalidRequest(
            "partitionKey and sequence must be provided together with a non-empty eventId"
        )
    if not isinstance(partition_key, str) or not partition_key or len(partition_key) > 100:
        raise InvalidRequest("partitionKey must be a non-empty string of at most 100 characters")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise InvalidRequest("sequence must be a positive integer")
    return partition_key, sequence


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
    ``timeoutMs`` (integer 1..86400000) after which the wait may be dead-lettered."""
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
        if not 1 <= timeout <= 86_400_000:
            raise InvalidRequest(f"steps[{index}].await.timeoutMs must be between 1 and 86400000")
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


def _now_ms() -> int:
    """Current wall clock as Unix epoch milliseconds."""
    return int(time.time() * 1000)


def _wait_fields(step: dict[str, Any], now_ms: int | None) -> tuple[str | None, int | None]:
    """(waitingFor, deadlineAt) for entering *step*.

    A step without ``await`` waits for nothing (None, None); an await without
    ``timeoutMs`` waits forever (event, None); an await with ``timeoutMs`` gets an
    absolute deadline of now + timeoutMs.
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
    if outcome not in {"succeeded", "failed", "timed_out"}:
        raise InvalidRequest("outcome must be 'succeeded', 'failed' or 'timed_out'")
    if outcome == "timed_out":
        return _apply_timed_out(workflow, state, detail, now_ms)
    if state.get("waitingFor") is not None:
        raise InvalidTransition(f"instance is waiting for signal {state['waitingFor']!r}")
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
        # keep completed/compensated untouched.
        next_state["attempt"] = attempt + 1
        waiting_for, deadline_at = _wait_fields(steps[index], now_ms)
        next_state["waitingFor"] = waiting_for
        next_state["deadlineAt"] = deadline_at
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
    """Dead-letter a running instance whose current wait has passed its deadline.

    Only a waiting step with a non-null, already-reached deadlineAt accepts this;
    anything else (not waiting, wait without timeoutMs, deadline still in the
    future) is an invalid transition. The instance keeps its position (step/index)
    and its completed/compensated lists; the wait markers are cleared.
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


def apply_recover(workflow: dict[str, Any], state: dict[str, Any],
                  now_ms: int | None = None) -> dict[str, Any]:
    """Pure transition: explicitly return a dead_lettered instance to running.

    Recovery is the only transition a dead_lettered instance accepts, and it is
    deliberately narrow:

    * the instance keeps its position (step/index) and its completed,
      compensated and context lists exactly as they were;
    * failure clears, attempt resets to 1;
    * the current step's wait is re-armed as if the step were freshly entered —
      waitingFor carries the awaited event name, deadlineAt is rebuilt as of the
      recovery moment when the await has timeoutMs, else null; a step without an
      await waits for nothing (both null).

    Nothing is replayed or compensated: completed steps are not re-run, and a
    following event or signal is still required to move the instance on. Any
    status other than dead_lettered is an invalid transition.
    """
    if state["status"] != "dead_lettered":
        raise InvalidTransition(f"instance is {state['status']}, not dead_lettered")
    steps = workflow["steps"]
    index = int(state["index"])
    next_state = json.loads(json.dumps(state))
    next_state["status"] = "running"
    next_state["failure"] = None
    next_state["attempt"] = 1
    waiting_for, deadline_at = _wait_fields(steps[index], now_ms)
    next_state["waitingFor"] = waiting_for
    next_state["deadlineAt"] = deadline_at
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
    waiting_for, deadline_at = _wait_fields(steps[index + 1], now_ms)
    next_state["waitingFor"] = waiting_for
    next_state["deadlineAt"] = deadline_at
    return next_state


def apply_signal(workflow: dict[str, Any], state: dict[str, Any], event: str, detail: Any = None,
                 now_ms: int | None = None) -> dict[str, Any]:
    """Pure transition for an external signal.

    Only a running instance whose current step awaits *event* accepts it; the signal
    completes the current step exactly like a succeeded outcome and moves to the next
    step, which may itself await another event. A signal accepted before the deadline
    takes this success path regardless of any timeoutMs on the wait. Detail
    participates only in request identity (ledger replay), like event detail.
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
        # Version pinning: each instance records the workflow version it runs against.
        # Files created by older builds lack the column; it is added with NULL, and a
        # NULL version reads back as workflowVersion null until the first successful
        # advance backfills the then-latest version.
        columns = {row[1] for row in self._db.execute("PRAGMA table_info(instances)")}
        if "version" not in columns:
            self._db.execute("ALTER TABLE instances ADD COLUMN version INTEGER")
        # Optimistic concurrency: every instance starts at revision 1 and the
        # revision advances by one only on a change that really mutates the
        # instance. Files created before concurrency control lack the column; it
        # is added with NULL, which reads back as revision 1 and becomes 2 on the
        # instance's first real change — no historical change count is fabricated.
        if "instance_version" not in columns:
            self._db.execute("ALTER TABLE instances ADD COLUMN instance_version INTEGER")
        # Immutable workflow definitions: one row per (name, version), never updated
        # or deleted, so a pinned instance always finds the exact definition it
        # started with — including after the process reopens the same file.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS workflow_versions ("
            "name TEXT NOT NULL, version INTEGER NOT NULL, definition TEXT NOT NULL, "
            "PRIMARY KEY (name, version))"
        )
        # Built-in workflows seed version 1; later PUTs of the same name add 2, 3, ...
        for builtin, definition in WORKFLOWS.items():
            self._db.execute(
                "INSERT OR IGNORE INTO workflow_versions (name, version, definition) VALUES (?, 1, ?)",
                (builtin, json.dumps(definition)),
            )
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
        # Partition ordering cursors: one row per (workflow, partitionKey). The
        # cursor is the last sequence consumed by an accepted ordered call; the
        # partition's next call must carry cursor + 1. Events and signals share
        # the cursor and the partition spans every instance of the workflow.
        # IF NOT EXISTS keeps files created by older builds readable: the table
        # is added empty, so a partition's first post-upgrade call starts at 1;
        # no history is fabricated from pre-upgrade calls.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS partition_cursors ("
            "workflow TEXT NOT NULL, partition_key TEXT NOT NULL, "
            "cursor INTEGER NOT NULL, "
            "PRIMARY KEY (workflow, partition_key))"
        )
        self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def define(self, name: str, payload: Any) -> dict[str, Any]:
        """Append a new immutable version of *name* and return it with its version.

        Validation is unchanged from the baseline; a rejected payload leaves every
        existing version untouched. Versions are per-name monotonically increasing
        integers (built-ins seed 1), persisted so they survive reopening the file.
        """
        if not isinstance(name, str) or not name or len(name) > 100:
            raise InvalidRequest("workflow name must be a non-empty string of at most 100 characters")
        workflow = validate_workflow(payload)
        with self._lock:
            row = self._db.execute(
                "SELECT MAX(version) FROM workflow_versions WHERE name = ?", (name,)
            ).fetchone()
            version = (row[0] or 0) + 1
            self._db.execute(
                "INSERT INTO workflow_versions (name, version, definition) VALUES (?, ?, ?)",
                (name, version, json.dumps(workflow)),
            )
            self._db.commit()
        return {"steps": workflow["steps"], "version": version}

    def workflow_names(self) -> list[str]:
        with self._lock:
            rows = self._db.execute("SELECT DISTINCT name FROM workflow_versions").fetchall()
        return sorted(row[0] for row in rows)

    def workflow(self, name: str, version: int | None = None) -> dict[str, Any]:
        """The definition of *name* at *version* (latest when omitted).

        Unknown names raise InvalidRequest (the baseline behavior for starting an
        instance); a known name with an unknown version raises NotFound.
        """
        with self._lock:
            if version is None:
                row = self._db.execute(
                    "SELECT definition FROM workflow_versions WHERE name = ? "
                    "ORDER BY version DESC LIMIT 1",
                    (name,),
                ).fetchone()
                if row is None:
                    raise InvalidRequest(f"unknown workflow {name!r}")
            else:
                row = self._db.execute(
                    "SELECT definition FROM workflow_versions WHERE name = ? AND version = ?",
                    (name, version),
                ).fetchone()
                if row is None:
                    raise NotFound(f"unknown version {version} for workflow {name!r}")
        return json.loads(row[0])

    def workflow_info(self, name: str) -> dict[str, Any]:
        """Latest version of *name* as {"workflow", "version", "steps"}; 404 if unknown."""
        with self._lock:
            row = self._db.execute(
                "SELECT version, definition FROM workflow_versions WHERE name = ? "
                "ORDER BY version DESC LIMIT 1",
                (name,),
            ).fetchone()
        if row is None:
            raise NotFound(f"unknown workflow {name!r}")
        return {"workflow": name, "version": row[0], "steps": json.loads(row[1])["steps"]}

    def start(self, name: str, context: Any = None, version: Any = None) -> dict[str, Any]:
        if context is not None and not isinstance(context, dict):
            raise InvalidRequest("context must be a JSON object when present")
        if version is not None and (isinstance(version, bool) or not isinstance(version, int) or version < 1):
            raise InvalidRequest("version must be a positive integer when present")
        with self._lock:
            # Definition and version number resolve in one critical section, so a
            # concurrent PUT cannot slip a new version between the two lookups.
            workflow = self.workflow(name, version)
            if version is None:
                version = self._db.execute(
                    "SELECT MAX(version) FROM workflow_versions WHERE name = ?", (name,)
                ).fetchone()[0]
            instance_id = str(uuid.uuid4())
            state = initial_state(workflow, context or {})
            self._db.execute(
                "INSERT INTO instances (id, workflow, state, version, instance_version) "
                "VALUES (?, ?, ?, ?, 1)",
                (instance_id, name, json.dumps(state), version),
            )
            self._db.commit()
        return _RevisionedResponse(
            {"id": instance_id, "workflow": name, "workflowVersion": version, "state": state}, 1
        )

    def advance(self, instance_id: str, outcome: Any, detail: Any = None, event_id: Any = _UNSET,
                if_match: int | None = None, partition_key: Any = _UNSET,
                sequence: Any = _UNSET) -> dict[str, Any]:
        """Apply one outcome event atomically.

        Without event_id the behavior is the baseline: every call re-enters the state
        machine, so a terminal instance raises InvalidTransition. With an event_id the
        (instance_id, event_id) pair is idempotent: a repeat with an equal normalized
        request returns the first response verbatim (including its historical state)
        without touching the instance state; a repeat whose outcome/detail differs
        raises InvalidTransition. Nothing is written to the ledger unless the transition
        succeeds.

        With ``if_match`` set, the call proceeds only when the instance's current
        revision equals it; a stale observation raises InvalidTransition. The
        idempotency replay is checked first: a repeat of an already-accepted
        eventId returns the first response even when the revision has moved on.

        With a validated ``partition_key``/``sequence`` pair (both present, alongside
        an eventId) the call is additionally ordered within the instance's workflow:
        sequence 1 opens the partition and every later accepted ordered call consumes
        the next integer; gaps, stale numbers and mid-flight reordering are 409 with
        no state change (see :meth:`_commit`).
        """
        if event_id is _UNSET:
            # Anonymous call: no ledger, but the audit trail still records the
            # normalized input with a null eventId.
            normalized = self._normalize_request(None, {"outcome": outcome, "detail": detail},
                                                 ("eventId", "outcome", "detail"), anonymous=True,
                                                 partition_key=partition_key, sequence=sequence)
        else:
            normalized = self._normalize_request(event_id, {"outcome": outcome, "detail": detail},
                                                 ("eventId", "outcome", "detail"),
                                                 partition_key=partition_key, sequence=sequence)
        return self._commit(instance_id, "instance_events", "event", normalized,
                            lambda workflow, state: apply_outcome(workflow, state, outcome, detail),
                            if_match)

    def signal(self, instance_id: str, event: Any, detail: Any = None, event_id: Any = _UNSET,
               if_match: int | None = None, partition_key: Any = _UNSET,
               sequence: Any = _UNSET) -> dict[str, Any]:
        """Deliver an external signal to a waiting instance, with eventId semantics
        identical to :meth:`advance` (instance-scoped, full-response replay).

        Rejections (bad request, unknown instance, not waiting / mismatched event)
        write no ledger rows. An optional ``if_match`` revision precondition works
        exactly as on :meth:`advance`, and replay still takes precedence over it.
        Ordered signals (``partition_key``/``sequence``) share the workflow partition
        cursor with ordered events; see :meth:`advance` and :meth:`_commit`.
        """
        if not isinstance(event, str) or not event or len(event) > 100:
            raise InvalidRequest("event must be a non-empty string of at most 100 characters")
        if event_id is _UNSET:
            normalized = self._normalize_request(None, {"event": event, "detail": detail},
                                                 ("eventId", "event", "detail"), anonymous=True,
                                                 partition_key=partition_key, sequence=sequence)
        else:
            normalized = self._normalize_request(event_id, {"event": event, "detail": detail},
                                                 ("eventId", "event", "detail"),
                                                 partition_key=partition_key, sequence=sequence)
        return self._commit(instance_id, "instance_signals", "signal", normalized,
                            lambda workflow, state: apply_signal(workflow, state, event, detail),
                            if_match)

    def _commit(self, instance_id: str, table: str, kind: str, normalized: dict[str, Any],
                transition: Any, if_match: int | None = None) -> dict[str, Any]:
        """Shared idempotent commit for outcome events and signals.

        Lookup, ledger replay, precondition check, sequence check, state
        transition, revision bump, cursor advance, ledger insert and audit
        append happen under one lock and in one transaction; the ledger primary
        key makes a racing duplicate insert fail even if the lock were ever
        bypassed. Only a call that actually advances the state machine appends
        an audit row, increments the revision and consumes a partition sequence:
        replays return before the precondition and sequence checks, and
        rejections raise before any write. The ordered resolution is
        eventId replay first, then the If-Match precondition, then the sequence.
        """
        event_id = normalized["eventId"]
        ordered = normalized.get("partitionKey") is not None
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            revision = row[3] if row[3] is not None else 1
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
                    # Replay beats the precondition and the sequence check: the
                    # first response is handed back verbatim on whatever revision
                    # the instance is now at and the cursor is not re-consumed.
                    return _RevisionedResponse(json.loads(stored_response), revision)
            # No ledger replay: an explicit precondition must match the current
            # revision before the sequence gate and the state machine are
            # entered. A rejected precondition writes nothing (no state, no
            # ledger, no audit, no cursor).
            if if_match is not None and if_match != revision:
                raise InvalidTransition(
                    f"instance version is {revision}, not {if_match}"
                )
            # Ordered call: the sequence must be exactly one past the partition's
            # persisted cursor (the first call opens the partition at 1). A gap,
            # a stale number or a reordered in-flight call is rejected here,
            # before the state machine runs — rejected ordered calls change no
            # state, revision, ledger, audit or cursor. Events and signals share
            # the row for (instance workflow, partitionKey).
            if ordered:
                cursor_row = self._db.execute(
                    "SELECT cursor FROM partition_cursors WHERE workflow = ? AND partition_key = ?",
                    (row[0], normalized["partitionKey"]),
                ).fetchone()
                current_cursor = cursor_row[0] if cursor_row is not None else 0
                if normalized["sequence"] != current_cursor + 1:
                    raise InvalidTransition(
                        f"partition {normalized['partitionKey']!r} expects sequence "
                        f"{current_cursor + 1}, not {normalized['sequence']}"
                    )
            # The instance advances against the definition of its pinned version, so a
            # later PUT of the same name never affects in-flight instances. Instances
            # persisted before versioning existed (version NULL) resolve the latest
            # version now and backfill it on this first successful advance.
            version = row[2]
            workflow = self.workflow(row[0], version)
            if version is None:
                version = self._db.execute(
                    "SELECT MAX(version) FROM workflow_versions WHERE name = ?", (row[0],)
                ).fetchone()[0]
            state = _read_state(row[1])
            state = transition(workflow, state)
            response = {"id": instance_id, "workflow": row[0], "workflowVersion": version, "state": state}
            if ordered:
                # The accepted ordered call carries its partition coordinates in
                # the response; replays keep whatever the first response carried.
                response["ordering"] = {"partitionKey": normalized["partitionKey"],
                                        "sequence": normalized["sequence"]}
            # The transition was accepted: the instance really changed, so the
            # revision advances by exactly one (a NULL legacy revision becomes 2
            # after its first post-upgrade change, never 1 — it read as 1).
            next_revision = revision + 1
            self._db.execute(
                "UPDATE instances SET state = ?, version = ?, instance_version = ? WHERE id = ?",
                (json.dumps(state), version, next_revision, instance_id),
            )
            if ordered:
                # Consume the sequence in the same transaction as the state
                # machine, ledger and audit updates. INSERT ... ON CONFLICT keeps
                # the partition on its single monotonically increasing row.
                self._db.execute(
                    "INSERT INTO partition_cursors (workflow, partition_key, cursor) "
                    "VALUES (?, ?, ?) ON CONFLICT(workflow, partition_key) "
                    "DO UPDATE SET cursor = excluded.cursor",
                    (row[0], normalized["partitionKey"], normalized["sequence"]),
                )
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
        return _RevisionedResponse(response, next_revision)

    @staticmethod
    def _normalize_request(event_id: Any, payload: dict[str, Any], key_order: tuple[str, ...],
                           anonymous: bool = False, partition_key: Any = _UNSET,
                           sequence: Any = _UNSET) -> dict[str, Any]:
        """Validate eventId/ordering and produce the canonical request used for
        replay comparison and for the audit trail.

        A missing detail and an explicit null detail are the same value, so the
        normalized form always carries ``"detail": null`` unless a detail was given.
        With ``anonymous=True`` the call carried no eventId at all: eventId
        validation is skipped and the normalized form records ``"eventId": null``;
        ordering fields are meaningless on such a call and are rejected.

        Partition ordering activates only when *both* ``partition_key`` and
        ``sequence`` are carried alongside a real eventId: a lone field, a bad
        partitionKey (anything but a 1..100-char string) or a bad sequence
        (anything but a positive integer, booleans included) is 400. Ordered
        normalized requests append ``"partitionKey"`` and ``"sequence"`` last;
        unordered ones omit the keys entirely, keeping the baseline JSON form so
        pre-upgrade ledger rows and audit records still compare exactly.
        """
        if anonymous:
            event_id = None
        elif not isinstance(event_id, str) or not event_id or len(event_id) > 100:
            raise InvalidRequest("eventId must be a non-empty string of at most 100 characters")
        partition_key, sequence = _validate_ordering(partition_key, sequence, event_id is not None)
        normalized: dict[str, Any] = {"eventId": event_id}
        for key in key_order:
            if key == "eventId":
                continue
            value = payload.get(key)
            normalized[key] = value if value is not None else None
        # Ordering keys are appended only for ordered calls. An unordered
        # normalized request stays byte-identical to the baseline form, so a
        # ledger row written before this feature still replays after upgrade
        # (its stored JSON lacks the keys and so does every new unordered call);
        # old audit records are likewise never supplemented with ordering.
        if partition_key is not None:
            normalized["partitionKey"] = partition_key
            normalized["sequence"] = sequence
        return normalized

    def get(self, instance_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
        if row is None:
            raise InstanceNotFound(f"no instance {instance_id}")
        revision = row[3] if row[3] is not None else 1
        return _RevisionedResponse(
            {"id": instance_id, "workflow": row[0], "workflowVersion": row[2],
             "state": _read_state(row[1])},
            revision,
        )

    def list_instances(self, workflow: Any = None, status: Any = None,
                       after_id: Any = None, limit: Any = None) -> dict[str, Any]:
        """Read-only page of instances ordered by id ascending (byte order).

        ``workflow`` filters by exact workflow name, ``status`` by the state's
        status field (one of :data:`LIST_STATUSES`); both are omitted (None)
        when the caller did not ask for them. ``after_id`` is an exclusive
        byte-order boundary on the instance id — it is a position in the
        ordering, not a lookup, so it need not name an existing instance.
        ``limit`` is the raw query literal: a decimal integer in 1..100 with
        no leading zeros, defaulting to 50 when omitted. Any invalid shape
        raises InvalidRequest and nothing is written (the query is read-only).

        The page carries ``limit`` items at most; one extra row is fetched to
        decide ``nextCursor``, which is the id of the page's last item when
        further matches exist beyond it and None otherwise. Items expose the
        same ``id``/``workflow``/``workflowVersion``/``state`` shape as
        :meth:`get` (``workflowVersion`` is None for pre-versioning instances,
        and ``state`` goes through the same legacy backfill).
        """
        if workflow is not None and (
            not isinstance(workflow, str) or not workflow or len(workflow) > 100
        ):
            raise InvalidRequest("workflow must be a non-empty string of at most 100 characters")
        if status is not None and status not in LIST_STATUSES:
            raise InvalidRequest(
                "status must be one of running, completed, compensated, dead_lettered"
            )
        if after_id is not None and (
            not isinstance(after_id, str) or not after_id or len(after_id) > 200
        ):
            raise InvalidRequest("afterId must be a non-empty string of at most 200 characters")
        if limit is None:
            size = 50
        else:
            if (not isinstance(limit, str) or not limit or not limit.isascii()
                    or not limit.isdigit() or (len(limit) > 1 and limit[0] == "0")):
                raise InvalidRequest(
                    "limit must be a decimal integer between 1 and 100 without leading zeros"
                )
            size = int(limit)
            if not 1 <= size <= 100:
                raise InvalidRequest("limit must be between 1 and 100")
        clauses: list[str] = []
        params: list[Any] = []
        if workflow is not None:
            clauses.append("workflow = ?")
            params.append(workflow)
        if status is not None:
            clauses.append("json_extract(state, '$.status') = ?")
            params.append(status)
        if after_id is not None:
            clauses.append("id > ?")
            params.append(after_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            rows = self._db.execute(
                f"SELECT id, workflow, state, version FROM instances{where} "
                "ORDER BY id LIMIT ?",
                (*params, size + 1),
            ).fetchall()
        page = rows[:size]
        instances = [
            {"id": row_id, "workflow": name, "workflowVersion": version,
             "state": _read_state(raw_state)}
            for row_id, name, raw_state, version in page
        ]
        next_cursor = page[-1][0] if len(rows) > size else None
        return {"instances": instances, "nextCursor": next_cursor}


    def migrate(self, instance_id: str, version: Any, if_match: int | None = None) -> dict[str, Any]:
        """Re-pin a running instance to another existing version of the same workflow.

        The target version must have the same number of steps with the same ordered
        step names, and every step the instance has already entered (indexes
        0..current) must be defined identically; only not-yet-entered steps may
        differ in compensation/retry/await. The state itself is carried over
        untouched — status, step, index, attempt, completed, compensated, context,
        failure, waitingFor and deadlineAt all survive — and later advances run
        against the target version. Rejections change nothing: no state write, no
        ledger row, no audit record, no revision bump. A successful migration is a
        real instance change, so the revision advances by one even when the state
        body is carried over field for field. An optional ``if_match`` precondition
        works exactly as on :meth:`advance`.
        """
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise InvalidRequest("version must be a positive integer")
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            name, raw_state, current_version, raw_revision = row
            revision = raw_revision if raw_revision is not None else 1
            if if_match is not None and if_match != revision:
                raise InvalidTransition(f"instance version is {revision}, not {if_match}")
            target = self.workflow(name, version)  # unknown version -> NotFound
            state = _read_state(raw_state)
            if state["status"] != "running":
                raise InvalidTransition(f"instance is {state['status']}, not running")
            if current_version is None:
                raise InvalidTransition(
                    "instance has no recorded workflow version; advance it once before migrating"
                )
            current = self.workflow(name, current_version)
            self._check_compatible(current["steps"], target["steps"], int(state["index"]))
            next_revision = revision + 1
            self._db.execute(
                "UPDATE instances SET version = ?, instance_version = ? WHERE id = ?",
                (version, next_revision, instance_id),
            )
            self._db.commit()
        return _RevisionedResponse(
            {"id": instance_id, "workflow": name, "workflowVersion": version, "state": state},
            next_revision,
        )

    def migration_plan(self, instance_id: str, target_version: Any) -> dict[str, Any]:
        """Read-only migration preview: the plan ``migrate`` would follow, without
        changing anything.

        ``target_version`` is the raw query literal: a positive decimal integer
        with no leading zeros. Shape validation runs *before* the instance
        lookup, so an invalid query is 400 even for a missing instance; a valid
        query on a missing instance is 404, as is an unknown target version.
        Semantic incompatibility is not an error here — it is reported in the
        200 plan via ``compatible``/``reason``.

        The response carries the instance's current revision for ETag emission
        exactly like :meth:`get`, and the query itself advances nothing: no
        state, version, instance_version, ledger, audit or cursor writes, and
        no compensation, retry or wait side effects. Only the instance row and
        the immutable workflow versions are read.
        """
        if (not isinstance(target_version, str) or not target_version
                or not target_version.isascii() or not target_version.isdigit()
                or (len(target_version) > 1 and target_version[0] == "0")):
            raise InvalidRequest(
                "targetVersion must be a positive decimal integer without leading zeros"
            )
        version = int(target_version)
        if version < 1:
            raise InvalidRequest(
                "targetVersion must be a positive decimal integer without leading zeros"
            )
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            name, raw_state, current_version, raw_revision = row
            revision = raw_revision if raw_revision is not None else 1
            target = self.workflow(name, version)  # unknown target version -> NotFound
            state = _read_state(raw_state)
            current_steps = None
            if current_version is not None:
                current_steps = self.workflow(name, current_version)["steps"]
        index = int(state["index"])
        reason = self._migration_block_reason(state, current_steps, target["steps"], index)
        return _RevisionedResponse(
            {"id": instance_id, "workflow": name,
             "currentVersion": current_version, "targetVersion": version,
             "currentIndex": index, "state": state,
             "currentSteps": current_steps, "targetSteps": target["steps"],
             "compatible": reason is None, "reason": reason},
            revision,
        )

    @staticmethod
    def _migration_block_reason(state: dict[str, Any], current_steps: Any,
                                target_steps: list[dict[str, Any]], index: int) -> str | None:
        """The first reason a migration is blocked, or None when it is compatible.

        Mirrors :meth:`_check_compatible` (same pinning, running-only and
        entered-step immutability rules) but reports instead of raising, and
        adds the two plan-only preconditions ahead of the step comparison:
        the instance must be running and must have a recorded version (a
        legacy NULL version has no pinned definition to compare against).
        """
        if state["status"] != "running":
            return "instance_not_running"
        if current_steps is None:
            return "missing_recorded_version"
        if len(current_steps) != len(target_steps):
            return "step_count_changed"
        for current, target in zip(current_steps, target_steps):
            if current["name"] != target["name"]:
                return "step_name_changed"
        for position, (current, target) in enumerate(zip(current_steps, target_steps)):
            if position <= index and current != target:
                return "entered_step_changed"
        return None

    def recover(self, instance_id: str, if_match: int | None = None,
                now_ms: int | None = None) -> dict[str, Any]:
        """Explicitly recover a dead_lettered instance back to running.

        Recovery neither replays completed steps nor runs compensation, and it
        does not stand in for a later event or signal: it only re-arms the
        current step's wait against the instance's pinned workflow definition.
        Lookup, the If-Match precondition, the state-machine check, the state
        write and the revision bump happen under one lock and in one
        transaction. Rejections (unknown instance, stale precondition, any
        status other than dead_lettered) write nothing: no state, no ledger
        row, no audit record, no cursor change. A successful recovery is a real
        instance change: the revision advances by exactly one, but it appends
        neither ledger rows nor audit records — the accepted history keeps the
        meaning it had before recovery.
        """
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            name, raw_state, current_version, raw_revision = row
            revision = raw_revision if raw_revision is not None else 1
            if if_match is not None and if_match != revision:
                raise InvalidTransition(f"instance version is {revision}, not {if_match}")
            # Recovery runs against the pinned definition; a NULL legacy version
            # resolves the latest one and is backfilled on this change, exactly
            # like a normal advance would.
            workflow = self.workflow(name, current_version)
            version = current_version
            if version is None:
                version = self._db.execute(
                    "SELECT MAX(version) FROM workflow_versions WHERE name = ?", (name,)
                ).fetchone()[0]
            state = _read_state(raw_state)
            state = apply_recover(workflow, state, now_ms)
            next_revision = revision + 1
            self._db.execute(
                "UPDATE instances SET state = ?, version = ?, instance_version = ? WHERE id = ?",
                (json.dumps(state), version, next_revision, instance_id),
            )
            self._db.commit()
        return _RevisionedResponse(
            {"id": instance_id, "workflow": name, "workflowVersion": version, "state": state},
            next_revision,
        )

    @staticmethod
    def _check_compatible(current_steps: list[dict[str, Any]], target_steps: list[dict[str, Any]],
                          index: int) -> None:
        if len(current_steps) != len(target_steps):
            raise InvalidTransition("target version has a different number of steps")
        for position, (current, target) in enumerate(zip(current_steps, target_steps)):
            if current["name"] != target["name"]:
                raise InvalidTransition(
                    f"step {position} is {current['name']!r} in the current version "
                    f"but {target['name']!r} in the target version"
                )
            if position <= index and current != target:
                raise InvalidTransition(
                    f"step {position} ({current['name']!r}) was already entered and its "
                    "definition differs in the target version"
                )

    def audit(self, instance_id: str, limit: Any = None, after_seq: Any = None,
              kind: Any = None) -> dict[str, Any]:
        """Read-only audit view: current state plus the accepted calls in commit order.

        History rows come back as ``{"seq", "kind", "eventId", "request", "response"}``
        with seq strictly increasing from 1; instances that predate the audit table
        simply have an empty (or short) history — nothing is fabricated.

        Without any of ``limit``/``after_seq``/``kind`` the response is exactly the
        baseline shape (full history, no pagination fields). As soon as one of them
        is carried the call switches to paged mode: the parameters are validated
        *before* the instance is looked up (an invalid query is 400 even for a
        missing instance; a valid query on a missing instance is 404), and the
        response adds ``nextSeq`` — the seq of the page's last record when further
        matching records exist beyond it, else None. ``after_seq`` is a cursor on
        the unfiltered audit sequence: records are selected by ``seq > after_seq``
        first and only then filtered by ``kind``, so skipped kinds never disturb
        the seq ordering. The query is read-only: nothing is written to state,
        ledgers, audit or partition cursors either way.
        """
        paged = limit is not None or after_seq is not None or kind is not None
        if paged:
            size = _parse_audit_limit(limit)
            cursor = _parse_audit_after_seq(after_seq)
            if kind is not None and kind not in ("event", "signal"):
                raise InvalidRequest("kind must be 'event' or 'signal'")
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            if paged:
                clauses = ["instance_id = ?", "seq > ?"]
                params: list[Any] = [instance_id, cursor]
                if kind is not None:
                    clauses.append("kind = ?")
                    params.append(kind)
                # One extra row decides whether a next page exists.
                rows = self._db.execute(
                    "SELECT seq, kind, event_id, request, response FROM instance_audit "
                    f"WHERE {' AND '.join(clauses)} ORDER BY seq LIMIT ?",
                    (*params, size + 1),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT seq, kind, event_id, request, response FROM instance_audit "
                    "WHERE instance_id = ? ORDER BY seq",
                    (instance_id,),
                ).fetchall()
        if paged:
            page = rows[:size]
            next_seq: int | None = page[-1][0] if len(rows) > size else None
        else:
            page, next_seq = rows, None
        history = [
            {"seq": seq, "kind": record_kind, "eventId": event_id,
             "request": json.loads(request), "response": json.loads(response)}
            for seq, record_kind, event_id, request, response in page
        ]
        revision = row[3] if row[3] is not None else 1
        body: dict[str, Any] = {"id": instance_id, "workflow": row[0],
                                "workflowVersion": row[2],
                                "state": _read_state(row[1]), "history": history}
        if paged:
            body["nextSeq"] = next_seq
        return _RevisionedResponse(body, revision)

    @staticmethod
    def _timeline_entry(seq: int, kind: str, event_id: str | None,
                        request_raw: str, response_raw: str) -> dict[str, Any]:
        """Project one stored audit row into a timeline entry.

        The entry fixes seq/kind/eventId/request plus the state-diff fields read
        from that record's stored ``response.state`` (never re-derived by re-running
        the machine). The full response is omitted, as is ``context``. Records
        persisted before a field existed are projected with display defaults only —
        attempt 1, waitingFor/deadlineAt null — and nothing is rewritten in storage.
        Any other state key is carried through verbatim after the fixed fields.
        """
        state = json.loads(response_raw)["state"]
        entry: dict[str, Any] = {
            "seq": seq,
            "kind": kind,
            "eventId": event_id,
            "request": json.loads(request_raw),
            "status": state["status"],
            "step": state["step"],
            "index": state["index"],
            # Legacy audit rows predate attempt: show 1 without backfilling storage.
            "attempt": state.get("attempt", 1),
            "completed": state["completed"],
            "compensated": state["compensated"],
            # Legacy rows predate waits: both read as null, display-only.
            "waitingFor": state.get("waitingFor"),
            "deadlineAt": state.get("deadlineAt"),
            "failure": state["failure"],
        }
        placed = set(entry) | {"context"}
        for key, value in state.items():
            if key not in placed:
                entry[key] = value
        return entry

    def timeline(self, instance_id: str, limit: Any = None,
                 after_seq: Any = None) -> dict[str, Any]:
        """Read-only timeline view: current state plus audit records projected into
        compact state-diff entries in commit order.

        Shares the audit trail (same seq set, ascending order and ``afterSeq``
        cursor) but never returns ``context`` or the full ``response``; each entry
        fixes seq/kind/eventId/request and the response-state fields status, step,
        index, attempt, completed, compensated, waitingFor, deadlineAt, failure.
        Old records missing attempt/waitingFor/deadlineAt display 1/null/null
        without any rewrite. Pagination always applies: ``limit`` is a decimal
        integer in 1..200 (default 50), ``after_seq`` a non-negative decimal
        cursor (default 0), both validated *before* the instance lookup — an
        invalid query is 400 even for a missing instance, a valid query on a
        missing one is 404. ``nextSeq`` is the last entry's seq when further
        records remain beyond the page, else None (empty page: empty timeline,
        None). The query writes nothing: state, ledgers, audit and partition
        cursors are untouched.
        """
        size = _parse_timeline_limit(limit)
        cursor = _parse_audit_after_seq(after_seq)
        with self._lock:
            row = self._db.execute(
                "SELECT workflow, state, version, instance_version FROM instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if row is None:
                raise InstanceNotFound(f"no instance {instance_id}")
            # One extra row decides whether a next page exists.
            rows = self._db.execute(
                "SELECT seq, kind, event_id, request, response FROM instance_audit "
                "WHERE instance_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (instance_id, cursor, size + 1),
            ).fetchall()
        page = rows[:size]
        next_seq: int | None = page[-1][0] if len(rows) > size else None
        timeline = [self._timeline_entry(*record) for record in page]
        revision = row[3] if row[3] is not None else 1
        body: dict[str, Any] = {"id": instance_id, "workflow": row[0],
                                "workflowVersion": row[2],
                                "state": _read_state(row[1]),
                                "timeline": timeline, "nextSeq": next_seq}
        return _RevisionedResponse(body, revision)


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
            # Instance-carrying responses expose their revision as a quoted
            # decimal ETag (e.g. ETag: "2"); other responses send none.
            revision = getattr(body, "revision", None)
            if isinstance(revision, int):
                self.send_header("ETag", f'"{revision}"')
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

        def _list_query_args(self) -> dict[str, Any]:
            """Query arguments for GET /v1/instances.

            Only workflow/status/afterId/limit are known; a repeated or
            unknown parameter is 400. Values are handed to the engine as raw
            (percent-decoded) strings; shape validation happens there.
            """
            query = self.path.partition("?")[2]
            pairs = parse_qsl(query, keep_blank_values=True)
            names = [name for name, _ in pairs]
            if len(set(names)) != len(names):
                raise InvalidRequest("query parameters must not be repeated")
            unknown = sorted(set(names) - {"workflow", "status", "afterId", "limit"})
            if unknown:
                raise InvalidRequest(f"unknown query parameters: {unknown}")
            params = dict(pairs)
            return {
                "workflow": params.get("workflow"),
                "status": params.get("status"),
                "after_id": params.get("afterId"),
                "limit": params.get("limit"),
            }

        def _audit_query_args(self) -> dict[str, Any]:
            """Query arguments for GET /v1/instances/{id}/audit.

            Only limit/afterSeq/kind are known; a repeated or unknown parameter
            is 400. Values are handed to the engine as raw (percent-decoded)
            strings; shape validation happens there, before the instance lookup.
            No query string at all means the baseline unpaged response.
            """
            query = self.path.partition("?")[2]
            pairs = parse_qsl(query, keep_blank_values=True)
            names = [name for name, _ in pairs]
            if len(set(names)) != len(names):
                raise InvalidRequest("query parameters must not be repeated")
            unknown = sorted(set(names) - {"limit", "afterSeq", "kind"})
            if unknown:
                raise InvalidRequest(f"unknown query parameters: {unknown}")
            params = dict(pairs)
            return {
                "limit": params.get("limit"),
                "after_seq": params.get("afterSeq"),
                "kind": params.get("kind"),
            }

        def _timeline_query_args(self) -> dict[str, Any]:
            """Query arguments for GET /v1/instances/{id}/timeline.

            Only limit/afterSeq are known; a repeated or unknown parameter is
            400. Values are handed to the engine as raw (percent-decoded)
            strings; shape validation happens there, before the instance lookup.
            Unlike /audit there is no unpaged mode: omitting both just applies
            the defaults (limit 50, afterSeq 0).
            """
            query = self.path.partition("?")[2]
            pairs = parse_qsl(query, keep_blank_values=True)
            names = [name for name, _ in pairs]
            if len(set(names)) != len(names):
                raise InvalidRequest("query parameters must not be repeated")
            unknown = sorted(set(names) - {"limit", "afterSeq"})
            if unknown:
                raise InvalidRequest(f"unknown query parameters: {unknown}")
            params = dict(pairs)
            return {
                "limit": params.get("limit"),
                "after_seq": params.get("afterSeq"),
            }

        def _migration_plan_query_args(self) -> dict[str, Any]:
            """Query arguments for GET /v1/instances/{id}/migration-plan.

            Only targetVersion is known; a repeated or unknown parameter is
            400. The value is handed to the engine as a raw (percent-decoded)
            string; shape validation happens there, before the instance
            lookup.
            """
            query = self.path.partition("?")[2]
            pairs = parse_qsl(query, keep_blank_values=True)
            names = [name for name, _ in pairs]
            if len(set(names)) != len(names):
                raise InvalidRequest("query parameters must not be repeated")
            unknown = sorted(set(names) - {"targetVersion"})
            if unknown:
                raise InvalidRequest(f"unknown query parameters: {unknown}")
            params = dict(pairs)
            return {"target_version": params.get("targetVersion")}

        def do_GET(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if parts == ["v1", "workflows"]:
                    return self._send(200, {"workflows": engine.workflow_names()})
                if len(parts) == 3 and parts[:2] == ["v1", "workflows"]:
                    return self._send(200, engine.workflow_info(parts[2]))
                if parts == ["v1", "instances"]:
                    return self._send(200, engine.list_instances(**self._list_query_args()))
                if len(parts) == 3 and parts[:2] == ["v1", "instances"]:
                    return self._send(200, engine.get(parts[2]))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "audit":
                    return self._send(200, engine.audit(parts[2], **self._audit_query_args()))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "timeline":
                    return self._send(200, engine.timeline(parts[2], **self._timeline_query_args()))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "migration-plan":
                    return self._send(200, engine.migration_plan(parts[2], **self._migration_plan_query_args()))
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
                return self._send(200, {"workflow": parts[2], "version": workflow["version"],
                                        "steps": workflow["steps"]})
            except SagaError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802
            try:
                parts = self._parts()
                if len(parts) == 4 and parts[:2] == ["v1", "workflows"] and parts[3] == "instances":
                    body = self._read_json() or {}
                    if not isinstance(body, dict) or set(body) - {"context", "version"}:
                        raise InvalidRequest('body must be {"context": {...}, "version": <int>} when present')
                    return self._send(201, engine.start(parts[2], body.get("context"), body.get("version")))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "migrate":
                    body = self._read_json()
                    if_match = parse_if_match(self.headers.get("If-Match"))
                    if not isinstance(body, dict) or set(body) - {"version"} or "version" not in body:
                        raise InvalidRequest('body must be {"version": <positive integer>}')
                    return self._send(200, engine.migrate(parts[2], body["version"], if_match))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "recover":
                    body = self._read_json()
                    if_match = parse_if_match(self.headers.get("If-Match"))
                    # Recovery carries no payload: only an empty JSON object is accepted.
                    if body != {}:
                        raise InvalidRequest("body must be an empty JSON object {}")
                    return self._send(200, engine.recover(parts[2], if_match))
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "events":
                    body = self._read_json()
                    if_match = parse_if_match(self.headers.get("If-Match"))
                    if not isinstance(body, dict) or set(body) - {"outcome", "detail", "eventId",
                                                                  "partitionKey", "sequence"}:
                        raise InvalidRequest(
                            'body must be {"outcome": "succeeded|failed|timed_out", "detail": ..., '
                            '"eventId": "...", "partitionKey": "...", "sequence": <int>}'
                        )
                    event_id = body["eventId"] if "eventId" in body else _UNSET
                    partition_key = body["partitionKey"] if "partitionKey" in body else _UNSET
                    sequence = body["sequence"] if "sequence" in body else _UNSET
                    return self._send(
                        200, engine.advance(parts[2], body.get("outcome"), body.get("detail"),
                                            event_id, if_match, partition_key, sequence)
                    )
                if len(parts) == 4 and parts[:2] == ["v1", "instances"] and parts[3] == "signals":
                    body = self._read_json()
                    if_match = parse_if_match(self.headers.get("If-Match"))
                    if not isinstance(body, dict) or set(body) - {"event", "detail", "eventId",
                                                                  "partitionKey", "sequence"}:
                        raise InvalidRequest(
                            'body must be {"event": "<name>", "detail": ..., "eventId": "...", '
                            '"partitionKey": "...", "sequence": <int>}'
                        )
                    if "event" not in body:
                        raise InvalidRequest('body must be {"event": "<name>", ...}')
                    event_id = body["eventId"] if "eventId" in body else _UNSET
                    partition_key = body["partitionKey"] if "partitionKey" in body else _UNSET
                    sequence = body["sequence"] if "sequence" in body else _UNSET
                    return self._send(
                        200, engine.signal(parts[2], body.get("event"), body.get("detail"),
                                           event_id, if_match, partition_key, sequence)
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
