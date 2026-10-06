"""Baseline tests for the saga orchestrator."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from saga import Engine, InstanceNotFound, InvalidRequest, InvalidTransition, NotFound, SagaError, WORKFLOWS, apply_outcome, apply_signal, initial_state


class TransitionUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = WORKFLOWS["order"]

    def test_happy_path_advances_then_completes(self) -> None:
        state = initial_state(self.workflow, {"customer": "c1"})
        self.assertEqual((state["step"], state["index"]), ("reserve-stock", 0))
        state = apply_outcome(self.workflow, state, "succeeded")
        self.assertEqual(state["completed"], ["reserve-stock"])
        state = apply_outcome(self.workflow, state, "succeeded")
        self.assertEqual(state["step"], "create-shipment")
        state = apply_outcome(self.workflow, state, "succeeded")
        self.assertEqual(state["status"], "completed")
        self.assertIsNone(state["step"])

    def test_failure_runs_compensations_in_reverse_order(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "succeeded")
        state = apply_outcome(self.workflow, state, "failed", {"reason": "card declined"})
        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["compensated"], ["refund-payment", "release-stock"])
        self.assertEqual(state["failure"]["detail"], {"reason": "card declined"})

    def test_terminal_state_rejects_further_events(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "failed", None)
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "succeeded")

    def test_invalid_outcome_and_definition_are_rejected(self) -> None:
        state = initial_state(self.workflow, {})
        with self.assertRaises(InvalidRequest):
            apply_outcome(self.workflow, state, "maybe")
        engine = Engine()
        try:
            for bad in ["nope", {"steps": []}, {"steps": [{"name": ""}]}, {"steps": [{"name": "a", "x": 1}]}]:
                with self.assertRaises(InvalidRequest):
                    engine.define("w", bad)
        finally:
            engine.close()


class HttpSurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_health_define_start_advance_and_get(self) -> None:
        self.assertEqual(self.call("GET", "/health")[0], 200)
        status, body = self.call("PUT", "/v1/workflows/mini", {"steps": [{"name": "a", "compensation": "undo-a"}]})
        self.assertEqual((status, body["workflow"]), (200, "mini"))
        status, body = self.call("POST", "/v1/workflows/mini/instances", {"context": {"x": 1}})
        self.assertEqual(status, 201)
        instance = body["id"]
        status, body = self.call("POST", f"/v1/instances/{instance}/events", {"outcome": "succeeded"})
        self.assertEqual((status, body["state"]["status"]), (200, "completed"))
        self.assertEqual(self.call("GET", f"/v1/instances/{instance}")[1]["state"]["status"], "completed")

    def test_failure_path_and_errors(self) -> None:
        status, body = self.call("POST", "/v1/workflows/order/instances", {})
        instance = body["id"]
        self.call("POST", f"/v1/instances/{instance}/events", {"outcome": "succeeded"})
        status, body = self.call("POST", f"/v1/instances/{instance}/events", {"outcome": "failed"})
        self.assertEqual(body["state"]["compensated"], ["refund-payment", "release-stock"])
        self.assertEqual(self.call("GET", "/v1/instances/missing")[0], 404)
        self.assertEqual(self.call("POST", "/v1/instances/missing/events", {"outcome": "succeeded"})[0], 404)
        self.assertEqual(self.call("POST", "/v1/nope", {})[0], 404)


class IdempotencyEngineTests(unittest.TestCase):
    """Direct Engine-level coverage of the event ledger."""

    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "order") -> str:
        return self.engine.start(workflow, {})["id"]

    def test_repeated_event_returns_first_response_without_advancing(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.assertEqual(first["state"]["completed"], ["reserve-stock"])
        first_json = json.dumps(first, sort_keys=True)
        second = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.assertEqual(json.dumps(second, sort_keys=True), first_json)
        # The replay must hand back the state as of the FIRST processing, and the
        # instance itself must not have advanced or double-counted the step.
        current = self.engine.get(iid)["state"]
        self.assertEqual(current["index"], 1)
        self.assertEqual(current["completed"], ["reserve-stock"])

    def test_omitted_detail_equals_explicit_null_on_replay(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", event_id="e-1")
        replayed = self.engine.advance(iid, "succeeded", None, event_id="e-1")
        self.assertEqual(replayed, first)
        # Reverse direction too: explicit null first, omitted later.
        iid2 = self._start()
        first2 = self.engine.advance(iid2, "failed", None, event_id="e-2")
        replayed2 = self.engine.advance(iid2, "failed", event_id="e-2")
        self.assertEqual(replayed2, first2)

    def test_same_event_id_with_different_outcome_is_409_and_state_unchanged(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", event_id="e-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", {"x": 1}, event_id="e-1")
        state = self.engine.get(iid)["state"]
        self.assertEqual(state["index"], 1)
        self.assertEqual(state["status"], "running")
        # The original event still replays cleanly after the conflicting attempts.
        replayed = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.assertEqual(replayed["state"]["index"], 1)

    def test_same_event_id_with_different_detail_is_409(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", {"a": 1}, event_id="e-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", {"a": 2}, event_id="e-1")
        # Detail equality is by JSON value, not by key order or whitespace.
        same = self.engine.advance(iid, "succeeded", json.loads('{"a": 1}'), event_id="e-1")
        self.assertEqual(same["state"]["completed"], ["reserve-stock"])

    def test_event_id_scoped_per_instance(self) -> None:
        a = self._start()
        b = self._start()
        ra = self.engine.advance(a, "succeeded", event_id="shared-id")
        rb = self.engine.advance(b, "succeeded", event_id="shared-id")
        self.assertEqual(ra["id"], a)
        self.assertEqual(rb["id"], b)

    def test_invalid_event_id_shapes_are_400_and_write_nothing(self) -> None:
        iid = self._start()
        for bad in (None, "", 123, 12.5, ["x"], {"x": 1}, "x" * 101):
            with self.assertRaises(InvalidRequest):
                self.engine.advance(iid, "succeeded", event_id=bad)
        rows = self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0]
        self.assertEqual(rows, 0)
        # Instance still at its initial state.
        self.assertEqual(self.engine.get(iid)["state"]["index"], 0)

    def test_invalid_outcome_terminal_and_missing_instance_write_no_ledger_rows(self) -> None:
        iid = self._start()
        with self.assertRaises(InvalidRequest):
            self.engine.advance(iid, "bogus", event_id="e-1")
        self.engine.advance(iid, "failed", event_id="e-2")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-3")
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("no-such-instance", "succeeded", event_id="e-4")
        rows = self.engine._db.execute(
            "SELECT event_id FROM instance_events WHERE instance_id = ?", (iid,)
        ).fetchall()
        self.assertEqual(rows, [("e-2",)])

    def test_event_chain_then_replay_old_events_keeps_current_state(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="step-0")
        self.engine.advance(iid, "succeeded", event_id="step-1")
        third = self.engine.advance(iid, "succeeded", event_id="step-2")
        self.assertEqual(third["state"]["status"], "completed")
        # Replaying step-0 after completion returns the historical 200 response and
        # must not revive or otherwise mutate the now-terminal instance.
        replay = self.engine.advance(iid, "succeeded", event_id="step-0")
        self.assertEqual(replay["state"]["index"], 1)
        self.assertEqual(replay["state"]["status"], "running")
        current = self.engine.get(iid)["state"]
        self.assertEqual(current["status"], "completed")
        self.assertEqual(len(current["completed"]), 3)

    def test_failure_event_replay_does_not_reaccumulate_compensations(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="ok-1")
        failed = self.engine.advance(iid, "failed", {"reason": "boom"}, event_id="fail-1")
        self.assertEqual(failed["state"]["compensated"], ["refund-payment", "release-stock"])
        replay = self.engine.advance(iid, "failed", {"reason": "boom"}, event_id="fail-1")
        self.assertEqual(replay["state"]["compensated"], ["refund-payment", "release-stock"])
        self.assertEqual(self.engine.get(iid)["state"]["compensated"],
                         ["refund-payment", "release-stock"])

    def test_without_event_id_baseline_behavior_unchanged(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "failed")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded")
        rows = self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0]
        self.assertEqual(rows, 0)

    def test_concurrent_same_event_id_one_writer_one_replay(self) -> None:
        iid = self._start()
        barrier = threading.Barrier(2)
        results: list[dict[str, Any]] = []
        errors: list[Exception] = []

        def submit() -> None:
            try:
                barrier.wait()
                results.append(self.engine.advance(iid, "succeeded", event_id="race-1"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=submit) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.engine.get(iid)["state"]["completed"], ["reserve-stock"])
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0], 1
        )

    def test_concurrent_distinct_events_equal_some_serial_order(self) -> None:
        iid = self._start()
        barrier = threading.Barrier(3)
        outcomes = ["succeeded", "succeeded", "succeeded"]
        results: list[dict[str, Any]] = [None, None, None]  # type: ignore[list-item]
        errors: list[Exception] = []

        def submit(index: int, event_id: str) -> None:
            try:
                barrier.wait()
                results[index] = self.engine.advance(iid, outcomes[index], event_id=event_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=submit, args=(i, f"ev-{i}")) for i in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        statuses = [r["state"]["status"] for r in results]
        self.assertEqual(sorted(statuses), ["completed", "running", "running"])
        current = self.engine.get(iid)["state"]
        self.assertEqual(current["status"], "completed")
        self.assertEqual(len(current["completed"]), 3)


class IdempotencyPersistenceTests(unittest.TestCase):
    """Ledger survival across reopen and compatibility with pre-existing sqlite files."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_replay_hits_saved_response_after_reopen(self) -> None:
        engine = Engine(self.path)
        iid = engine.start("order", {})["id"]
        first = engine.advance(iid, "succeeded", {"note": "first"}, event_id="durable-1")
        engine.advance(iid, "succeeded", event_id="durable-2")
        engine.close()

        engine = Engine(self.path)
        try:
            replayed = engine.advance(iid, "succeeded", {"note": "first"}, event_id="durable-1")
            self.assertEqual(replayed, first)
            # Historical state inside the replayed response, not the advanced one.
            self.assertEqual(replayed["state"]["completed"], ["reserve-stock"])
            current = engine.get(iid)["state"]
            self.assertEqual(current["index"], 2)
            with self.assertRaises(InvalidTransition):
                engine.advance(iid, "failed", event_id="durable-1")
        finally:
            engine.close()

    def test_legacy_sqlite_file_without_event_table_still_works(self) -> None:
        # Create a database with only the baseline schema and one mid-flight instance.
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(WORKFLOWS["order"], {})
        state = apply_outcome(WORKFLOWS["order"], state, "succeeded")
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            current = engine.get("legacy-id")["state"]
            self.assertEqual(current["completed"], ["reserve-stock"])
            # Old instance can now use eventIds and keeps advancing.
            result = engine.advance("legacy-id", "succeeded", event_id="new-era-1")
            self.assertEqual(result["state"]["step"], "create-shipment")
            replay = engine.advance("legacy-id", "succeeded", event_id="new-era-1")
            self.assertEqual(replay, result)
        finally:
            engine.close()


class IdempotencyHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def _start(self) -> str:
        return self.call("POST", "/v1/workflows/order/instances", {})[1]["id"]

    def test_http_replay_returns_first_200_verbatim(self) -> None:
        iid = self._start()
        status, first = self.call("POST", f"/v1/instances/{iid}/events",
                                  {"outcome": "succeeded", "eventId": "evt-1"})
        self.assertEqual(status, 200)
        status, second = self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded", "detail": None, "eventId": "evt-1"})
        self.assertEqual(status, 200)
        self.assertEqual(second, first)

    def test_http_conflict_and_validation_statuses(self) -> None:
        iid = self._start()
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded", "eventId": "evt-1"})[0], 200)
        status, body = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "failed", "eventId": "evt-1"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "invalid_transition")
        for bad_event_id in (None, "", "x" * 101):
            status, body = self.call("POST", f"/v1/instances/{iid}/events",
                                     {"outcome": "succeeded", "eventId": bad_event_id})
            self.assertEqual(status, 400, bad_event_id)
            self.assertEqual(body["error"]["code"], "invalid_request")

    def test_http_terminal_replay_is_still_200(self) -> None:
        iid = self._start()
        status, first = self.call("POST", f"/v1/instances/{iid}/events",
                                  {"outcome": "failed", "eventId": "fin"})
        self.assertEqual(status, 200)
        status, replay = self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "failed", "eventId": "fin"})
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        # A fresh eventId against the terminal instance is not replayed and writes nothing.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded", "eventId": "other"})[0], 409)


class RetryDefinitionTests(unittest.TestCase):
    """Validation of the optional per-step retry object."""

    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def test_valid_retry_boundaries_accepted(self) -> None:
        for attempts in (2, 10):
            workflow = self.engine.define(
                f"w-{attempts}", {"steps": [{"name": "a", "retry": {"maxAttempts": attempts}}]}
            )
            self.assertEqual(workflow["steps"][0]["retry"], {"maxAttempts": attempts})

    def test_invalid_retry_shapes_are_400_and_keep_existing_definition(self) -> None:
        self.engine.define("w", {"steps": [{"name": "a", "retry": {"maxAttempts": 3}}]})
        bad_retries = [
            {},                          # missing maxAttempts
            {"maxAttempts": 3, "x": 1},  # unknown field
            {"maxAttempts": True},       # boolean
            {"maxAttempts": 2.5},        # non-integer
            {"maxAttempts": "3"},        # non-integer
            {"maxAttempts": 1},          # below range
            {"maxAttempts": 11},         # above range
            None,                        # not an object
            [("maxAttempts", 3)],        # not a dict
        ]
        for bad in bad_retries:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.define("w", {"steps": [{"name": "a", "retry": bad}]})
        # The previously stored definition is untouched.
        self.assertEqual(self.engine.workflow("w")["steps"][0]["retry"], {"maxAttempts": 3})

    def test_step_without_retry_keeps_baseline_shape(self) -> None:
        workflow = self.engine.define("plain", {"steps": [{"name": "a", "compensation": "undo-a"}]})
        self.assertNotIn("retry", workflow["steps"][0])


class RetryTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = {
            "steps": [
                {"name": "first", "compensation": "undo-first"},
                {"name": "flaky", "compensation": "undo-flaky", "retry": {"maxAttempts": 3}},
                {"name": "last", "compensation": None},
            ]
        }

    def test_initial_state_starts_at_attempt_1(self) -> None:
        state = initial_state(self.workflow, {})
        self.assertEqual(state["attempt"], 1)

    def test_retryable_failure_stays_running_and_bumps_attempt(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "succeeded")
        self.assertEqual(state["attempt"], 1)  # reset on entering the new step
        state = apply_outcome(self.workflow, state, "failed", {"reason": "boom"})
        self.assertEqual(state["status"], "running")
        self.assertEqual((state["step"], state["index"]), ("flaky", 1))
        self.assertEqual(state["attempt"], 2)
        self.assertEqual(state["failure"], {"step": "flaky", "detail": {"reason": "boom"}})
        self.assertEqual(state["completed"], ["first"])
        self.assertEqual(state["compensated"], [])

    def test_success_after_retries_advances_and_clears_failure(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "succeeded")
        state = apply_outcome(self.workflow, state, "failed", "e1")
        state = apply_outcome(self.workflow, state, "failed", "e2")
        self.assertEqual(state["attempt"], 3)
        state = apply_outcome(self.workflow, state, "succeeded")
        self.assertEqual((state["step"], state["index"]), ("last", 2))
        self.assertEqual(state["attempt"], 1)
        self.assertIsNone(state["failure"])
        self.assertEqual(state["completed"], ["first", "flaky"])

    def test_exhausted_attempts_compensate_in_reverse_order(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "succeeded")
        state = apply_outcome(self.workflow, state, "failed", "e1")
        state = apply_outcome(self.workflow, state, "failed", "e2")
        state = apply_outcome(self.workflow, state, "failed", "e3")
        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["compensated"], ["undo-flaky", "undo-first"])
        self.assertEqual(state["completed"], ["first"])
        self.assertEqual(state["failure"], {"step": "flaky", "detail": "e3"})

    def test_step_without_retry_compensates_immediately(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "failed", "nope")
        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["compensated"], ["undo-first"])

    def test_legacy_state_without_attempt_is_backfilled(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_outcome(self.workflow, state, "succeeded")
        del state["attempt"]  # simulate a pre-retry persisted instance
        state = apply_outcome(self.workflow, state, "failed", "boom")
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["attempt"], 2)  # treated as attempt=1, then bumped
        # Success path backfills too.
        legacy = initial_state(self.workflow, {})
        del legacy["attempt"]
        legacy = apply_outcome(self.workflow, legacy, "succeeded")
        self.assertEqual(legacy["attempt"], 1)


class RetryEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("retryable", {"steps": [
            {"name": "a", "compensation": "undo-a"},
            {"name": "b", "compensation": "undo-b", "retry": {"maxAttempts": 2}},
        ]})

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self) -> str:
        return self.engine.start("retryable", {})["id"]

    def test_failed_attempt_events_are_ledgered_and_replay_verbatim(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-0")
        first = self.engine.advance(iid, "failed", {"try": 1}, event_id="e-1")
        self.assertEqual(first["state"]["status"], "running")
        self.assertEqual(first["state"]["attempt"], 2)
        replay = self.engine.advance(iid, "failed", {"try": 1}, event_id="e-1")
        self.assertEqual(replay, first)  # historical mid-retry state, not current state
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", {"try": "different"}, event_id="e-1")
        # The instance itself only recorded one failure for e-1.
        self.assertEqual(self.engine.get(iid)["state"]["attempt"], 2)

    def test_retry_chain_then_exhaustion_via_events(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-0")
        self.engine.advance(iid, "failed", "x", event_id="e-1")
        final = self.engine.advance(iid, "failed", "x", event_id="e-2")
        self.assertEqual(final["state"]["status"], "compensated")
        self.assertEqual(final["state"]["compensated"], ["undo-b", "undo-a"])
        rows = self.engine._db.execute(
            "SELECT event_id FROM instance_events WHERE instance_id = ? ORDER BY event_id", (iid,)
        ).fetchall()
        self.assertEqual(rows, [("e-0",), ("e-1",), ("e-2",)])

    def test_legacy_persisted_instance_without_attempt_advances(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            legacy = sqlite3.connect(path)
            legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
            state = initial_state(WORKFLOWS["order"], {})
            assert "attempt" in state
            del state["attempt"]
            legacy.execute("INSERT INTO instances VALUES (?, ?, ?)", ("old-1", "order", json.dumps(state)))
            legacy.commit()
            legacy.close()
            engine = Engine(path)
            try:
                # Readable as-is; first advance backfills attempt.
                self.assertNotIn("attempt", engine.get("old-1")["state"])
                result = engine.advance("old-1", "succeeded", event_id="ev-1")
                self.assertEqual(result["state"]["attempt"], 1)
                self.assertEqual(engine.get("old-1")["state"]["attempt"], 1)
            finally:
                engine.close()
        finally:
            os.unlink(path)


class RetryHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_put_rejects_bad_retry_and_preserves_existing_workflow(self) -> None:
        status, _ = self.call("PUT", "/v1/workflows/rw", {"steps": [{"name": "a", "retry": {"maxAttempts": 2}}]})
        self.assertEqual(status, 200)
        for bad in ({"retry": {}}, {"retry": {"maxAttempts": 1}}, {"retry": {"maxAttempts": 11}},
                    {"retry": {"maxAttempts": True}}, {"retry": {"maxAttempts": 2, "backoff": 1}}):
            status, body = self.call("PUT", "/v1/workflows/rw", {"steps": [{"name": "a", **bad}]})
            self.assertEqual(status, 400, bad)
            self.assertEqual(body["error"]["code"], "invalid_request")
        # Redefine with a valid body and the retry round-trips through the API.
        status, body = self.call("PUT", "/v1/workflows/rw",
                                 {"steps": [{"name": "a", "retry": {"maxAttempts": 2}}]})
        self.assertEqual(status, 200)
        self.assertEqual(body["steps"][0]["retry"], {"maxAttempts": 2})

    def test_http_retry_flow_end_to_end(self) -> None:
        self.call("PUT", "/v1/workflows/rflow", {"steps": [
            {"name": "s1", "compensation": "c1"},
            {"name": "s2", "compensation": "c2", "retry": {"maxAttempts": 2}},
        ]})
        status, body = self.call("POST", "/v1/workflows/rflow/instances", {})
        iid = body["id"]
        self.assertEqual(body["state"]["attempt"], 1)
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        status, body = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "failed", "detail": "flaky", "eventId": "f1"})
        self.assertEqual(status, 200)
        self.assertEqual((body["state"]["status"], body["state"]["attempt"]), ("running", 2))
        # Replay of the same eventId returns the historical mid-retry response.
        status, replay = self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "failed", "detail": "flaky", "eventId": "f1"})
        self.assertEqual((status, replay), (200, body))
        status, body = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        self.assertEqual(body["state"]["status"], "completed")
        self.assertIsNone(body["state"]["failure"])


class AwaitDefinitionTests(unittest.TestCase):
    """Validation of the optional per-step await object."""

    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def test_valid_await_accepted_and_round_trips(self) -> None:
        workflow = self.engine.define(
            "w", {"steps": [{"name": "a", "compensation": "undo-a", "await": {"event": "approved"}}]}
        )
        self.assertEqual(workflow["steps"][0]["await"], {"event": "approved"})
        for event in ("e", "x" * 100):
            workflow = self.engine.define(
                f"w-{len(event)}", {"steps": [{"name": "a", "await": {"event": event}}]}
            )
            self.assertEqual(workflow["steps"][0]["await"], {"event": event})

    def test_step_without_await_keeps_baseline_shape(self) -> None:
        workflow = self.engine.define("plain", {"steps": [{"name": "a", "compensation": "undo-a"}]})
        self.assertNotIn("await", workflow["steps"][0])

    def test_invalid_await_shapes_are_400_and_keep_existing_definition(self) -> None:
        self.engine.define("w", {"steps": [{"name": "a", "await": {"event": "go"}}]})
        bad_awaits = [
            None,
            "go",
            ["go"],
            {},
            {"x": "go"},
            {"event": "go", "x": 1},
            {"event": None},
            {"event": ""},
            {"event": 5},
            {"event": True},
            {"event": "x" * 101},
        ]
        for bad in bad_awaits:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.define("w", {"steps": [{"name": "a", "await": bad}]})
        self.assertEqual(self.engine.workflow("w")["steps"][0]["await"], {"event": "go"})
        # Unknown fields on the step itself are still rejected.
        with self.assertRaises(InvalidRequest):
            self.engine.define("w", {"steps": [{"name": "a", "waitingOn": "go"}]})
        self.assertEqual(self.engine.workflow("w")["steps"][0]["await"], {"event": "go"})


def _await_workflow() -> dict:
    return {
        "steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "cancel-ship", "await": {"event": "shipped"}},
        ]
    }


class AwaitTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = _await_workflow()

    def test_arriving_at_await_step_keeps_running_and_sets_waiting_for(self) -> None:
        state = initial_state(self.workflow, {})
        self.assertEqual(state["status"], "running")
        self.assertEqual((state["step"], state["index"], state["attempt"]), ("ask", 0, 1))
        self.assertEqual(state["waitingFor"], "approved")
        self.assertEqual(state["completed"], [])

    def test_events_are_blocked_while_waiting(self) -> None:
        state = initial_state(self.workflow, {})
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "succeeded")
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "failed", {"reason": "nope"})

    def test_matching_signal_completes_step_like_success(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_signal(self.workflow, state, "approved", {"by": "manager"})
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["completed"], ["ask"])
        self.assertIsNone(state["failure"])
        self.assertEqual(state["attempt"], 1)
        self.assertEqual((state["step"], state["index"]), ("do", 1))
        self.assertIsNone(state["waitingFor"])

    def test_plain_step_takes_events_then_next_await_waits(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_signal(self.workflow, state, "approved")
        state = apply_outcome(self.workflow, state, "succeeded")
        self.assertEqual((state["step"], state["index"]), ("ship", 2))
        self.assertEqual(state["waitingFor"], "shipped")
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "succeeded")
        state = apply_signal(self.workflow, state, "shipped")
        self.assertEqual(state["status"], "completed")
        self.assertIsNone(state["step"])
        self.assertIsNone(state["waitingFor"])
        self.assertEqual(state["completed"], ["ask", "do", "ship"])

    def test_mismatched_or_unexpected_signals_are_409(self) -> None:
        state = initial_state(self.workflow, {})
        with self.assertRaises(InvalidTransition):
            apply_signal(self.workflow, state, "rejected")
        state = apply_signal(self.workflow, state, "approved")
        # Plain step is not waiting for anything.
        with self.assertRaises(InvalidTransition):
            apply_signal(self.workflow, state, "approved")
        state = apply_outcome(self.workflow, state, "succeeded")
        with self.assertRaises(InvalidTransition):
            apply_signal(self.workflow, state, "approved")

    def test_terminal_state_rejects_signals(self) -> None:
        state = initial_state(self.workflow, {})
        state = apply_signal(self.workflow, state, "approved")
        state = apply_outcome(self.workflow, state, "failed")
        self.assertEqual(state["status"], "compensated")
        with self.assertRaises(InvalidTransition):
            apply_signal(self.workflow, state, "approved")

    def test_legacy_state_without_waiting_for_reads_as_null(self) -> None:
        state = initial_state(WORKFLOWS["order"], {})
        del state["waitingFor"]  # simulate a pre-await persisted instance
        # Original events keep working.
        advanced = apply_outcome(WORKFLOWS["order"], state, "succeeded")
        self.assertEqual(advanced["attempt"], 1)
        self.assertIsNone(advanced.get("waitingFor"))
        # Such an instance is not waiting, so signals are refused.
        with self.assertRaises(InvalidTransition):
            apply_signal(WORKFLOWS["order"], state, "anything")


class SignalEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self) -> str:
        return self.engine.start("approval", {})["id"]

    def test_signal_advances_and_unblocks_events(self) -> None:
        iid = self._start()
        self.assertEqual(self.engine.get(iid)["state"]["waitingFor"], "approved")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded")
        result = self.engine.signal(iid, "approved", {"note": "ok"})
        self.assertEqual(result["state"]["completed"], ["ask"])
        self.assertIsNone(result["state"]["waitingFor"])
        # Signal detail is not written into instance state.
        self.assertNotIn("note", json.dumps(result["state"]))
        advanced = self.engine.advance(iid, "succeeded")
        self.assertEqual(advanced["state"]["waitingFor"], "shipped")
        done = self.engine.signal(iid, "shipped")
        self.assertEqual(done["state"]["status"], "completed")
        self.assertIsNone(done["state"]["step"])
        self.assertIsNone(done["state"]["waitingFor"])

    def test_signal_rejections_write_no_ledger_rows(self) -> None:
        iid = self._start()
        with self.assertRaises(InvalidRequest):
            self.engine.signal(iid, "", event_id="s-0")
        with self.assertRaises(InvalidRequest):
            self.engine.signal(iid, "approved", event_id="")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "rejected", event_id="s-1")
        with self.assertRaises(InstanceNotFound):
            self.engine.signal("missing", "approved", event_id="s-2")
        rows = self.engine._db.execute("SELECT COUNT(*) FROM instance_signals").fetchone()[0]
        self.assertEqual(rows, 0)
        state = self.engine.get(iid)["state"]
        self.assertEqual((state["index"], state["waitingFor"]), (0, "approved"))
        # Advance past the awaiting step; the now-plain current step refuses signals.
        self.engine.signal(iid, "approved", event_id="s-3")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", event_id="s-4")
        ids = [r[0] for r in self.engine._db.execute(
            "SELECT event_id FROM instance_signals WHERE instance_id = ?", (iid,)).fetchall()]
        self.assertEqual(ids, ["s-3"])

    def test_signal_replay_returns_first_response_verbatim(self) -> None:
        iid = self._start()
        first = self.engine.signal(iid, "approved", {"v": 1}, event_id="sig-1")
        first_json = json.dumps(first, sort_keys=True)
        second = self.engine.signal(iid, "approved", {"v": 1}, event_id="sig-1")
        self.assertEqual(json.dumps(second, sort_keys=True), first_json)
        # Omitted detail equals explicit null detail (fresh instance: after sig-1 the
        # current step is the plain "do" and no longer waits).
        iid2 = self._start()
        third = self.engine.signal(iid2, "approved", None, event_id="sig-2")
        fourth = self.engine.signal(iid2, "approved", event_id="sig-2")
        self.assertEqual(fourth, third)
        # Current state only advanced once per distinct signal.
        state = self.engine.get(iid)["state"]
        self.assertEqual(state["index"], 1)
        self.assertEqual(state["completed"], ["ask"])

    def test_same_signal_id_with_different_payload_is_409(self) -> None:
        iid = self._start()
        self.engine.signal(iid, "approved", {"v": 1}, event_id="sig-1")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "rejected", {"v": 1}, event_id="sig-1")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", {"v": 2}, event_id="sig-1")
        # Equal JSON value (parsed anew) still replays.
        replay = self.engine.signal(iid, "approved", json.loads('{"v": 1}'), event_id="sig-1")
        self.assertEqual(replay["state"]["index"], 1)

    def test_signal_event_id_scoped_per_instance_and_independent_of_events(self) -> None:
        a, b = self._start(), self._start()
        ra = self.engine.signal(a, "approved", event_id="shared")
        rb = self.engine.signal(b, "approved", event_id="shared")
        self.assertEqual(ra["id"], a)
        self.assertEqual(rb["id"], b)
        # The same id string may back one event and one signal on one instance.
        self.engine.advance(a, "succeeded", event_id="same-id")
        self.engine.signal(a, "shipped", event_id="same-id")
        counts = {
            table: self.engine._db.execute(f"SELECT COUNT(*) FROM {table} WHERE event_id = ?",
                                          ("same-id",)).fetchone()[0]
            for table in ("instance_events", "instance_signals")
        }
        self.assertEqual(counts, {"instance_events": 1, "instance_signals": 1})

    def test_signal_replay_after_completion_returns_historical_response(self) -> None:
        iid = self._start()
        first = self.engine.signal(iid, "approved", event_id="sig-0")
        self.engine.advance(iid, "succeeded")
        self.engine.signal(iid, "shipped", event_id="sig-2")
        replay = self.engine.signal(iid, "approved", event_id="sig-0")
        self.assertEqual(replay, first)
        self.assertEqual(replay["state"]["index"], 1)
        self.assertEqual(self.engine.get(iid)["state"]["status"], "completed")

    def test_signal_without_event_id_writes_nothing(self) -> None:
        iid = self._start()
        self.engine.signal(iid, "approved")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved")  # no longer waiting
        rows = self.engine._db.execute("SELECT COUNT(*) FROM instance_signals").fetchone()[0]
        self.assertEqual(rows, 0)

    def test_concurrent_same_signal_id_one_writer_one_replay(self) -> None:
        iid = self._start()
        barrier = threading.Barrier(2)
        results: list[dict[str, Any]] = []
        errors: list[Exception] = []

        def submit() -> None:
            try:
                barrier.wait()
                results.append(self.engine.signal(iid, "approved", event_id="race-1"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=submit) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.engine.get(iid)["state"]["completed"], ["ask"])
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_signals").fetchone()[0], 1
        )


class SignalPersistenceTests(unittest.TestCase):
    """Waiting state and signal ledger survive reopen; legacy files stay readable."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_waiting_state_and_replay_persist_across_reopen(self) -> None:
        engine = Engine(self.path)
        engine.define("approval", _await_workflow())
        iid = engine.start("approval", {})["id"]
        engine.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            self.assertEqual(engine.get(iid)["state"]["waitingFor"], "approved")
            with self.assertRaises(InvalidTransition):
                engine.advance(iid, "succeeded")
            first = engine.signal(iid, "approved", {"by": "boss"}, event_id="durable-sig")
        finally:
            engine.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            # The waiting state and the saved signal response both survived the reopen.
            self.assertEqual(engine.get(iid)["state"]["index"], 1)
            replayed = engine.signal(iid, "approved", {"by": "boss"}, event_id="durable-sig")
            self.assertEqual(replayed, first)
            # Different payload for the saved signal id is still a conflict.
            with self.assertRaises(InvalidTransition):
                engine.signal(iid, "approved", {"by": "other"}, event_id="durable-sig")
            # Advance past the plain "do" step, then wait on "ship".
            engine.advance(iid, "succeeded")
            self.assertEqual(engine.get(iid)["state"]["waitingFor"], "shipped")
            done = engine.signal(iid, "shipped", event_id="durable-sig-2")
            self.assertEqual(done["state"]["status"], "completed")
            self.assertIsNone(done["state"]["waitingFor"])
        finally:
            engine.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            # After another reopen the completed instance stays terminal, while the
            # historical signal still replays with its original response.
            self.assertEqual(engine.get(iid)["state"]["status"], "completed")
            replayed = engine.signal(iid, "approved", {"by": "boss"}, event_id="durable-sig")
            self.assertEqual(replayed, first)
        finally:
            engine.close()

    def test_legacy_sqlite_file_without_waiting_field_or_signal_table(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(WORKFLOWS["order"], {})
        state = apply_outcome(WORKFLOWS["order"], state, "succeeded")
        assert "waitingFor" in state
        del state["waitingFor"]
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            current = engine.get("legacy-id")["state"]
            self.assertIsNone(current["waitingFor"])
            # Original event flow keeps working.
            result = engine.advance("legacy-id", "failed", event_id="ev-1")
            self.assertEqual(result["state"]["status"], "compensated")
            # A non-waiting instance refuses signals with 409 and writes nothing.
            with self.assertRaises(InvalidTransition):
                engine.signal("legacy-id", "whatever", event_id="sig-1")
            self.assertEqual(
                engine._db.execute("SELECT COUNT(*) FROM instance_signals").fetchone()[0], 0
            )
        finally:
            engine.close()


class AwaitHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_put_rejects_bad_await_and_preserves_existing_workflow(self) -> None:
        status, _ = self.call("PUT", "/v1/workflows/aw",
                              {"steps": [{"name": "a", "await": {"event": "go"}}]})
        self.assertEqual(status, 200)
        for bad in (None, {}, {"x": "go"}, {"event": ""}, {"event": 3}, {"event": "go", "x": 1}):
            status, body = self.call("PUT", "/v1/workflows/aw",
                                     {"steps": [{"name": "a", "await": bad}]})
            self.assertEqual(status, 400, bad)
            self.assertEqual(body["error"]["code"], "invalid_request")
        status, body = self.call("GET", "/v1/workflows")
        self.assertIn("aw", body["workflows"])

    def test_full_wait_signal_flow_end_to_end(self) -> None:
        self.call("PUT", "/v1/workflows/awflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "approved"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body = self.call("POST", "/v1/workflows/awflow/instances", {"context": {"k": "v"}})
        self.assertEqual(status, 201)
        iid = body["id"]
        self.assertEqual((body["state"]["step"], body["state"]["waitingFor"]), ("s1", "approved"))
        # Events are blocked while waiting.
        status, err = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        self.assertEqual((status, err["error"]["code"]), (409, "invalid_transition"))
        # Mismatched and malformed signals.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/signals",
                                   {"event": "denied"})[0], 409)
        for bad_body in (None, {}, {"detail": 1}, {"event": "approved", "bogus": 1},
                         {"event": ""}, {"event": 5}, {"event": "approved", "eventId": ""}):
            status, body = self.call("POST", f"/v1/instances/{iid}/signals", bad_body)
            self.assertEqual(status, 400, bad_body)
        self.assertEqual(self.call("POST", "/v1/instances/missing/signals",
                                   {"event": "approved"})[0], 404)
        # Matching signal with eventId; replay is verbatim, conflict is 409.
        status, first = self.call("POST", f"/v1/instances/{iid}/signals",
                                  {"event": "approved", "detail": {"by": "boss"}, "eventId": "sig-1"})
        self.assertEqual(status, 200)
        self.assertEqual(first["state"]["completed"], ["s1"])
        self.assertIsNone(first["state"]["waitingFor"])
        status, replay = self.call("POST", f"/v1/instances/{iid}/signals",
                                   {"event": "approved", "detail": {"by": "boss"}, "eventId": "sig-1"})
        self.assertEqual((status, replay), (200, first))
        status, body = self.call("POST", f"/v1/instances/{iid}/signals",
                                 {"event": "approved", "detail": {"by": "intern"}, "eventId": "sig-1"})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        # Now unblocked: the plain final step accepts a normal succeeded event and completes.
        status, body = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        self.assertEqual(status, 200)
        self.assertEqual(body["state"]["status"], "completed")
        self.assertIsNone(body["state"]["step"])
        self.assertIsNone(body["state"]["waitingFor"])
        # Signals against a terminal instance are 409, but a historical signal replays 200.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/signals",
                                   {"event": "approved"})[0], 409)
        status, replayed = self.call("POST", f"/v1/instances/{iid}/signals",
                                     {"event": "approved", "detail": {"by": "boss"}, "eventId": "sig-1"})
        self.assertEqual((status, replayed), (200, first))


class AuditEngineTests(unittest.TestCase):
    """Engine-level coverage of the per-instance audit trail."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "order") -> str:
        return self.engine.start(workflow, {})["id"]

    def test_new_instance_has_empty_history(self) -> None:
        iid = self._start()
        audit = self.engine.audit(iid)
        self.assertEqual(audit["id"], iid)
        self.assertEqual(audit["workflow"], "order")
        self.assertEqual(audit["state"]["status"], "running")
        self.assertEqual(audit["history"], [])

    def test_missing_instance_audit_raises_not_found(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.audit("no-such-instance")

    def test_accepted_events_recorded_in_seq_order(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", event_id="e-1")
        second = self.engine.advance(iid, "succeeded", {"note": "d"}, event_id="e-2")
        third = self.engine.advance(iid, "succeeded")  # anonymous call
        audit = self.engine.audit(iid)
        self.assertEqual(audit["state"]["status"], "completed")
        history = audit["history"]
        self.assertEqual([h["seq"] for h in history], [1, 2, 3])
        self.assertEqual([h["kind"] for h in history], ["event", "event", "event"])
        self.assertEqual([h["eventId"] for h in history], ["e-1", "e-2", None])
        self.assertEqual(history[0]["request"],
                         {"eventId": "e-1", "outcome": "succeeded", "detail": None})
        self.assertEqual(history[1]["request"],
                         {"eventId": "e-2", "outcome": "succeeded", "detail": {"note": "d"}})
        self.assertEqual(history[2]["request"],
                         {"eventId": None, "outcome": "succeeded", "detail": None})
        # Each record keeps the full response returned at the time.
        self.assertEqual(history[0]["response"], first)
        self.assertEqual(history[1]["response"], second)
        self.assertEqual(history[2]["response"], third)

    def test_signals_and_events_share_one_sequence_distinguished_by_kind(self) -> None:
        iid = self._start("approval")
        sig = self.engine.signal(iid, "approved", {"by": "boss"}, event_id="shared-id")
        evt = self.engine.advance(iid, "succeeded", event_id="shared-id")  # same id, other kind
        done = self.engine.signal(iid, "shipped")
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["seq"] for h in history], [1, 2, 3])
        self.assertEqual([h["kind"] for h in history], ["signal", "event", "signal"])
        self.assertEqual([h["eventId"] for h in history], ["shared-id", "shared-id", None])
        self.assertEqual(history[0]["request"],
                         {"eventId": "shared-id", "event": "approved", "detail": {"by": "boss"}})
        self.assertEqual(history[0]["response"], sig)
        self.assertEqual(history[1]["response"], evt)
        self.assertEqual(history[2]["response"], done)
        self.assertEqual(history[2]["request"],
                         {"eventId": None, "event": "shipped", "detail": None})

    def test_omitted_and_null_detail_both_recorded_as_null(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-1")          # detail omitted
        self.engine.advance(iid, "succeeded", None, event_id="e-2")    # explicit null
        history = self.engine.audit(iid)["history"]
        self.assertIsNone(history[0]["request"]["detail"])
        self.assertIsNone(history[1]["request"]["detail"])

    def test_rejected_calls_append_nothing(self) -> None:
        iid = self._start("approval")
        rejections = [
            lambda: self.engine.advance(iid, "bogus", event_id="r-1"),       # invalid outcome
            lambda: self.engine.advance(iid, "succeeded", event_id="r-2"),   # waiting for signal
            lambda: self.engine.advance(iid, "succeeded", event_id=""),      # bad eventId
            lambda: self.engine.signal(iid, "rejected", event_id="r-3"),     # mismatched signal
            lambda: self.engine.signal("missing", "approved", event_id="r-4"),
            lambda: self.engine.advance("missing", "succeeded", event_id="r-5"),
        ]
        for reject in rejections:
            with self.assertRaises(SagaError):
                reject()
        self.assertEqual(self.engine.audit(iid)["history"], [])
        # A conflicting resubmission of an accepted eventId is also not audited.
        self.engine.signal(iid, "approved", event_id="ok-1")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", {"different": 1}, event_id="ok-1")
        history = self.engine.audit(iid)["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["eventId"], "ok-1")

    def test_terminal_advance_attempts_append_nothing(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "failed", event_id="fin")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="after-end")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "anything", event_id="after-end-2")
        history = self.engine.audit(iid)["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["eventId"], "fin")
        self.assertEqual(history[0]["response"]["state"]["status"], "compensated")

    def test_replay_returns_first_response_and_appends_nothing(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.engine.advance(iid, "succeeded", event_id="e-2")
        self.engine.advance(iid, "succeeded", event_id="e-3")  # terminal
        replay = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.assertEqual(replay, first)
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["seq"] for h in history], [1, 2, 3])
        self.assertEqual([h["eventId"] for h in history], ["e-1", "e-2", "e-3"])
        # The replayed response is exactly what the first audit record stored.
        self.assertEqual(history[0]["response"], replay)

    def test_concurrent_distinct_ids_get_contiguous_seqs(self) -> None:
        iid = self._start()
        barrier = threading.Barrier(3)
        errors: list[Exception] = []

        def submit(index: int) -> None:
            try:
                barrier.wait()
                self.engine.advance(iid, "succeeded", event_id=f"ev-{index}")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["seq"] for h in history], [1, 2, 3])
        self.assertEqual(sorted(h["eventId"] for h in history), ["ev-0", "ev-1", "ev-2"])
        # Responses recorded in audit order reflect a valid serial progression.
        self.assertEqual(history[-1]["response"]["state"]["status"], "completed")


class AuditPersistenceTests(unittest.TestCase):
    """Audit history survives reopen; pre-audit files stay readable without backfill."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_history_and_state_consistent_across_reopen(self) -> None:
        engine = Engine(self.path)
        engine.define("approval", _await_workflow())
        iid = engine.start("approval", {})["id"]
        first = engine.signal(iid, "approved", event_id="s-1")
        second = engine.advance(iid, "succeeded", event_id="e-1")
        engine.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            audit = engine.audit(iid)
            self.assertEqual(audit["state"]["waitingFor"], "shipped")
            history = audit["history"]
            self.assertEqual([h["seq"] for h in history], [1, 2])
            self.assertEqual([h["kind"] for h in history], ["signal", "event"])
            self.assertEqual(history[0]["response"], first)
            self.assertEqual(history[1]["response"], second)
            # New accepted calls continue the sequence after reopen.
            third = engine.signal(iid, "shipped", event_id="s-2")
            history = engine.audit(iid)["history"]
            self.assertEqual([h["seq"] for h in history], [1, 2, 3])
            self.assertEqual(history[2]["response"], third)
            self.assertEqual(engine.audit(iid)["state"]["status"], "completed")
        finally:
            engine.close()

    def test_legacy_file_without_audit_table_works_and_is_not_backfilled(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(WORKFLOWS["order"], {})
        state = apply_outcome(WORKFLOWS["order"], state, "succeeded")
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            # Pre-upgrade advances are not fabricated: history starts empty even
            # though the instance is already mid-flight.
            audit = engine.audit("legacy-id")
            self.assertEqual(audit["state"]["completed"], ["reserve-stock"])
            self.assertEqual(audit["history"], [])
            # Existing reads and idempotent replay keep working.
            result = engine.advance("legacy-id", "succeeded", event_id="new-era-1")
            self.assertEqual(engine.advance("legacy-id", "succeeded", event_id="new-era-1"), result)
            # History accumulates from the first accepted call after the upgrade.
            history = engine.audit("legacy-id")["history"]
            self.assertEqual([h["seq"] for h in history], [1])
            self.assertEqual(history[0]["eventId"], "new-era-1")
            self.assertEqual(history[0]["response"], result)
        finally:
            engine.close()


class AuditHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_audit_endpoint_end_to_end(self) -> None:
        self.call("PUT", "/v1/workflows/audited", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body = self.call("POST", "/v1/workflows/audited/instances", {"context": {"k": "v"}})
        iid = body["id"]
        # A rejected call and a replay must not appear in the history.
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        status, sig = self.call("POST", f"/v1/instances/{iid}/signals",
                                {"event": "go", "eventId": "sig-1"})
        self.assertEqual(status, 200)
        self.call("POST", f"/v1/instances/{iid}/signals", {"event": "go", "eventId": "sig-1"})
        status, evt = self.call("POST", f"/v1/instances/{iid}/events",
                                {"outcome": "succeeded", "detail": None})
        self.assertEqual(status, 200)

        status, audit = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(status, 200)
        self.assertEqual(audit["id"], iid)
        self.assertEqual(audit["workflow"], "audited")
        self.assertEqual(audit["state"]["status"], "completed")
        history = audit["history"]
        self.assertEqual([h["seq"] for h in history], [1, 2])
        self.assertEqual([h["kind"] for h in history], ["signal", "event"])
        self.assertEqual(history[0]["eventId"], "sig-1")
        self.assertEqual(history[0]["request"],
                         {"eventId": "sig-1", "event": "go", "detail": None})
        self.assertEqual(history[0]["response"], sig)
        self.assertIsNone(history[1]["eventId"])
        self.assertEqual(history[1]["request"],
                         {"eventId": None, "outcome": "succeeded", "detail": None})
        self.assertEqual(history[1]["response"], evt)

    def test_audit_unknown_instance_is_404(self) -> None:
        status, body = self.call("GET", "/v1/instances/missing/audit")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")


class AwaitTimeoutDefinitionTests(unittest.TestCase):
    """Validation of the optional timeoutMs field on a step's await object."""

    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def test_valid_timeout_boundaries_accepted_and_round_trip(self) -> None:
        for timeout in (1, 60_000, 86_400_000):
            workflow = self.engine.define(
                f"w-{timeout}", {"steps": [{"name": "a", "await": {"event": "go", "timeoutMs": timeout}}]}
            )
            self.assertEqual(workflow["steps"][0]["await"], {"event": "go", "timeoutMs": timeout})

    def test_await_without_timeout_keeps_baseline_shape(self) -> None:
        workflow = self.engine.define("plain", {"steps": [{"name": "a", "await": {"event": "go"}}]})
        self.assertEqual(workflow["steps"][0]["await"], {"event": "go"})

    def test_invalid_timeout_shapes_are_400_and_keep_existing_definition(self) -> None:
        self.engine.define("w", {"steps": [{"name": "a", "await": {"event": "go", "timeoutMs": 10}}]})
        bad_awaits = [
            {"event": "go", "timeoutMs": True},      # boolean
            {"event": "go", "timeoutMs": False},     # boolean
            {"event": "go", "timeoutMs": 1.5},       # non-integer
            {"event": "go", "timeoutMs": "100"},     # non-integer
            {"event": "go", "timeoutMs": None},      # null
            {"event": "go", "timeoutMs": 0},         # below range
            {"event": "go", "timeoutMs": -1},        # below range
            {"event": "go", "timeoutMs": 86_400_001},  # above range
            {"event": "go", "timeoutMs": 10, "x": 1},  # unknown field
            {"timeoutMs": 10},                       # missing event
        ]
        for bad in bad_awaits:
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.define("w", {"steps": [{"name": "a", "await": bad}]})
        self.assertEqual(
            self.engine.workflow("w")["steps"][0]["await"], {"event": "go", "timeoutMs": 10}
        )


def _timeout_workflow() -> dict:
    return {
        "steps": [
            {"name": "ask", "compensation": "cancel-ask",
             "await": {"event": "approved", "timeoutMs": 5000}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "wait-forever", "await": {"event": "never"}},
        ]
    }


class TimeoutTransitionTests(unittest.TestCase):
    """Pure state-machine transitions for wait deadlines and the timed_out outcome."""

    def setUp(self) -> None:
        self.workflow = _timeout_workflow()

    def test_waiting_step_with_timeout_sets_deadline(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        self.assertEqual(state["waitingFor"], "approved")
        self.assertEqual(state["deadlineAt"], 6000)
        self.assertEqual(state["status"], "running")

    def test_wait_without_timeout_and_plain_step_have_null_deadline(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        state = apply_signal(self.workflow, state, "approved", now_ms=2000)
        self.assertEqual((state["step"], state["waitingFor"], state["deadlineAt"]), ("do", None, None))
        state = apply_outcome(self.workflow, state, "succeeded", now_ms=3000)
        self.assertEqual(state["waitingFor"], "never")
        self.assertIsNone(state["deadlineAt"])

    def test_timed_out_before_deadline_is_409(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "timed_out", now_ms=5999)
        # State is untouched by the rejected call.
        self.assertEqual(state["deadlineAt"], 6000)

    def test_timed_out_at_and_after_deadline_dead_letters(self) -> None:
        for now in (6000, 7000):
            state = initial_state(self.workflow, {}, now_ms=1000)
            state = apply_outcome(self.workflow, state, "timed_out", {"why": "late"}, now_ms=now)
            self.assertEqual(state["status"], "dead_lettered")
            # Position and progress are preserved exactly as they were.
            self.assertEqual((state["step"], state["index"]), ("ask", 0))
            self.assertEqual(state["completed"], [])
            self.assertEqual(state["compensated"], [])
            self.assertEqual(
                state["failure"], {"step": "ask", "reason": "timeout", "detail": {"why": "late"}}
            )
            self.assertIsNone(state["waitingFor"])
            self.assertIsNone(state["deadlineAt"])

    def test_timed_out_rejected_on_plain_step_and_on_wait_without_timeout(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        state = apply_signal(self.workflow, state, "approved", now_ms=2000)
        with self.assertRaises(InvalidTransition):  # plain step is not waiting
            apply_outcome(self.workflow, state, "timed_out", now_ms=10_000)
        state = apply_outcome(self.workflow, state, "succeeded", now_ms=3000)
        with self.assertRaises(InvalidTransition):  # waiting, but no timeoutMs
            apply_outcome(self.workflow, state, "timed_out", now_ms=10**15)

    def test_signal_before_deadline_takes_success_path_and_clears_deadline(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        state = apply_signal(self.workflow, state, "approved", {"by": "boss"}, now_ms=5999)
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["completed"], ["ask"])
        self.assertIsNone(state["waitingFor"])
        self.assertIsNone(state["deadlineAt"])
        # The wait is over; a late timed_out no longer applies.
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "timed_out", now_ms=10**15)

    def test_dead_lettered_is_terminal_for_events_signals_and_timeouts(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        state = apply_outcome(self.workflow, state, "timed_out", now_ms=6000)
        self.assertEqual(state["status"], "dead_lettered")
        for outcome in ("succeeded", "failed", "timed_out"):
            with self.assertRaises(InvalidTransition, msg=outcome):
                apply_outcome(self.workflow, state, outcome, now_ms=10**15)
        with self.assertRaises(InvalidTransition):
            apply_signal(self.workflow, state, "approved", now_ms=10**15)

    def test_legacy_state_without_deadline_at_reads_as_null(self) -> None:
        state = initial_state(self.workflow, {}, now_ms=1000)
        del state["deadlineAt"]  # simulate a pre-timeout persisted instance
        # A waiting legacy instance has no deadline: timed_out is refused, signals work.
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.workflow, state, "timed_out", now_ms=10**15)
        state = apply_signal(self.workflow, state, "approved", now_ms=2000)
        self.assertEqual(state["completed"], ["ask"])


class TimeoutEngineTests(unittest.TestCase):
    """Ledger, audit and replay behavior of the timed_out outcome."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("expiring", {"steps": [
            {"name": "ask", "compensation": "cancel-ask",
             "await": {"event": "approved", "timeoutMs": 1}},
            {"name": "do", "compensation": "undo-do"},
        ]})
        self.engine.define("patient", {"steps": [
            {"name": "ask", "await": {"event": "approved", "timeoutMs": 86_400_000}},
            {"name": "do"},
        ]})
        self.engine.define("timeless", {"steps": [
            {"name": "ask", "await": {"event": "approved"}},
        ]})

    def tearDown(self) -> None:
        self.engine.close()

    def _expired(self) -> str:
        iid = self.engine.start("expiring", {})["id"]
        time.sleep(0.02)  # let the 1ms deadline pass
        return iid

    def test_timed_out_dead_letters_and_is_audited(self) -> None:
        iid = self._expired()
        result = self.engine.advance(iid, "timed_out", {"why": "late"}, event_id="t-1")
        state = result["state"]
        self.assertEqual(state["status"], "dead_lettered")
        self.assertEqual((state["step"], state["index"]), ("ask", 0))
        self.assertEqual(state["failure"],
                         {"step": "ask", "reason": "timeout", "detail": {"why": "late"}})
        self.assertIsNone(state["waitingFor"])
        self.assertIsNone(state["deadlineAt"])
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["seq"] for h in history], [1])
        self.assertEqual(history[0]["kind"], "event")
        self.assertEqual(history[0]["eventId"], "t-1")
        self.assertEqual(history[0]["request"],
                         {"eventId": "t-1", "outcome": "timed_out", "detail": {"why": "late"}})
        self.assertEqual(history[0]["response"], result)

    def test_timed_out_replay_returns_first_response_without_new_audit(self) -> None:
        iid = self._expired()
        first = self.engine.advance(iid, "timed_out", {"v": 1}, event_id="t-1")
        replay = self.engine.advance(iid, "timed_out", {"v": 1}, event_id="t-1")
        self.assertEqual(replay, first)
        # Omitted detail equals explicit null detail.
        iid2 = self._expired()
        second = self.engine.advance(iid2, "timed_out", None, event_id="t-2")
        self.assertEqual(self.engine.advance(iid2, "timed_out", event_id="t-2"), second)
        # No state change, no extra audit rows from replays.
        self.assertEqual(self.engine.get(iid)["state"]["status"], "dead_lettered")
        self.assertEqual(len(self.engine.audit(iid)["history"]), 1)
        self.assertEqual(
            self.engine._db.execute(
                "SELECT COUNT(*) FROM instance_events WHERE instance_id = ?", (iid,)
            ).fetchone()[0],
            1,
        )

    def test_same_event_id_with_different_payload_is_409_and_writes_nothing(self) -> None:
        iid = self._expired()
        self.engine.advance(iid, "timed_out", {"v": 1}, event_id="t-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "timed_out", {"v": 2}, event_id="t-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", {"v": 1}, event_id="t-1")
        rows = self.engine._db.execute(
            "SELECT COUNT(*) FROM instance_events WHERE instance_id = ?", (iid,)
        ).fetchone()[0]
        self.assertEqual(rows, 1)
        self.assertEqual(len(self.engine.audit(iid)["history"]), 1)

    def test_rejected_timed_out_calls_write_no_ledger_or_audit(self) -> None:
        patient = self.engine.start("patient", {})["id"]      # deadline far in the future
        timeless = self.engine.start("timeless", {})["id"]    # wait without timeoutMs
        plain = self.engine.start("order", {})["id"]          # not waiting at all
        with self.assertRaises(InvalidTransition):
            self.engine.advance(patient, "timed_out", event_id="r-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(timeless, "timed_out", event_id="r-2")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(plain, "timed_out", event_id="r-3")
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("missing", "timed_out", event_id="r-4")
        with self.assertRaises(InvalidRequest):
            self.engine.advance(patient, "bogus", event_id="r-5")
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0], 0
        )
        for iid in (patient, timeless, plain):
            self.assertEqual(self.engine.audit(iid)["history"], [])
            self.assertEqual(self.engine.get(iid)["state"]["status"], "running")

    def test_timed_out_on_terminal_instance_is_409(self) -> None:
        iid = self._expired()
        self.engine.advance(iid, "timed_out", event_id="t-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "timed_out", event_id="t-2")
        self.assertEqual(len(self.engine.audit(iid)["history"]), 1)

    def test_anonymous_timed_out_audited_without_ledger_row(self) -> None:
        iid = self._expired()
        result = self.engine.advance(iid, "timed_out")
        self.assertEqual(result["state"]["status"], "dead_lettered")
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0], 0
        )
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["seq"] for h in history], [1])
        self.assertEqual(history[0]["request"],
                         {"eventId": None, "outcome": "timed_out", "detail": None})

    def test_signal_before_deadline_still_succeeds_via_engine(self) -> None:
        iid = self.engine.start("patient", {})["id"]
        deadline = self.engine.get(iid)["state"]["deadlineAt"]
        self.assertIsInstance(deadline, int)
        result = self.engine.signal(iid, "approved")
        self.assertEqual(result["state"]["completed"], ["ask"])
        self.assertIsNone(result["state"]["deadlineAt"])
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "timed_out")


class TimeoutPersistenceTests(unittest.TestCase):
    """deadlineAt, dead_lettered state, failure and replay survive reopen."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    @staticmethod
    def _define(engine: Engine) -> None:
        engine.define("expiring", {"steps": [
            {"name": "ask", "await": {"event": "approved", "timeoutMs": 1}},
            {"name": "do"},
        ]})
        engine.define("patient", {"steps": [
            {"name": "ask", "await": {"event": "approved", "timeoutMs": 86_400_000}},
        ]})

    def test_deadline_and_dead_letter_replay_consistent_across_reopen(self) -> None:
        engine = Engine(self.path)
        self._define(engine)
        waiting = engine.start("patient", {})["id"]
        deadline = engine.get(waiting)["state"]["deadlineAt"]
        expiring = engine.start("expiring", {})["id"]
        time.sleep(0.02)
        first = engine.advance(expiring, "timed_out", {"why": "late"}, event_id="t-1")
        engine.close()

        engine = Engine(self.path)
        try:
            self._define(engine)
            # The waiting instance kept its exact deadline.
            self.assertEqual(engine.get(waiting)["state"]["deadlineAt"], deadline)
            with self.assertRaises(InvalidTransition):
                engine.advance(waiting, "timed_out", event_id="t-9")
            # The dead-lettered instance kept status, position and failure...
            state = engine.get(expiring)["state"]
            self.assertEqual(state["status"], "dead_lettered")
            self.assertEqual((state["step"], state["index"]), ("ask", 0))
            self.assertEqual(state["failure"],
                             {"step": "ask", "reason": "timeout", "detail": {"why": "late"}})
            # ...and the timed_out event replays the first response verbatim.
            self.assertEqual(engine.advance(expiring, "timed_out", {"why": "late"}, event_id="t-1"),
                             first)
            with self.assertRaises(InvalidTransition):
                engine.advance(expiring, "timed_out", {"why": "other"}, event_id="t-1")
            # Audit history survived too.
            history = engine.audit(expiring)["history"]
            self.assertEqual([h["seq"] for h in history], [1])
            self.assertEqual(history[0]["request"]["outcome"], "timed_out")
            self.assertEqual(history[0]["response"], first)
        finally:
            engine.close()

    def test_legacy_waiting_state_without_deadline_at(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(_await_workflow(), {})
        assert state["waitingFor"] == "approved"
        del state["deadlineAt"]
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "approval", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            current = engine.get("legacy-id")["state"]
            self.assertEqual(current["waitingFor"], "approved")
            self.assertIsNone(current["deadlineAt"])
            # No deadline recorded: timed_out is refused, the signal path still works.
            with self.assertRaises(InvalidTransition):
                engine.advance("legacy-id", "timed_out", event_id="t-1")
            result = engine.signal("legacy-id", "approved", event_id="s-1")
            self.assertEqual(result["state"]["completed"], ["ask"])
            self.assertIsNone(result["state"]["deadlineAt"])
        finally:
            engine.close()


class TimeoutHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_put_rejects_bad_timeout_ms(self) -> None:
        status, _ = self.call("PUT", "/v1/workflows/tw",
                              {"steps": [{"name": "a", "await": {"event": "go", "timeoutMs": 100}}]})
        self.assertEqual(status, 200)
        for bad in ({"event": "go", "timeoutMs": True}, {"event": "go", "timeoutMs": 1.5},
                    {"event": "go", "timeoutMs": 0}, {"event": "go", "timeoutMs": 86_400_001},
                    {"event": "go", "timeoutMs": "100"}, {"timeoutMs": 100},
                    {"event": "go", "timeoutMs": 100, "x": 1}):
            status, body = self.call("PUT", "/v1/workflows/tw", {"steps": [{"name": "a", "await": bad}]})
            self.assertEqual(status, 400, bad)
            self.assertEqual(body["error"]["code"], "invalid_request")
        status, body = self.call("PUT", "/v1/workflows/tw",
                                 {"steps": [{"name": "a", "await": {"event": "go", "timeoutMs": 100}}]})
        self.assertEqual(status, 200)
        self.assertEqual(body["steps"][0]["await"], {"event": "go", "timeoutMs": 100})

    def test_timed_out_end_to_end(self) -> None:
        self.call("PUT", "/v1/workflows/tflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go", "timeoutMs": 1}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body = self.call("POST", "/v1/workflows/tflow/instances", {})
        self.assertEqual(status, 201)
        iid = body["id"]
        self.assertEqual(body["state"]["waitingFor"], "go")
        self.assertIsInstance(body["state"]["deadlineAt"], int)
        time.sleep(0.02)
        # succeeded/failed are still blocked while waiting; timed_out is accepted.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded"})[0], 409)
        status, first = self.call("POST", f"/v1/instances/{iid}/events",
                                  {"outcome": "timed_out", "detail": {"why": "late"}, "eventId": "t-1"})
        self.assertEqual(status, 200)
        state = first["state"]
        self.assertEqual(state["status"], "dead_lettered")
        self.assertEqual((state["step"], state["index"]), ("s1", 0))
        self.assertEqual(state["completed"], [])
        self.assertEqual(state["compensated"], [])
        self.assertEqual(state["failure"],
                         {"step": "s1", "reason": "timeout", "detail": {"why": "late"}})
        self.assertIsNone(state["waitingFor"])
        self.assertIsNone(state["deadlineAt"])
        # Idempotent replay and conflict.
        status, replay = self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "timed_out", "detail": {"why": "late"}, "eventId": "t-1"})
        self.assertEqual((status, replay), (200, first))
        status, body = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "timed_out", "detail": {"why": "x"}, "eventId": "t-1"})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        # Terminal for everything else.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded"})[0], 409)
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/signals", {"event": "go"})[0], 409)
        # Audit recorded exactly the accepted timed_out call.
        status, audit = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(status, 200)
        self.assertEqual([h["seq"] for h in audit["history"]], [1])
        self.assertEqual(audit["history"][0]["kind"], "event")
        self.assertEqual(audit["history"][0]["request"],
                         {"eventId": "t-1", "outcome": "timed_out", "detail": {"why": "late"}})
        self.assertEqual(audit["history"][0]["response"], first)

    def test_timed_out_rejection_statuses(self) -> None:
        self.call("PUT", "/v1/workflows/tlong", {"steps": [
            {"name": "s1", "await": {"event": "go", "timeoutMs": 86_400_000}},
            {"name": "s2", "await": {"event": "later"}},
        ]})
        iid = self.call("POST", "/v1/workflows/tlong/instances", {})[1]["id"]
        # Deadline not reached yet.
        status, body = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "timed_out"})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        # Signal before the deadline takes the success path and clears the deadline.
        status, body = self.call("POST", f"/v1/instances/{iid}/signals", {"event": "go"})
        self.assertEqual(status, 200)
        self.assertIsNone(body["state"]["deadlineAt"])
        # The next wait has no timeoutMs: timed_out is refused there too.
        self.assertEqual(body["state"]["waitingFor"], "later")
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "timed_out"})[0], 409)
        # Unknown instance and illegal outcome.
        self.assertEqual(self.call("POST", "/v1/instances/missing/events",
                                   {"outcome": "timed_out"})[0], 404)
        status, body = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "bogus"})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))


def _versioned_workflow() -> dict:
    return {
        "steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "cancel-ship"},
        ]
    }


class VersionDefinitionTests(unittest.TestCase):
    """PUT appends immutable versions; definitions persist across reopen."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_builtins_seed_version_1_and_puts_append(self) -> None:
        engine = Engine(self.path)
        try:
            self.assertEqual(engine.workflow_info("order")["version"], 1)
            self.assertEqual(engine.workflow_info("order")["steps"], WORKFLOWS["order"]["steps"])
            self.assertEqual(engine.workflow_info("provision")["version"], 1)
            first = engine.define("order", {"steps": [{"name": "only"}]})
            self.assertEqual(first["version"], 2)
            second = engine.define("order", {"steps": [{"name": "x"}, {"name": "y"}]})
            self.assertEqual(second["version"], 3)
            # GET-style read returns the latest; pinned versions stay fetchable.
            self.assertEqual(engine.workflow_info("order")["version"], 3)
            self.assertEqual(engine.workflow("order", 1)["steps"], WORKFLOWS["order"]["steps"])
            self.assertEqual(engine.workflow("order", 2)["steps"], [{"name": "only", "compensation": None}])
            # A fresh name starts at version 1.
            self.assertEqual(engine.define("custom", _versioned_workflow())["version"], 1)
            self.assertIn("custom", engine.workflow_names())
        finally:
            engine.close()

    def test_definitions_survive_reopen(self) -> None:
        engine = Engine(self.path)
        engine.define("custom", _versioned_workflow())
        engine.define("custom", {"steps": [{"name": "ask"}, {"name": "do"}, {"name": "ship"}]})
        engine.close()

        engine = Engine(self.path)
        try:
            info = engine.workflow_info("custom")
            self.assertEqual(info["version"], 2)
            self.assertEqual([s["name"] for s in info["steps"]], ["ask", "do", "ship"])
            self.assertEqual(engine.workflow("custom", 1)["steps"][0]["await"], {"event": "approved"})
            self.assertEqual(engine.workflow_info("order")["version"], 1)
            # The next PUT continues the sequence, it does not restart it.
            self.assertEqual(engine.define("custom", _versioned_workflow())["version"], 3)
        finally:
            engine.close()

    def test_invalid_put_appends_no_version(self) -> None:
        engine = Engine(self.path)
        try:
            engine.define("w", {"steps": [{"name": "a"}]})
            with self.assertRaises(InvalidRequest):
                engine.define("w", {"steps": []})
            self.assertEqual(engine.workflow_info("w")["version"], 1)
        finally:
            engine.close()

    def test_unknown_workflow_and_unknown_version(self) -> None:
        engine = Engine(self.path)
        try:
            with self.assertRaises(NotFound):
                engine.workflow_info("nope")
            with self.assertRaises(NotFound):
                engine.workflow("order", 99)
            with self.assertRaises(InvalidRequest):
                engine.start("nope", {})
            with self.assertRaises(NotFound):
                engine.start("order", {}, version=99)
            for bad in (0, -1, 1.5, "2", True):
                with self.assertRaises(InvalidRequest, msg=repr(bad)):
                    engine.start("order", {}, version=bad)
        finally:
            engine.close()


class VersionPinningTests(unittest.TestCase):
    """Instances pin the version they start on; PUTs do not affect them in flight."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("flow", _versioned_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def test_start_defaults_to_latest_and_response_carries_version(self) -> None:
        started = self.engine.start("flow", {})
        self.assertEqual(started["workflowVersion"], 1)
        self.assertEqual(self.engine.get(started["id"])["workflowVersion"], 1)
        self.assertEqual(self.engine.audit(started["id"])["workflowVersion"], 1)
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}, {"name": "ship"}]})
        started2 = self.engine.start("flow", {})
        self.assertEqual(started2["workflowVersion"], 2)

    def test_start_with_explicit_version_pins_it(self) -> None:
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}, {"name": "ship"}]})
        started = self.engine.start("flow", {}, version=1)
        self.assertEqual(started["workflowVersion"], 1)
        self.assertEqual(started["state"]["waitingFor"], "approved")  # v1 first step awaits

    def test_later_put_does_not_change_in_flight_definition(self) -> None:
        iid = self.engine.start("flow", {})["id"]
        # v2 drops the await on step 0 and renames the compensations of steps 0/1.
        self.engine.define("flow", {"steps": [
            {"name": "ask", "compensation": "cancel-ask-v2"},
            {"name": "do", "compensation": "undo-do-v2"},
            {"name": "ship", "compensation": "abort-ship"},
        ]})
        # The pinned instance still waits on the v1 await.
        self.assertEqual(self.engine.get(iid)["state"]["waitingFor"], "approved")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded")
        self.engine.signal(iid, "approved", event_id="s-1")
        result = self.engine.advance(iid, "failed", event_id="e-1")
        # Compensation names come from v1, not v2.
        self.assertEqual(result["state"]["compensated"], ["undo-do", "cancel-ask"])
        # Responses carry the pinned version, including ledger replays.
        self.assertEqual(result["workflowVersion"], 1)
        replay = self.engine.advance(iid, "failed", event_id="e-1")
        self.assertEqual(replay, result)


class VersionLegacyTests(unittest.TestCase):
    """Instances persisted before versioning read as null and backfill on advance."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(WORKFLOWS["order"], {})
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_null_version_reads_back_and_backfills_on_first_advance(self) -> None:
        engine = Engine(self.path)
        try:
            self.assertIsNone(engine.get("legacy-id")["workflowVersion"])
            self.assertIsNone(engine.audit("legacy-id")["workflowVersion"])
            # A rejected advance does not backfill anything.
            engine.define("order", {"steps": [{"name": "reserve-stock"},
                                              {"name": "charge-payment"},
                                              {"name": "create-shipment"}]})
            result = engine.advance("legacy-id", "succeeded", event_id="e-1")
            self.assertEqual(result["workflowVersion"], 2)  # latest at advance time
            self.assertEqual(engine.get("legacy-id")["workflowVersion"], 2)
            self.assertEqual(engine.audit("legacy-id")["workflowVersion"], 2)
            # And the instance is now pinned: a newer PUT does not move it.
            engine.define("order", {"steps": [{"name": "only"}]})
            self.assertEqual(engine.get("legacy-id")["workflowVersion"], 2)
        finally:
            engine.close()


class MigrateEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("flow", _versioned_workflow())  # v1

    def tearDown(self) -> None:
        self.engine.close()

    def _v2(self) -> None:
        # Same ordered names; steps 0 and 1 identical; only the not-yet-entered
        # step 2 changes (compensation renamed).
        self.engine.define("flow", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "abort-ship"},
        ]})

    def test_migrate_preserves_state_and_switches_definition(self) -> None:
        self._v2()
        iid = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(iid, "approved", event_id="s-1")  # now at index 1 ("do")
        before = self.engine.get(iid)["state"]
        migrated = self.engine.migrate(iid, 2)
        self.assertEqual(migrated["workflowVersion"], 2)
        self.assertEqual(migrated["state"], before)  # every state field carried over
        self.assertEqual(self.engine.get(iid)["workflowVersion"], 2)
        # Advance onto the redefined step, then fail: compensation uses v2 names.
        self.engine.advance(iid, "succeeded", event_id="e-1")
        result = self.engine.advance(iid, "failed", event_id="e-2")
        self.assertEqual(result["workflowVersion"], 2)
        self.assertEqual(result["state"]["compensated"], ["abort-ship", "undo-do", "cancel-ask"])
        # Historical responses keep the version they were recorded with.
        history = self.engine.audit(iid)["history"]
        self.assertEqual(history[0]["response"]["workflowVersion"], 1)
        self.assertEqual([h["seq"] for h in history], [1, 2, 3])  # migrate appended nothing

    def test_migrate_to_same_version_is_a_noop(self) -> None:
        iid = self.engine.start("flow", {})["id"]
        migrated = self.engine.migrate(iid, 1)
        self.assertEqual(migrated["workflowVersion"], 1)

    def test_migrate_rejections(self) -> None:
        self._v2()
        iid = self.engine.start("flow", {}, version=1)["id"]
        # 400: missing / non-positive / non-integer versions.
        for bad in (None, 0, -3, 1.5, "2", True):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.migrate(iid, bad)
        # 404: unknown instance, unknown version.
        with self.assertRaises(InstanceNotFound):
            self.engine.migrate("missing", 2)
        with self.assertRaises(NotFound):
            self.engine.migrate(iid, 99)
        # 409: terminal instance.
        done = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(done, "approved")
        self.engine.advance(done, "succeeded")
        self.engine.advance(done, "succeeded")
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(done, 2)
        # Nothing changed for the rejected attempts.
        self.assertEqual(self.engine.get(iid)["workflowVersion"], 1)
        self.assertEqual(self.engine.audit(iid)["history"], [])

    def test_migrate_incompatible_sequences_are_409(self) -> None:
        iid = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(iid, "approved")  # index 1
        # Different step count.
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}]})
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 2)
        # Same count, renamed step.
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}, {"name": "deliver"}]})
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 3)
        # Entered step (index 0, "ask") changed its await.
        self.engine.define("flow", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "ok"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "cancel-ship"},
        ]})
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 4)
        # Current step (index 1, "do") changed compensation.
        self.engine.define("flow", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do-differently"},
            {"name": "ship", "compensation": "cancel-ship"},
        ]})
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 5)
        self.assertEqual(self.engine.get(iid)["workflowVersion"], 1)

    def test_migrate_then_timeout_uses_target_version(self) -> None:
        self.engine.define("flow", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "cancel-ship",
             "await": {"event": "shipped", "timeoutMs": 1}},
        ]})
        iid = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(iid, "approved")  # index 1 ("do"); ship not yet entered
        self.engine.migrate(iid, 2)
        # Advancing now enters v2's awaiting ship step with its timeout.
        result = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.assertEqual(result["state"]["waitingFor"], "shipped")
        self.assertIsInstance(result["state"]["deadlineAt"], int)
        time.sleep(0.02)
        dead = self.engine.advance(iid, "timed_out", event_id="t-1")
        self.assertEqual(dead["state"]["status"], "dead_lettered")
        self.assertEqual(dead["state"]["failure"]["step"], "ship")


class VersionHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_workflow_get_and_versioned_start(self) -> None:
        status, body = self.call("GET", "/v1/workflows/order")
        self.assertEqual((status, body["version"]), (200, 1))
        self.assertEqual(body["workflow"], "order")
        self.assertEqual(len(body["steps"]), 3)
        self.assertEqual(self.call("GET", "/v1/workflows/nope")[0], 404)
        status, body = self.call("PUT", "/v1/workflows/vflow", {"steps": [{"name": "a"}]})
        self.assertEqual((status, body["version"]), (200, 1))
        status, body = self.call("PUT", "/v1/workflows/vflow", {"steps": [{"name": "b"}]})
        self.assertEqual(body["version"], 2)
        status, body = self.call("GET", "/v1/workflows/vflow")
        self.assertEqual((body["version"], body["steps"][0]["name"]), (2, "b"))
        # Start pinned to v1 vs. latest.
        status, body = self.call("POST", "/v1/workflows/vflow/instances", {"version": 1})
        self.assertEqual(status, 201)
        self.assertEqual((body["workflowVersion"], body["state"]["step"]), (1, "a"))
        status, body = self.call("POST", "/v1/workflows/vflow/instances", {})
        self.assertEqual((body["workflowVersion"], body["state"]["step"]), (2, "b"))
        self.assertEqual(self.call("GET", f"/v1/instances/{body['id']}")[1]["workflowVersion"], 2)
        # Bad version shapes on start.
        self.assertEqual(self.call("POST", "/v1/workflows/vflow/instances", {"version": 0})[0], 400)
        self.assertEqual(self.call("POST", "/v1/workflows/vflow/instances", {"version": 9})[0], 404)
        self.assertEqual(self.call("POST", "/v1/workflows/vflow/instances", {"version": 1, "x": 1})[0], 400)

    def test_migrate_endpoint(self) -> None:
        self.call("PUT", "/v1/workflows/mflow", {"steps": [
            {"name": "s1", "compensation": "c1"},
            {"name": "s2", "compensation": "c2"},
        ]})
        self.call("PUT", "/v1/workflows/mflow", {"steps": [
            {"name": "s1", "compensation": "c1"},
            {"name": "s2", "compensation": "c2-new"},
        ]})
        iid = self.call("POST", "/v1/workflows/mflow/instances", {"version": 1})[1]["id"]
        # Malformed bodies.
        for bad_body, expected in ((None, 400), ({}, 400), ({"version": 0}, 400),
                                   ({"version": "2"}, 400), ({"version": 2, "x": 1}, 400),
                                   ({"version": 99}, 404)):
            status, body = self.call("POST", f"/v1/instances/{iid}/migrate", bad_body)
            self.assertEqual(status, expected, bad_body)
        self.assertEqual(self.call("POST", "/v1/instances/missing/migrate", {"version": 2})[0], 404)
        # Happy path: state untouched, version switched, v2 compensation applies.
        status, body = self.call("POST", f"/v1/instances/{iid}/migrate", {"version": 2})
        self.assertEqual(status, 200)
        self.assertEqual(body["workflowVersion"], 2)
        self.assertEqual((body["state"]["status"], body["state"]["step"]), ("running", "s1"))
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        status, body = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "failed"})
        self.assertEqual(body["state"]["compensated"], ["c2-new", "c1"])
        self.assertEqual(body["workflowVersion"], 2)
        # Terminal instances cannot migrate.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/migrate", {"version": 1})[0], 409)


class InstanceVersionEngineTests(unittest.TestCase):
    """Engine-level coverage of instance versions and If-Match preconditions."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "order") -> str:
        return self.engine.start(workflow, {})["id"]

    def test_new_instance_is_version_1_and_changes_bump_by_one(self) -> None:
        iid = self._start()
        self.assertEqual(self.engine.instance_version(iid), 1)
        self.engine.advance(iid, "succeeded")
        self.assertEqual(self.engine.instance_version(iid), 2)
        self.engine.advance(iid, "succeeded")
        self.assertEqual(self.engine.instance_version(iid), 3)
        self.engine.advance(iid, "succeeded")  # terminal
        self.assertEqual(self.engine.instance_version(iid), 4)

    def test_rejected_calls_do_not_bump_version(self) -> None:
        iid = self._start("approval")
        rejections = [
            lambda: self.engine.advance(iid, "bogus"),          # invalid outcome
            lambda: self.engine.advance(iid, "succeeded"),      # waiting for a signal
            lambda: self.engine.advance(iid, "timed_out"),      # wait has no timeout
            lambda: self.engine.signal(iid, "rejected"),        # mismatched signal name
            lambda: self.engine.advance(iid, "succeeded", event_id=""),  # bad eventId
        ]
        for reject in rejections:
            with self.assertRaises(SagaError):
                reject()
            self.assertEqual(self.engine.instance_version(iid), 1)
        # A real change bumps it exactly once, and terminal-state attempts do not.
        self.engine.signal(iid, "approved")
        self.assertEqual(self.engine.instance_version(iid), 2)
        self.engine.advance(iid, "failed")
        self.assertEqual(self.engine.instance_version(iid), 3)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved")
        self.assertEqual(self.engine.instance_version(iid), 3)

    def test_if_match_gates_events_and_signals(self) -> None:
        iid = self._start("approval")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", if_match=2)  # stale
        self.assertEqual(self.engine.instance_version(iid), 1)
        self.assertEqual(self.engine.get(iid)["state"]["waitingFor"], "approved")
        self.engine.signal(iid, "approved", if_match=1)
        self.assertEqual(self.engine.instance_version(iid), 2)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", if_match=1)  # stale after the bump
        self.engine.advance(iid, "succeeded", if_match=2)
        self.assertEqual(self.engine.instance_version(iid), 3)

    def test_if_match_rejection_writes_no_ledger_or_audit(self) -> None:
        iid = self._start()
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-1", if_match=99)
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0], 0
        )
        self.assertEqual(self.engine.audit(iid)["history"], [])
        self.assertEqual(self.engine.instance_version(iid), 1)

    def test_replay_precedes_if_match_and_neither_replay_nor_conflict_bumps(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", event_id="e-1")  # version 2
        self.engine.advance(iid, "succeeded", event_id="e-2")          # version 3
        # Same eventId, same payload: the first response comes back even though
        # the If-Match version is long stale, and nothing changes.
        replay = self.engine.advance(iid, "succeeded", event_id="e-1", if_match=1)
        self.assertEqual(replay, first)
        self.assertEqual(self.engine.instance_version(iid), 3)
        # Same eventId, different payload: still a conflict, If-Match irrelevant.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", event_id="e-1", if_match=3)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", event_id="e-1", if_match=1)
        self.assertEqual(self.engine.instance_version(iid), 3)
        self.assertEqual(len(self.engine.audit(iid)["history"]), 2)

    def test_migrate_bumps_version_and_honors_if_match(self) -> None:
        self.engine.define("flow", _versioned_workflow())  # v1
        self.engine.define("flow", {"steps": [             # v2: only step 2 differs
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "abort-ship"},
        ]})
        iid = self.engine.start("flow", {}, version=1)["id"]
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 2, if_match=7)
        self.assertEqual(self.engine.instance_version(iid), 1)
        self.assertEqual(self.engine.get(iid)["workflowVersion"], 1)
        before = self.engine.get(iid)["state"]
        migrated = self.engine.migrate(iid, 2, if_match=1)
        self.assertEqual(migrated["state"], before)  # state carried over field by field
        self.assertEqual(self.engine.instance_version(iid), 2)
        # An incompatible migration is a rejection and does not bump the version.
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}]})
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 3, if_match=2)
        self.assertEqual(self.engine.instance_version(iid), 2)

    def test_unknown_instance_still_404_with_if_match(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("missing", "succeeded", if_match=1)
        with self.assertRaises(InstanceNotFound):
            self.engine.signal("missing", "approved", if_match=1)
        with self.assertRaises(InstanceNotFound):
            self.engine.migrate("missing", 1, if_match=1)
        with self.assertRaises(InstanceNotFound):
            self.engine.instance_version("missing")


class InstanceVersionPersistenceTests(unittest.TestCase):
    """Instance versions survive reopen; legacy rows read as version 1."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_version_consistent_across_reopen(self) -> None:
        engine = Engine(self.path)
        iid = engine.start("order", {})["id"]
        engine.advance(iid, "succeeded", event_id="e-1")
        engine.advance(iid, "succeeded", event_id="e-2")
        self.assertEqual(engine.instance_version(iid), 3)
        engine.close()

        engine = Engine(self.path)
        try:
            self.assertEqual(engine.instance_version(iid), 3)
            with self.assertRaises(InvalidTransition):
                engine.advance(iid, "succeeded", event_id="e-3", if_match=2)
            engine.advance(iid, "succeeded", event_id="e-3", if_match=3)
            self.assertEqual(engine.instance_version(iid), 4)
            # Replay after reopen still precedes the precondition.
            replay = engine.advance(iid, "succeeded", event_id="e-1", if_match=1)
            self.assertEqual(replay["state"]["completed"], ["reserve-stock"])
            self.assertEqual(engine.instance_version(iid), 4)
        finally:
            engine.close()

    def test_legacy_instance_reads_as_1_and_bumps_to_2(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(WORKFLOWS["order"], {})
        state = apply_outcome(WORKFLOWS["order"], state, "succeeded")
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            # No fabricated history: the legacy instance reads as version 1 even
            # though it is already mid-flight.
            self.assertEqual(engine.instance_version("legacy-id"), 1)
            with self.assertRaises(InvalidTransition):
                engine.advance("legacy-id", "succeeded", if_match=2)
            result = engine.advance("legacy-id", "succeeded", if_match=1, event_id="e-1")
            self.assertEqual(result["state"]["step"], "create-shipment")
            self.assertEqual(engine.instance_version("legacy-id"), 2)
        finally:
            engine.close()


class InstanceVersionHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body: dict | None = None, headers: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), response.headers.get("ETag")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), error.headers.get("ETag")

    def _start(self, workflow: str = "order") -> str:
        status, body, etag = self.call("POST", f"/v1/workflows/{workflow}/instances", {})
        self.assertEqual((status, etag), (201, '"1"'))
        return body["id"]

    def test_etag_on_start_get_and_audit(self) -> None:
        iid = self._start()
        status, body, etag = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual((status, etag), (200, '"1"'))
        status, body, etag = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual((status, etag), (200, '"1"'))
        # A real change bumps the version reflected by every read.
        status, body, etag = self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        self.assertEqual((status, etag), (200, '"2"'))
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2], '"2"')
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}/audit")[2], '"2"')

    def test_if_match_gates_events_end_to_end(self) -> None:
        iid = self._start()
        # Stale precondition: 409 invalid_transition, and nothing changes.
        status, body, etag = self.call("POST", f"/v1/instances/{iid}/events",
                                       {"outcome": "succeeded"}, headers={"If-Match": '"2"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2], '"1"')
        _, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(audit["history"], [])
        # Matching precondition proceeds and bumps.
        status, body, etag = self.call("POST", f"/v1/instances/{iid}/events",
                                       {"outcome": "succeeded"}, headers={"If-Match": '"1"'})
        self.assertEqual((status, etag), (200, '"2"'))
        # The same precondition is now stale.
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "succeeded"}, headers={"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        status, body, etag = self.call("POST", f"/v1/instances/{iid}/events",
                                       {"outcome": "succeeded"}, headers={"If-Match": '"2"'})
        self.assertEqual((status, etag), (200, '"3"'))

    def test_malformed_if_match_is_400_and_writes_nothing(self) -> None:
        iid = self._start()
        for bad in ("", "*", 'W/"1"', "1", '"1", "2"', '"x"', '""', '"1.5"'):
            status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                        {"outcome": "succeeded"}, headers={"If-Match": bad})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad)
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2], '"1"')
        _, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(audit["history"], [])

    def test_duplicate_if_match_header_lines_are_400(self) -> None:
        import http.client

        iid = self._start()
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.putrequest("POST", f"/v1/instances/{iid}/events")
            connection.putheader("Content-Type", "application/json")
            connection.putheader("If-Match", '"1"')
            connection.putheader("If-Match", '"1"')
            payload = json.dumps({"outcome": "succeeded"}).encode()
            connection.putheader("Content-Length", str(len(payload)))
            connection.endheaders(payload)
            response = connection.getresponse()
            body = json.loads(response.read() or b"{}")
            self.assertEqual((response.status, body["error"]["code"]), (400, "invalid_request"))
        finally:
            connection.close()
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2], '"1"')

    def test_replay_precedes_if_match_over_http(self) -> None:
        iid = self._start()
        status, first, etag = self.call("POST", f"/v1/instances/{iid}/events",
                                        {"outcome": "succeeded", "eventId": "e-1"})
        self.assertEqual((status, etag), (200, '"2"'))
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded", "eventId": "e-2"})
        # Same payload with a stale If-Match: the first response verbatim, and the
        # ETag reflects the CURRENT version, not the one from the first processing.
        status, replay, etag = self.call("POST", f"/v1/instances/{iid}/events",
                                         {"outcome": "succeeded", "eventId": "e-1"},
                                         headers={"If-Match": '"1"'})
        self.assertEqual((status, replay), (200, first))
        self.assertEqual(etag, '"3"')
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2], '"3"')
        # Same eventId with a different payload is still a conflict.
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "failed", "eventId": "e-1"},
                                    headers={"If-Match": '"3"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))

    def test_signals_and_migrate_honor_if_match(self) -> None:
        self.call("PUT", "/v1/workflows/cflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        self.call("PUT", "/v1/workflows/cflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2-new"},
        ]})
        iid = self._start("cflow")
        # Signals.
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/signals",
                                    {"event": "go"}, headers={"If-Match": '"9"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        status, body, etag = self.call("POST", f"/v1/instances/{iid}/signals",
                                       {"event": "go"}, headers={"If-Match": '"1"'})
        self.assertEqual((status, etag), (200, '"2"'))
        # Migrate: matching precondition, state preserved, version bumped.
        status, body, etag = self.call("POST", f"/v1/instances/{iid}/migrate",
                                       {"version": 2}, headers={"If-Match": '"2"'})
        self.assertEqual((status, etag), (200, '"3"'))
        self.assertEqual((body["workflowVersion"], body["state"]["step"]), (2, "s2"))
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/migrate",
                                    {"version": 1}, headers={"If-Match": '"2"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[1]["workflowVersion"], 2)

    def test_unknown_instance_is_404_with_if_match(self) -> None:
        status, body, _ = self.call("POST", "/v1/instances/missing/events",
                                    {"outcome": "succeeded"}, headers={"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        status, body, _ = self.call("POST", "/v1/instances/missing/migrate",
                                    {"version": 1}, headers={"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))


if __name__ == "__main__":
    unittest.main()