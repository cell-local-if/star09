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

from saga import Engine, InstanceNotFound, InvalidRequest, InvalidTransition, NotFound, SagaError, WORKFLOWS, apply_outcome, apply_recover, apply_signal, initial_state, parse_if_match


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


class AuditPagingEngineTests(unittest.TestCase):
    """Engine-level coverage of the paged/filterable audit query."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _mixed_history(self) -> str:
        """Six accepted calls alternating kinds: seq 1,3,5 signals, 2,4,6 events."""
        iid = self.engine.start("approval", {})["id"]
        self.engine.signal(iid, "approved", event_id="s-1")          # seq 1 signal
        self.engine.advance(iid, "succeeded", event_id="e-2")        # seq 2 event
        self.engine.signal(iid, "shipped", event_id="s-3")           # seq 3 signal
        return iid

    def test_no_params_keeps_baseline_shape_without_next_seq(self) -> None:
        iid = self._mixed_history()
        audit = self.engine.audit(iid)
        self.assertEqual([h["seq"] for h in audit["history"]], [1, 2, 3])
        self.assertNotIn("nextSeq", audit)

    def test_limit_pages_with_next_seq_cursor(self) -> None:
        iid = self._mixed_history()
        page1 = self.engine.audit(iid, limit="2")
        self.assertEqual([h["seq"] for h in page1["history"]], [1, 2])
        self.assertEqual(page1["nextSeq"], 2)
        page2 = self.engine.audit(iid, limit="2", after_seq=str(page1["nextSeq"]))
        self.assertEqual([h["seq"] for h in page2["history"]], [3])
        self.assertIsNone(page2["nextSeq"])

    def test_limit_defaults_to_50_when_other_params_given(self) -> None:
        iid = self._mixed_history()
        audit = self.engine.audit(iid, after_seq="0")
        self.assertEqual([h["seq"] for h in audit["history"]], [1, 2, 3])
        self.assertIsNone(audit["nextSeq"])

    def test_kind_filter_selects_records_without_changing_them(self) -> None:
        iid = self._mixed_history()
        signals = self.engine.audit(iid, kind="signal")
        self.assertEqual([h["seq"] for h in signals["history"]], [1, 3])
        self.assertEqual([h["kind"] for h in signals["history"]], ["signal", "signal"])
        self.assertIsNone(signals["nextSeq"])
        full = self.engine.audit(iid)
        # Records are identical to the unpaged ones, only the set is filtered.
        self.assertEqual(signals["history"], [h for h in full["history"] if h["kind"] == "signal"])
        events = self.engine.audit(iid, kind="event")
        self.assertEqual([h["seq"] for h in events["history"]], [2])

    def test_after_seq_cursor_applies_before_kind_filter(self) -> None:
        iid = self._mixed_history()
        # afterSeq=1 skips seq 1 (a signal); the next signal is seq 3.
        page = self.engine.audit(iid, kind="signal", after_seq="1")
        self.assertEqual([h["seq"] for h in page["history"]], [3])
        self.assertIsNone(page["nextSeq"])

    def test_kind_filter_paginates_over_filtered_matches_only(self) -> None:
        iid = self._mixed_history()
        page1 = self.engine.audit(iid, kind="signal", limit="1")
        self.assertEqual([h["seq"] for h in page1["history"]], [1])
        self.assertEqual(page1["nextSeq"], 1)
        page2 = self.engine.audit(iid, kind="signal", limit="1", after_seq="1")
        self.assertEqual([h["seq"] for h in page2["history"]], [3])
        self.assertIsNone(page2["nextSeq"])

    def test_empty_page_returns_empty_history_and_null_next_seq(self) -> None:
        iid = self._mixed_history()
        page = self.engine.audit(iid, after_seq="99")
        self.assertEqual(page["history"], [])
        self.assertIsNone(page["nextSeq"])
        fresh = self.engine.start("order", {})["id"]
        page = self.engine.audit(fresh, limit="10")
        self.assertEqual(page["history"], [])
        self.assertIsNone(page["nextSeq"])

    def test_paged_response_carries_instance_fields_and_revision(self) -> None:
        iid = self._mixed_history()
        page = self.engine.audit(iid, limit="1")
        self.assertEqual(page["id"], iid)
        self.assertEqual(page["workflow"], "approval")
        self.assertEqual(page["workflowVersion"], 1)
        self.assertEqual(page["state"]["status"], "completed")
        self.assertEqual(page.revision, 4)  # start + 3 accepted calls

    def test_invalid_params_raise_before_instance_lookup(self) -> None:
        bad_calls = [
            {"limit": "0"}, {"limit": "101"}, {"limit": "01"}, {"limit": ""},
            {"limit": "1.5"}, {"limit": "true"}, {"limit": "abc"},
            {"after_seq": "-1"}, {"after_seq": "00"}, {"after_seq": "1.0"},
            {"after_seq": ""}, {"after_seq": "x"},
            {"kind": "events"}, {"kind": ""}, {"kind": "EVENT"},
        ]
        for params in bad_calls:
            with self.subTest(params=params):
                with self.assertRaises(InvalidRequest):
                    self.engine.audit("no-such-instance", **params)

    def test_valid_params_on_missing_instance_raise_not_found(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.audit("no-such-instance", limit="10", after_seq="0", kind="event")

    def test_paged_query_is_read_only(self) -> None:
        iid = self._mixed_history()
        before = self.engine.audit(iid)
        self.engine.audit(iid, limit="1", kind="event", after_seq="0")
        after = self.engine.audit(iid)
        self.assertEqual(before, after)
        self.assertEqual(before.revision, after.revision)


class AuditPagingHttpTests(unittest.TestCase):
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

    def get_headers(self, path: str):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", method="GET")
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, dict(response.headers), json.loads(response.read() or b"{}")

    def _instance_with_history(self) -> str:
        self.call("PUT", "/v1/workflows/paged", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body = self.call("POST", "/v1/workflows/paged/instances", {})
        iid = body["id"]
        self.call("POST", f"/v1/instances/{iid}/signals", {"event": "go", "eventId": "sig-1"})
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded", "eventId": "evt-2"})
        return iid

    def test_paged_audit_end_to_end_with_etag(self) -> None:
        iid = self._instance_with_history()
        status, headers, page = self.get_headers(f"/v1/instances/{iid}/audit?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')
        self.assertEqual([h["seq"] for h in page["history"]], [1])
        self.assertEqual(page["nextSeq"], 1)
        status, page2 = self.call("GET", f"/v1/instances/{iid}/audit?limit=1&afterSeq=1&kind=event")
        self.assertEqual(status, 200)
        self.assertEqual([h["seq"] for h in page2["history"]], [2])
        self.assertEqual(page2["history"][0]["kind"], "event")
        self.assertIsNone(page2["nextSeq"])

    def test_unpaged_audit_has_no_pagination_fields(self) -> None:
        iid = self._instance_with_history()
        status, audit = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(status, 200)
        self.assertEqual([h["seq"] for h in audit["history"]], [1, 2])
        self.assertNotIn("nextSeq", audit)

    def test_invalid_query_params_are_400(self) -> None:
        iid = self._instance_with_history()
        bad_paths = [
            f"/v1/instances/{iid}/audit?limit=0",
            f"/v1/instances/{iid}/audit?limit=101",
            f"/v1/instances/{iid}/audit?limit=01",
            f"/v1/instances/{iid}/audit?limit=",
            f"/v1/instances/{iid}/audit?limit=1.5",
            f"/v1/instances/{iid}/audit?limit=true",
            f"/v1/instances/{iid}/audit?afterSeq=-1",
            f"/v1/instances/{iid}/audit?afterSeq=00",
            f"/v1/instances/{iid}/audit?kind=events",
            f"/v1/instances/{iid}/audit?kind=",
            f"/v1/instances/{iid}/audit?limit=1&limit=2",
            f"/v1/instances/{iid}/audit?bogus=1",
            f"/v1/instances/{iid}/audit?limit=2&bogus=1",
        ]
        for path in bad_paths:
            with self.subTest(path=path):
                status, body = self.call("GET", path)
                self.assertEqual(status, 400)
                self.assertEqual(body["error"]["code"], "invalid_request")
        # The rejected queries changed nothing.
        status, audit = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual([h["seq"] for h in audit["history"]], [1, 2])

    def test_invalid_params_on_missing_instance_are_400_not_404(self) -> None:
        status, body = self.call("GET", "/v1/instances/missing/audit?limit=0")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_request")

    def test_valid_params_on_missing_instance_are_404(self) -> None:
        status, body = self.call("GET", "/v1/instances/missing/audit?limit=10&kind=event&afterSeq=0")
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


class IfMatchParsingTests(unittest.TestCase):
    """The only accepted If-Match shape is one double-quoted decimal version."""

    def test_absent_header_means_no_precondition(self) -> None:
        self.assertIsNone(parse_if_match(None))

    def test_quoted_decimals_parse(self) -> None:
        self.assertEqual(parse_if_match('"1"'), 1)
        self.assertEqual(parse_if_match('"0"'), 0)
        self.assertEqual(parse_if_match('"42"'), 42)
        self.assertEqual(parse_if_match('"1000000000"'), 1_000_000_000)

    def test_malformed_values_are_400(self) -> None:
        bad = [
            "",                # empty
            '"',               # lone quote
            '""',              # empty quoted string
            "1",               # unquoted number
            "-1",              # unquoted negative
            " 1 ",             # unquoted with whitespace
            'W/"1"',           # weak validator, uppercase
            'w/"1"',           # weak validator, lowercase
            "W/\"1\"",
            "*",               # wildcard
            '"*"',             # quoted wildcard
            '"1", "2"',        # multiple values
            '"1","2"',         # multiple values, no space
            '"1",*',           # value plus wildcard
            '" 1"',            # leading whitespace inside quotes
            '"1 "',            # trailing whitespace inside quotes
            '"-1"',            # quoted negative
            '"1.0"',           # decimal point
            '"01"',            # leading zero
            '"00"',            # leading zeros
            '"0x1"',           # hex
            '"1\\u0032"',      # escape, not raw digits
            '"abc"',           # non-numeric
            '"1" ',            # trailing data after the closing quote
            ' "*"',            # whitespace then wildcard
        ]
        for raw in bad:
            with self.assertRaises(InvalidRequest, msg=repr(raw)):
                parse_if_match(raw)


def _await2_workflow() -> dict:
    return {"steps": [
        {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved", "timeoutMs": 5000}},
        {"name": "do", "compensation": "undo-do"},
    ]}


class InstanceRevisionEngineTests(unittest.TestCase):
    """Instance-level revision: starts at 1, bumps only on real change."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await2_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "order") -> str:
        started = self.engine.start(workflow, {})
        self.assertEqual(started.revision, 1)  # every instance starts at revision 1
        return started["id"]

    def test_revision_bumps_on_accepted_events_and_signals(self) -> None:
        iid = self._start("approval")
        self.assertEqual(self.engine.get(iid).revision, 1)
        first = self.engine.signal(iid, "approved", event_id="s-1")
        self.assertEqual(first.revision, 2)
        self.assertEqual(self.engine.get(iid).revision, 2)
        second = self.engine.advance(iid, "succeeded", event_id="e-1")
        self.assertEqual(second.revision, 3)
        self.assertEqual(self.engine.get(iid).revision, 3)
        # Audit view reports the same current revision.
        self.assertEqual(self.engine.audit(iid).revision, 3)

    def test_retryable_failure_bumps_once(self) -> None:
        self.engine.define("retryable", {"steps": [
            {"name": "a", "compensation": "undo-a", "retry": {"maxAttempts": 2}}]})
        iid = self.engine.start("retryable", {})["id"]
        result = self.engine.advance(iid, "failed", {"why": 1}, event_id="e-1")
        self.assertEqual(result["state"]["attempt"], 2)
        self.assertEqual(result.revision, 2)
        self.assertEqual(self.engine.get(iid).revision, 2)

    def test_rejected_calls_do_not_bump_revision(self) -> None:
        iid = self._start("approval")  # waiting for approved, revision 1
        # Illegal outcome.
        with self.assertRaises(InvalidRequest):
            self.engine.advance(iid, "bogus", event_id="e-bad")
        # Ordinary result while waiting.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-wait")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", event_id="e-wait2")
        # Mismatched signal name.
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "rejected", event_id="s-x")
        # timed_out before its deadline.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "timed_out", event_id="t-early")
        self.assertEqual(self.engine.get(iid).revision, 1)
        # Accept the signal, then terminal-state pushes and late signals stay flat.
        self.engine.signal(iid, "approved", event_id="s-1")  # -> 2
        self.engine.advance(iid, "succeeded", event_id="e-1")  # completed -> 3
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-after")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", event_id="s-after")
        self.assertEqual(self.engine.get(iid).revision, 3)
        self.assertEqual([h["seq"] for h in self.engine.audit(iid)["history"]], [1, 2])

    def test_accepted_timed_out_bumps_revision(self) -> None:
        self.engine.define("expiring", {"steps": [
            {"name": "ask", "await": {"event": "approved", "timeoutMs": 1}}]})
        iid = self.engine.start("expiring", {})["id"]
        time.sleep(0.02)
        dead = self.engine.advance(iid, "timed_out", {"why": "late"}, event_id="t-1")
        self.assertEqual(dead["state"]["status"], "dead_lettered")
        self.assertEqual(dead.revision, 2)
        self.assertEqual(self.engine.get(iid).revision, 2)

    def test_matching_if_match_proceeds(self) -> None:
        iid = self._start()
        result = self.engine.advance(iid, "succeeded", event_id="e-1", if_match=1)
        self.assertEqual(result.revision, 2)
        result = self.engine.advance(iid, "succeeded", event_id="e-2", if_match=2)
        self.assertEqual(result.revision, 3)

    def test_stale_if_match_is_409_and_changes_nothing(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-1")  # revision now 2
        snapshot = self.engine.get(iid)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-2", if_match=1)
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", event_id="s-x", if_match=1)
        # State, revision, ledger and audit are all untouched.
        current = self.engine.get(iid)
        self.assertEqual(current["state"], snapshot["state"])
        self.assertEqual(current.revision, 2)
        audit = self.engine.audit(iid)
        self.assertEqual([h["eventId"] for h in audit["history"]], ["e-1"])
        self.assertEqual(
            self.engine._db.execute(
                "SELECT event_id FROM instance_events WHERE instance_id = ? ORDER BY event_id", (iid,)
            ).fetchall(),
            [("e-1",)],
        )

    def test_if_match_equal_but_illegal_transition_still_409_without_bump(self) -> None:
        iid = self._start()
        with self.assertRaises(InvalidRequest):
            self.engine.advance(iid, "bogus", event_id="e-bad", if_match=1)
        self.engine.advance(iid, "failed", event_id="e-fin")  # compensated -> 2
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-x", if_match=2)
        self.assertEqual(self.engine.get(iid).revision, 2)

    def test_unknown_instance_is_404_even_with_if_match(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("missing", "succeeded", event_id="e-1", if_match=1)
        with self.assertRaises(InstanceNotFound):
            self.engine.migrate("missing", 1, if_match=1)

    def test_replay_precedes_if_match_even_on_stale_revision(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", {"note": "first"}, event_id="e-1")  # -> 2
        self.engine.advance(iid, "succeeded", event_id="e-2")  # -> 3
        # Stale precondition, but the eventId is a replay: first response verbatim.
        replay = self.engine.advance(iid, "succeeded", {"note": "first"}, event_id="e-1", if_match=1)
        self.assertEqual(replay, first)
        self.assertEqual(replay["state"]["index"], 1)  # historical state
        # The replay envelope reports the *current* revision for its ETag.
        self.assertEqual(replay.revision, 3)
        self.assertEqual(self.engine.get(iid).revision, 3)
        self.assertEqual(len(self.engine.audit(iid)["history"]), 2)

    def test_replay_different_payload_is_409_regardless_of_if_match(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-1")  # -> 2
        # Matching precondition cannot rescue a conflicting replay.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", event_id="e-1", if_match=2)
        # Stale precondition and conflicting payload: still the idempotency 409.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", event_id="e-1", if_match=1)
        self.assertEqual(self.engine.get(iid).revision, 2)
        self.assertEqual(len(self.engine.audit(iid)["history"]), 1)

    def test_signal_replay_precedes_if_match(self) -> None:
        iid = self._start("approval")
        first = self.engine.signal(iid, "approved", {"by": "boss"}, event_id="s-1")  # -> 2
        replay = self.engine.signal(iid, "approved", {"by": "boss"}, event_id="s-1", if_match=1)
        self.assertEqual(replay, first)
        self.assertEqual(replay.revision, 2)
        self.assertEqual(self.engine.get(iid).revision, 2)

    def test_migrate_bumps_revision_and_preserves_state_field_by_field(self) -> None:
        # v2 keeps the entered step 0 identical and only reworks the not-yet-entered
        # step 1's compensation, so the migration is compatible.
        self.engine.define("approval", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved", "timeoutMs": 5000}},
            {"name": "do", "compensation": "undo-do-v2"},
        ]})
        iid = self.engine.start("approval", {}, version=1)["id"]
        before = self.engine.get(iid)["state"]
        migrated = self.engine.migrate(iid, 2)
        self.assertEqual(migrated["workflowVersion"], 2)
        self.assertEqual(migrated["state"], before)  # every state field carried over
        self.assertEqual(migrated.revision, 2)
        self.assertEqual(self.engine.get(iid).revision, 2)
        self.assertEqual(self.engine.get(iid)["state"], before)
        # Advancing after the migration uses v2's definitions.
        self.engine.signal(iid, "approved", event_id="s-1")  # enters "do" -> 3
        failed = self.engine.advance(iid, "failed", event_id="e-1")  # -> 4
        self.assertEqual(failed["state"]["compensated"], ["undo-do-v2", "cancel-ask"])
        # Migration wrote neither ledger rows nor audit records.
        self.assertEqual([h["seq"] for h in self.engine.audit(iid)["history"]], [1, 2])
        self.assertEqual(self.engine.get(iid).revision, 4)

    def test_migrate_with_if_match(self) -> None:
        self.engine.define("approval", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved", "timeoutMs": 5000}},
            {"name": "do", "compensation": "undo-do-v2"},
        ]})
        iid = self.engine.start("approval", {}, version=1)["id"]
        self.assertEqual(self.engine.migrate(iid, 2, if_match=1).revision, 2)
        # A second migration pinned to the old observation is rejected, changes nothing.
        self.engine.define("approval", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved", "timeoutMs": 5000}},
            {"name": "do", "compensation": "undo-do-v3"},
        ]})
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 3, if_match=1)
        self.assertEqual(self.engine.get(iid)["workflowVersion"], 2)
        self.assertEqual(self.engine.get(iid).revision, 2)
        # The current observation migrates fine.
        self.assertEqual(self.engine.migrate(iid, 3, if_match=2).revision, 3)

    def test_incompatible_migrate_does_not_bump(self) -> None:
        self.engine.define("approval", {"steps": [{"name": "ask"}]})  # different step count
        iid = self.engine.start("approval", {}, version=1)["id"]
        with self.assertRaises(InvalidTransition):
            self.engine.migrate(iid, 2, if_match=1)
        self.assertEqual(self.engine.get(iid).revision, 1)
        self.assertEqual(self.engine.get(iid)["workflowVersion"], 1)

    def test_concurrent_stale_preconditions_one_winner(self) -> None:
        iid = self._start()
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        lock = threading.Lock()

        def submit(event_id: str) -> None:
            try:
                barrier.wait()
                self.engine.advance(iid, "succeeded", event_id=event_id, if_match=1)
                with lock:
                    outcomes.append("ok")
            except InvalidTransition:
                with lock:
                    outcomes.append("conflict")
            except Exception as exc:  # noqa: BLE001
                with lock:
                    outcomes.append(f"error:{exc!r}")

        threads = [threading.Thread(target=submit, args=(f"e-{i}",)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(outcomes), ["conflict", "ok"])
        self.assertEqual(self.engine.get(iid).revision, 2)
        self.assertEqual(self.engine.get(iid)["state"]["completed"], ["reserve-stock"])
        # The loser's eventId wrote no ledger row.
        rows = self.engine._db.execute(
            "SELECT COUNT(*) FROM instance_events WHERE instance_id = ?", (iid,)
        ).fetchone()[0]
        self.assertEqual(rows, 1)


class InstanceRevisionPersistenceTests(unittest.TestCase):
    """Revisions survive reopen; legacy files read as revision 1, then jump to 2."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_revision_survives_reopen(self) -> None:
        engine = Engine(self.path)
        iid = engine.start("order", {})["id"]
        engine.advance(iid, "succeeded", event_id="e-1")
        engine.advance(iid, "succeeded", event_id="e-2")
        engine.close()

        engine = Engine(self.path)
        try:
            self.assertEqual(engine.get(iid).revision, 3)
            result = engine.advance(iid, "succeeded", event_id="e-3")
            self.assertEqual(result.revision, 4)
            engine.close()

            engine = Engine(self.path)
            self.assertEqual(engine.get(iid).revision, 4)
            # A replay after reopen still reports the current revision for its ETag.
            replay = engine.advance(iid, "succeeded", event_id="e-1")
            self.assertEqual(replay.revision, 4)
        finally:
            engine.close()

    def test_legacy_instance_reads_as_revision_1_then_becomes_2(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute(
            "CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)"
        )
        state = initial_state(WORKFLOWS["order"], {})
        state = apply_outcome(WORKFLOWS["order"], state, "succeeded")
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            # No historical change count is fabricated: reads as revision 1.
            current = engine.get("legacy-id")
            self.assertEqual(current.revision, 1)
            self.assertEqual(current["state"]["completed"], ["reserve-stock"])
            self.assertEqual(engine.audit("legacy-id").revision, 1)
            # A rejected call (signal to a step that is not waiting) does not bump.
            with self.assertRaises(InvalidTransition):
                engine.signal("legacy-id", "anything", event_id="s-x")
            self.assertEqual(engine.get("legacy-id").revision, 1)
            # First real post-upgrade change jumps 1 -> 2 and persists that way.
            result = engine.advance("legacy-id", "succeeded", event_id="e-1")
            self.assertEqual(result.revision, 2)
            stored = engine._db.execute(
                "SELECT instance_version FROM instances WHERE id = ?", ("legacy-id",)
            ).fetchone()[0]
            self.assertEqual(stored, 2)
            engine.close()

            engine = Engine(self.path)
            self.assertEqual(engine.get("legacy-id").revision, 2)
        finally:
            engine.close()


class InstanceRevisionHttpTests(unittest.TestCase):
    """ETag emission and If-Match precondition over HTTP."""

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
        hdrs = {"Content-Type": "application/json"}
        if headers:
            hdrs.update(headers)
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), response.headers
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), error.headers

    def _start(self, workflow: str = "order"):
        status, body, headers = self.call("POST", f"/v1/workflows/{workflow}/instances", {})
        self.assertEqual(status, 201)
        return body["id"], headers

    def test_etag_emitted_on_start_get_and_audit(self) -> None:
        iid, start_headers = self._start()
        self.assertEqual(start_headers.get("ETag"), '"1"')
        status, _, headers = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"1"')
        status, _, headers = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"1"')

    def test_successful_changes_return_current_etag(self) -> None:
        iid, _ = self._start()
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/events",
                                          {"outcome": "succeeded", "eventId": "e-1"})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"2"')
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/events",
                                          {"outcome": "succeeded", "eventId": "e-2"},
                                          {"If-Match": '"2"'})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2].get("ETag"), '"3"')

    def test_stale_if_match_is_409_and_state_unchanged(self) -> None:
        iid, _ = self._start()
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded", "eventId": "e-1"})
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/events",
                                          {"outcome": "succeeded", "eventId": "e-2"},
                                          {"If-Match": '"1"'})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "invalid_transition")
        self.assertIsNone(headers.get("ETag"))  # error responses carry no ETag
        # The rejected call did not move the revision or write a ledger row.
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2].get("ETag"), '"2"')
        status, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual([h["eventId"] for h in audit["history"]], ["e-1"])

    def test_malformed_if_match_is_400(self) -> None:
        iid, _ = self._start()
        for raw in ("", "1", 'W/"1"', "*", '"1", "2"', '"01"', '"-1"', '"1.0"', '""'):
            status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                        {"outcome": "succeeded", "eventId": "e-bad"},
                                        {"If-Match": raw})
            self.assertEqual(status, 400, repr(raw))
            self.assertEqual(body["error"]["code"], "invalid_request", repr(raw))
        # Nothing was written: instance still at revision 1 with an empty ledger.
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2].get("ETag"), '"1"')
        status, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(audit["history"], [])

    def test_omitted_if_match_keeps_baseline_contract(self) -> None:
        iid, _ = self._start()
        # No header: advances behave exactly as before, including terminal 409s.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "failed"})[0], 200)
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded"})[0], 409)

    def test_replay_on_stale_if_match_returns_first_response_with_current_etag(self) -> None:
        iid, _ = self._start()
        status, first, first_headers = self.call(
            "POST", f"/v1/instances/{iid}/events",
            {"outcome": "succeeded", "detail": {"n": 1}, "eventId": "e-1"})
        self.assertEqual(first_headers.get("ETag"), '"2"')
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded", "eventId": "e-2"})
        # Stale precondition, but e-1 is a replay: 200 with the first body and the
        # ETag of the *current* revision.
        status, replay, headers = self.call(
            "POST", f"/v1/instances/{iid}/events",
            {"outcome": "succeeded", "detail": {"n": 1}, "eventId": "e-1"},
            {"If-Match": '"1"'})
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        self.assertEqual(headers.get("ETag"), '"3"')

    def test_signal_and_migrate_if_match_end_to_end(self) -> None:
        self.call("PUT", "/v1/workflows/ccflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        self.call("PUT", "/v1/workflows/ccflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2-new"},
        ]})
        iid = self.call("POST", "/v1/workflows/ccflow/instances", {"version": 1})[1]["id"]
        # While waiting at s1: a stale precondition blocks the migration, a
        # matching one repins to v2 with state preserved and revision -> 2.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/migrate",
                                   {"version": 2}, {"If-Match": '"7"'})[0], 409)
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/migrate",
                                          {"version": 2}, {"If-Match": '"1"'})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"2"')
        self.assertEqual(body["workflowVersion"], 2)
        self.assertEqual((body["state"]["status"], body["state"]["step"], body["state"]["waitingFor"]),
                         ("running", "s1", "go"))
        # A stale signal is rejected; the matching precondition unblocks and
        # enters s2 under the now-pinned v2 definition, revision -> 3.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/signals",
                                   {"event": "go", "eventId": "s-0"},
                                   {"If-Match": '"1"'})[0], 409)
        status, _, headers = self.call("POST", f"/v1/instances/{iid}/signals",
                                       {"event": "go", "eventId": "s-1"},
                                       {"If-Match": '"2"'})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2].get("ETag"), '"3"')
        # A fail now compensates with the v2 name, bumping revision once more.
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/events",
                                          {"outcome": "failed", "eventId": "e-1"},
                                          {"If-Match": '"3"'})
        self.assertEqual(status, 200)
        self.assertEqual(body["state"]["compensated"], ["c2-new", "c1"])
        self.assertEqual(headers.get("ETag"), '"4"')


class PartitionOrderingValidationTests(unittest.TestCase):
    """partitionKey/sequence gate at the Engine boundary: 400 shapes, no writes."""

    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self) -> str:
        return self.engine.start("order", {})["id"]

    def test_ordering_requires_both_fields_and_event_id(self) -> None:
        iid = self._start()
        # Only one of the pair, with or without eventId.
        with self.assertRaises(InvalidRequest):
            self.engine.advance(iid, "succeeded", event_id="e-1", partition_key="p")
        with self.assertRaises(InvalidRequest):
            self.engine.advance(iid, "succeeded", event_id="e-1", sequence=1)
        # Both present but no eventId (anonymous and keyword-named).
        with self.assertRaises(InvalidRequest):
            self.engine.advance(iid, "succeeded", partition_key="p", sequence=1)
        # Signal endpoint behaves the same.
        with self.assertRaises(InvalidRequest):
            self.engine.signal(iid, "approved", event_id="s-1", partition_key="p")
        with self.assertRaises(InvalidRequest):
            self.engine.signal(iid, "approved", partition_key="p", sequence=1)

    def test_bad_partition_key_shapes_are_400(self) -> None:
        iid = self._start()
        for bad in (None, "", 1, 1.5, True, ["p"], {"p": 1}, "x" * 101):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.advance(iid, "succeeded", event_id="e-1",
                                    partition_key=bad, sequence=1)

    def test_bad_sequence_shapes_are_400(self) -> None:
        iid = self._start()
        for bad in (0, -1, -10**9, 1.5, 2.0, "1", "", None, True, False, [1], {"n": 1}):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.advance(iid, "succeeded", event_id="e-1",
                                    partition_key="p", sequence=bad)

    def test_partition_key_boundaries_accepted(self) -> None:
        # 1 and 100 characters are both valid; they are distinct partitions.
        a = self._start()
        r1 = self.engine.advance(a, "succeeded", event_id="e-1", partition_key="x", sequence=1)
        self.assertEqual(r1["ordering"], {"partitionKey": "x", "sequence": 1})
        b = self._start()
        key = "y" * 100
        r2 = self.engine.advance(b, "succeeded", event_id="e-2", partition_key=key, sequence=1)
        self.assertEqual(r2["ordering"], {"partitionKey": key, "sequence": 1})

    def test_invalid_ordering_writes_nothing(self) -> None:
        iid = self._start()
        for kwargs in (
            dict(event_id="e-1", partition_key="p"),
            dict(partition_key="p", sequence=1),
            dict(event_id="e-1", partition_key="p", sequence=0),
            dict(event_id="e-1", partition_key=7, sequence=1),
        ):
            with self.assertRaises(InvalidRequest):
                self.engine.advance(iid, "succeeded", **kwargs)
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
        )
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0], 0
        )
        self.assertEqual(self.engine.get(iid).revision, 1)


class PartitionOrderingEngineTests(unittest.TestCase):
    """Cursor continuity, replay/precondition/sequence precedence and isolation."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "order") -> str:
        return self.engine.start(workflow, {})["id"]

    def test_first_sequence_must_be_one_then_increments(self) -> None:
        iid = self._start()
        # A fresh partition only accepts 1 first.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-2", partition_key="p", sequence=2)
        first = self.engine.advance(iid, "succeeded", event_id="e-1",
                                    partition_key="p", sequence=1)
        self.assertEqual(first["ordering"], {"partitionKey": "p", "sequence": 1})
        self.assertEqual(first.revision, 2)
        second = self.engine.advance(iid, "succeeded", event_id="e-2",
                                     partition_key="p", sequence=2)
        self.assertEqual(second["ordering"]["sequence"], 2)

    def test_events_and_signals_share_one_cursor(self) -> None:
        iid = self._start("approval")
        s1 = self.engine.signal(iid, "approved", {"by": "boss"}, event_id="s-1",
                                partition_key="p", sequence=1)
        self.assertEqual(s1["ordering"]["sequence"], 1)
        e1 = self.engine.advance(iid, "succeeded", event_id="e-1",
                                 partition_key="p", sequence=2)
        self.assertEqual(e1["ordering"]["sequence"], 2)
        self.assertEqual(e1["state"]["waitingFor"], "shipped")
        s2 = self.engine.signal(iid, "shipped", event_id="s-2",
                                partition_key="p", sequence=3)
        self.assertEqual((s2["state"]["status"], s2["ordering"]["sequence"]),
                         ("completed", 3))
        self.assertEqual(
            self.engine._db.execute(
                "SELECT cursor FROM partition_cursors WHERE workflow = ? AND partition_key = ?",
                ("approval", "p")).fetchone()[0],
            3,
        )

    def test_cursor_spans_instances_and_is_scoped_by_workflow_and_key(self) -> None:
        a, b = self._start("order"), self._start("order")
        other = self._start("provision")
        self.engine.advance(a, "succeeded", event_id="a-1", partition_key="p", sequence=1)
        # Same workflow name, different instance: cursor continues.
        r = self.engine.advance(b, "succeeded", event_id="b-1", partition_key="p", sequence=2)
        self.assertEqual(r["ordering"]["sequence"], 2)
        # Same key, different workflow: independent partition from 1.
        r = self.engine.advance(other, "succeeded", event_id="o-1",
                                partition_key="p", sequence=1)
        self.assertEqual(r["ordering"], {"partitionKey": "p", "sequence": 1})
        # Same workflow, different key: independent partition from 1.
        c = self._start("order")
        r = self.engine.advance(c, "succeeded", event_id="c-1",
                                partition_key="other", sequence=1)
        self.assertEqual(r["ordering"]["sequence"], 1)
        rows = self.engine._db.execute(
            "SELECT workflow, partition_key, cursor FROM partition_cursors ORDER BY workflow, partition_key"
        ).fetchall()
        self.assertEqual(rows, [("order", "other", 1), ("order", "p", 2), ("provision", "p", 1)])

    def test_gap_and_stale_sequence_are_409_and_change_nothing(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", event_id="e-1", partition_key="p", sequence=1)
        snapshot = self.engine.get(iid)
        # Gap (3 while expecting 2) and stale (1 again) are both 409.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-gap",
                                partition_key="p", sequence=3)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-old",
                                partition_key="p", sequence=1)
        # No buffering: out-of-order messages are simply refused, not held.
        current = self.engine.get(iid)
        self.assertEqual(current["state"], snapshot["state"])
        self.assertEqual(current.revision, 2)
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 1
        )
        self.assertEqual(self.engine.audit(iid)["history"][0]["eventId"], "e-1")
        # The rejected eventIds wrote no ledger rows; seq 2 still proceeds.
        r = self.engine.advance(iid, "succeeded", event_id="e-2",
                                partition_key="p", sequence=2)
        self.assertEqual(r["ordering"]["sequence"], 2)

    def test_ordered_replay_returns_first_response_without_consuming_sequence(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", {"note": 1}, event_id="e-1",
                                    partition_key="p", sequence=1)
        replay = self.engine.advance(iid, "succeeded", {"note": 1}, event_id="e-1",
                                     partition_key="p", sequence=1)
        self.assertEqual(replay, first)
        self.assertEqual(replay["ordering"], {"partitionKey": "p", "sequence": 1})
        # State advanced once, cursor consumed once, one audit row.
        self.assertEqual(self.engine.get(iid)["state"]["completed"], ["reserve-stock"])
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 1
        )
        self.assertEqual(len(self.engine.audit(iid)["history"]), 1)

    def test_same_event_id_different_ordering_or_payload_is_409(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "succeeded", {"v": 1}, event_id="e-1",
                            partition_key="p", sequence=1)
        # Different sequence / key / outcome / detail all conflict on the eventId.
        for kwargs, overrides in (
            (dict(partition_key="p", sequence=2), {}),
            (dict(partition_key="q", sequence=1), {}),
            (dict(partition_key="p", sequence=1), dict(outcome="failed")),
        ):
            with self.assertRaises(InvalidTransition, msg=str(kwargs)):
                self.engine.advance(iid, overrides.get("outcome", "succeeded"), {"v": 1},
                                    event_id="e-1", **kwargs)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", {"v": 2}, event_id="e-1",
                                partition_key="p", sequence=1)
        # Cursor and state untouched by the conflicts; original still replays.
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 1
        )
        self.assertEqual(
            self.engine.advance(iid, "succeeded", {"v": 1}, event_id="e-1",
                                partition_key="p", sequence=1)["state"]["index"],
            1,
        )

    def test_replay_then_if_match_then_sequence_precedence(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "succeeded", event_id="e-1",
                                    partition_key="p", sequence=1)  # revision 2
        # 1) Replay wins over a stale If-Match and over the sequence gate.
        replay = self.engine.advance(iid, "succeeded", event_id="e-1",
                                     partition_key="p", sequence=1, if_match=1)
        self.assertEqual(replay, first)
        # 2) A new eventId: stale If-Match rejected before the sequence is read.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-2",
                                partition_key="p", sequence=2, if_match=1)
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 1
        )
        # 3) Matching If-Match but wrong sequence: sequence gate rejects.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="e-2",
                                partition_key="p", sequence=3, if_match=2)
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 1
        )
        # Both satisfied: accepted.
        r = self.engine.advance(iid, "succeeded", event_id="e-2",
                                partition_key="p", sequence=2, if_match=2)
        self.assertEqual(r["ordering"]["sequence"], 2)

    def test_correct_sequence_but_rejected_transition_consumes_nothing(self) -> None:
        a, b = self._start("provision"), self._start("provision")
        self.engine.advance(a, "succeeded", event_id="a-1", partition_key="p", sequence=1)
        self.engine.advance(a, "succeeded", event_id="a-2", partition_key="p", sequence=2)
        self.assertEqual(self.engine.get(a)["state"]["status"], "completed")
        # seq 3 is current but the terminal instance refuses the transition.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(a, "succeeded", event_id="a-3",
                                partition_key="p", sequence=3)
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 2
        )
        # The sequence was not consumed: another instance takes it.
        r = self.engine.advance(b, "succeeded", event_id="b-1",
                                partition_key="p", sequence=3)
        self.assertEqual(r["ordering"]["sequence"], 3)

    def test_unknown_ordered_instance_is_404_without_partition_row(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("missing", "succeeded", event_id="e-1",
                                partition_key="p", sequence=1)
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
        )

    def test_unordered_calls_keep_baseline_shape_and_create_no_partition(self) -> None:
        iid = self._start()
        r1 = self.engine.advance(iid, "succeeded", event_id="e-1")
        r2 = self.engine.advance(iid, "succeeded")
        self.assertNotIn("ordering", r1)
        self.assertNotIn("ordering", r2)
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
        )
        req = self.engine.audit(iid)["history"][0]["request"]
        self.assertNotIn("partitionKey", req)
        self.assertNotIn("sequence", req)

    def test_ordered_audit_request_and_response_record_ordering(self) -> None:
        iid = self._start("approval")
        result = self.engine.signal(iid, "approved", {"by": "boss"}, event_id="s-1",
                                   partition_key="p", sequence=1)
        record = self.engine.audit(iid)["history"][0]
        self.assertEqual(record["request"], {
            "eventId": "s-1", "event": "approved", "detail": {"by": "boss"},
            "partitionKey": "p", "sequence": 1,
        })
        self.assertEqual(record["response"], result)
        self.assertEqual(record["response"]["ordering"],
                         {"partitionKey": "p", "sequence": 1})

    def test_migration_keeps_the_partition_cursor(self) -> None:
        # The cursor is keyed by the workflow name, which never changes across a
        # version migration, so an ordered chain survives a migrate() in between.
        self.engine.define("approval", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "approved"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "cancel-ship"},
        ]})
        iid = self._start("approval")
        self.engine.signal(iid, "approved", event_id="s-1", partition_key="p", sequence=1)
        self.engine.migrate(iid, 2)
        r = self.engine.advance(iid, "succeeded", event_id="e-1",
                                partition_key="p", sequence=2)
        self.assertEqual(r["ordering"]["sequence"], 2)
        self.assertEqual(
            self.engine._db.execute(
                "SELECT cursor FROM partition_cursors WHERE workflow = 'approval' AND partition_key = 'p'"
            ).fetchone()[0],
            2,
        )

    def test_concurrent_same_next_sequence_has_single_winner(self) -> None:
        iid = self._start()
        barrier = threading.Barrier(3)
        outcomes: list[str] = []
        lock = threading.Lock()

        def submit(event_id: str) -> None:
            try:
                barrier.wait()
                self.engine.advance(iid, "succeeded", event_id=event_id,
                                    partition_key="p", sequence=1)
                with lock:
                    outcomes.append("ok")
            except InvalidTransition:
                with lock:
                    outcomes.append("conflict")
            except Exception as exc:  # noqa: BLE001
                with lock:
                    outcomes.append(f"error:{exc!r}")

        threads = [threading.Thread(target=submit, args=(f"e-{i}",)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(outcomes), ["conflict", "conflict", "ok"])
        # Exactly one call acquired the sequence: cursor 1, one ledger/audit row.
        self.assertEqual(
            self.engine._db.execute("SELECT cursor FROM partition_cursors").fetchone()[0], 1
        )
        self.assertEqual(
            self.engine._db.execute(
                "SELECT COUNT(*) FROM instance_events WHERE instance_id = ?", (iid,)).fetchone()[0],
            1,
        )
        self.assertEqual(len(self.engine.audit(iid)["history"]), 1)
        self.assertEqual(self.engine.get(iid)["state"]["completed"], ["reserve-stock"])


class PartitionOrderingPersistenceTests(unittest.TestCase):
    """Cursors survive reopen alongside ledger/audit; legacy files upgrade cleanly."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_cursor_ledger_audit_consistent_across_reopen(self) -> None:
        engine = Engine(self.path)
        engine.define("approval", _await_workflow())
        iid = engine.start("approval", {})["id"]
        first = engine.signal(iid, "approved", event_id="s-1", partition_key="p", sequence=1)
        engine.advance(iid, "succeeded", event_id="e-1", partition_key="p", sequence=2)
        engine.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            self.assertEqual(
                engine._db.execute(
                    "SELECT workflow, partition_key, cursor FROM partition_cursors").fetchone(),
                ("approval", "p", 2),
            )
            # The ordered replay still resolves to the first response.
            self.assertEqual(
                engine.signal(iid, "approved", event_id="s-1",
                              partition_key="p", sequence=1),
                first,
            )
            # Old numbers and gaps stay refused; the chain continues at 3.
            with self.assertRaises(InvalidTransition):
                engine.signal(iid, "approved", event_id="s-x",
                              partition_key="p", sequence=2)
            done = engine.signal(iid, "shipped", event_id="s-2",
                                 partition_key="p", sequence=3)
            self.assertEqual((done["state"]["status"], done["ordering"]["sequence"]),
                             ("completed", 3))
            history = engine.audit(iid)["history"]
            self.assertEqual([h["request"]["sequence"] for h in history], [1, 2, 3])
        finally:
            engine.close()

    def test_legacy_file_empty_cursor_table_new_partition_starts_at_one(self) -> None:
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
            # The cursor table is created on open, empty: no history fabricated.
            self.assertEqual(
                engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
            )
            # Old records are not supplemented with ordering.
            self.assertEqual(engine.audit("legacy-id")["history"], [])
            result = engine.advance("legacy-id", "succeeded", event_id="new-1",
                                    partition_key="p", sequence=1)
            self.assertEqual(result["ordering"], {"partitionKey": "p", "sequence": 1})
            self.assertEqual(
                engine._db.execute(
                    "SELECT workflow, partition_key, cursor FROM partition_cursors").fetchone(),
                ("order", "p", 1),
            )
        finally:
            engine.close()

    def test_pre_upgrade_ledger_row_still_replays_after_upgrade(self) -> None:
        # A ledger row written by the baseline build has a normalized request with
        # no ordering keys; the upgraded code must keep producing the identical
        # canonical form for an unordered replay instead of flagging a conflict.
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        state = initial_state(WORKFLOWS["order"], {})
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.execute(
            "CREATE TABLE instance_events (instance_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "request TEXT NOT NULL, response TEXT NOT NULL, PRIMARY KEY (instance_id, event_id))"
        )
        response = {"id": "legacy-id", "workflow": "order", "workflowVersion": None,
                    "state": apply_outcome(WORKFLOWS["order"], state, "succeeded")}
        legacy.execute(
            "INSERT INTO instance_events VALUES (?, ?, ?, ?)",
            ("legacy-id", "old-1",
             json.dumps({"eventId": "old-1", "outcome": "succeeded", "detail": None},
                        separators=(",", ":"), sort_keys=True),
             json.dumps(response)),
        )
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            replayed = engine.advance("legacy-id", "succeeded", event_id="old-1")
            self.assertEqual(replayed, response)
            # The old unordered row is not retroactively treated as ordered.
            self.assertNotIn("ordering", replayed)
            self.assertEqual(
                engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
            )
        finally:
            engine.close()


class PartitionOrderingHttpTests(unittest.TestCase):
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
        hdrs = {"Content-Type": "application/json"}
        if headers:
            hdrs.update(headers)
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), response.headers
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), error.headers

    def _start(self, workflow: str = "order") -> str:
        return self.call("POST", f"/v1/workflows/{workflow}/instances", {})[1]["id"]

    def test_ordered_validation_shapes_are_400(self) -> None:
        self.call("PUT", "/v1/workflows/approval", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "approved"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        iid = self._start("approval")
        bad_bodies = [
            {"event": "approved", "eventId": "s-1", "partitionKey": "p"},
            {"event": "approved", "eventId": "s-1", "sequence": 1},
            {"event": "approved", "partitionKey": "p", "sequence": 1},
            {"event": "approved", "eventId": "s-1", "partitionKey": "", "sequence": 1},
            {"event": "approved", "eventId": "s-1", "partitionKey": 5, "sequence": 1},
            {"event": "approved", "eventId": "s-1", "partitionKey": "p", "sequence": 0},
            {"event": "approved", "eventId": "s-1", "partitionKey": "p", "sequence": -3},
            {"event": "approved", "eventId": "s-1", "partitionKey": "p", "sequence": 1.5},
            {"event": "approved", "eventId": "s-1", "partitionKey": "p", "sequence": "1"},
            {"event": "approved", "eventId": "s-1", "partitionKey": "p", "sequence": True},
        ]
        for body in bad_bodies:
            status, resp, _ = self.call("POST", f"/v1/instances/{iid}/signals", body)
            self.assertEqual(status, 400, body)
            self.assertEqual(resp["error"]["code"], "invalid_request", body)
        # events endpoint: lone ordering field and unknown field are 400 too.
        status, resp, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "succeeded", "eventId": "e-1", "sequence": 1})
        self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        status, _, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "succeeded", "eventId": "e-1",
                                  "partitionKey": "p", "sequence": 1, "bogus": 1})
        self.assertEqual(status, 400)

    def test_ordered_flow_end_to_end_across_instances(self) -> None:
        self.call("PUT", "/v1/workflows/pflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        a = self.call("POST", "/v1/workflows/pflow/instances", {})[1]["id"]
        b = self.call("POST", "/v1/workflows/pflow/instances", {})[1]["id"]
        # seq 1: ordered signal on instance a, response carries ordering + ETag "2".
        status, body, headers = self.call("POST", f"/v1/instances/{a}/signals",
                                          {"event": "go", "detail": {"by": "boss"},
                                           "eventId": "s-1", "partitionKey": "tenant-1",
                                           "sequence": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body["ordering"], {"partitionKey": "tenant-1", "sequence": 1})
        self.assertEqual(headers.get("ETag"), '"2"')
        # Verbatim replay (same sequence number) returns the first response.
        status, replay, _ = self.call("POST", f"/v1/instances/{a}/signals",
                                      {"event": "go", "detail": {"by": "boss"},
                                       "eventId": "s-1", "partitionKey": "tenant-1",
                                       "sequence": 1})
        self.assertEqual((status, replay), (200, body))
        # seq 2 arrives at a *different* instance and is 409 for a gap, ok in order.
        status, resp, _ = self.call("POST", f"/v1/instances/{b}/signals",
                                    {"event": "go", "eventId": "s-2",
                                     "partitionKey": "tenant-1", "sequence": 3})
        self.assertEqual((status, resp["error"]["code"]), (409, "invalid_transition"))
        status, body, _ = self.call("POST", f"/v1/instances/{b}/signals",
                                    {"event": "go", "eventId": "s-2",
                                     "partitionKey": "tenant-1", "sequence": 2})
        self.assertEqual(status, 200)
        self.assertEqual(body["ordering"]["sequence"], 2)
        # Unknown instance stays 404 even with ordering fields.
        status, resp, _ = self.call("POST", "/v1/instances/missing/events",
                                    {"outcome": "succeeded", "eventId": "e-x",
                                     "partitionKey": "tenant-1", "sequence": 3})
        self.assertEqual((status, resp["error"]["code"]), (404, "not_found"))

    def test_ordered_if_match_conflict_and_audit(self) -> None:
        iid = self._start()
        status, _, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "succeeded", "eventId": "e-1",
                                  "partitionKey": "p", "sequence": 1})
        self.assertEqual(status, 200)
        # Stale If-Match beats the (correct) sequence: 409, no cursor movement.
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "succeeded", "eventId": "e-2",
                                     "partitionKey": "p", "sequence": 2},
                                    {"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/events",
                                          {"outcome": "succeeded", "eventId": "e-2",
                                           "partitionKey": "p", "sequence": 2},
                                          {"If-Match": '"2"'})
        self.assertEqual(status, 200)
        self.assertEqual(body["ordering"], {"partitionKey": "p", "sequence": 2})
        self.assertEqual(headers.get("ETag"), '"3"')
        # Audit request/response carry the ordering coordinates.
        status, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(status, 200)
        self.assertEqual([h["request"]["sequence"] for h in audit["history"]], [1, 2])
        self.assertEqual(audit["history"][1]["request"]["partitionKey"], "p")
        self.assertEqual(audit["history"][1]["response"]["ordering"],
                         {"partitionKey": "p", "sequence": 2})


class RecoverTransitionTests(unittest.TestCase):
    """Pure dead_lettered -> running recovery transition."""

    def setUp(self) -> None:
        self.workflow = _timeout_workflow()  # ask: await+timeout, do: plain, wait-forever: await no timeout

    def _dead_lettered(self, now_ms: int = 1000) -> dict:
        state = initial_state(self.workflow, {"customer": "c1"}, now_ms=now_ms)
        return apply_outcome(self.workflow, state, "timed_out", {"why": "late"}, now_ms=6000)

    def test_recover_re_arms_timeout_wait_and_resets_fields(self) -> None:
        dead = self._dead_lettered()
        recovered = apply_recover(self.workflow, dead, now_ms=7000)
        self.assertEqual(recovered["status"], "running")
        self.assertIsNone(recovered["failure"])
        self.assertEqual(recovered["attempt"], 1)
        # Position and progress are carried over exactly.
        self.assertEqual((recovered["step"], recovered["index"]), ("ask", 0))
        self.assertEqual(recovered["completed"], [])
        self.assertEqual(recovered["compensated"], [])
        self.assertEqual(recovered["context"], {"customer": "c1"})
        # The wait is re-armed with a fresh deadline measured from the recovery moment.
        self.assertEqual(recovered["waitingFor"], "approved")
        self.assertEqual(recovered["deadlineAt"], 7000 + 5000)

    def test_recover_does_not_mutate_input(self) -> None:
        dead = self._dead_lettered()
        apply_recover(self.workflow, dead, now_ms=7000)
        self.assertEqual(dead["status"], "dead_lettered")
        self.assertEqual(dead["failure"],
                         {"step": "ask", "reason": "timeout", "detail": {"why": "late"}})
        self.assertIsNone(dead["waitingFor"])
        self.assertIsNone(dead["deadlineAt"])

    def test_recover_after_progress_keeps_completed_and_awaits_without_timeout(self) -> None:
        # Hand-build a dead-lettered instance later in the workflow: "ask"/"do" are
        # already completed and the current step "wait-forever" awaits without timeoutMs.
        state = initial_state(self.workflow, {}, now_ms=1000)
        state = apply_signal(self.workflow, state, "approved", now_ms=2000)
        state = apply_outcome(self.workflow, state, "succeeded", now_ms=3000)
        self.assertEqual((state["step"], state["index"], state["waitingFor"]),
                         ("wait-forever", 2, "never"))
        state["status"] = "dead_lettered"
        state["failure"] = {"step": "wait-forever", "reason": "timeout", "detail": None}
        state["waitingFor"] = None
        state["deadlineAt"] = None
        recovered = apply_recover(self.workflow, state, now_ms=9000)
        self.assertEqual((recovered["status"], recovered["step"], recovered["index"]),
                         ("running", "wait-forever", 2))
        self.assertEqual(recovered["completed"], ["ask", "do"])
        self.assertEqual(recovered["attempt"], 1)
        self.assertIsNone(recovered["failure"])
        # An await without timeoutMs re-arms with a null deadline.
        self.assertEqual(recovered["waitingFor"], "never")
        self.assertIsNone(recovered["deadlineAt"])

    def test_recover_dead_letter_on_plain_step_waits_for_nothing(self) -> None:
        # Dead-lettering only happens via timeouts today, but recovery must still
        # follow the current step's definition for any persisted dead_lettered row.
        state = initial_state(self.workflow, {})
        state = apply_signal(self.workflow, state, "approved")
        self.assertEqual(state["step"], "do")  # plain step, no await
        state["status"] = "dead_lettered"
        state["failure"] = {"step": "do", "reason": "timeout", "detail": None}
        recovered = apply_recover(self.workflow, state, now_ms=1000)
        self.assertEqual(recovered["status"], "running")
        self.assertIsNone(recovered["waitingFor"])
        self.assertIsNone(recovered["deadlineAt"])
        self.assertEqual((recovered["step"], recovered["index"]), ("do", 1))

    def test_non_dead_states_are_409_and_untouched(self) -> None:
        running = initial_state(self.workflow, {})
        # The timeout workflow ends on an awaiting step, so build completed and
        # compensated states with the built-in order workflow instead.
        completed = initial_state(WORKFLOWS["order"], {})
        for _ in range(3):
            completed = apply_outcome(WORKFLOWS["order"], completed, "succeeded")
        self.assertEqual(completed["status"], "completed")
        compensated = apply_outcome(WORKFLOWS["order"],
                                    initial_state(WORKFLOWS["order"], {}), "failed")
        self.assertEqual(compensated["status"], "compensated")
        for state, label in ((running, "running"), (completed, "completed"),
                             (compensated, "compensated")):
            snapshot = json.loads(json.dumps(state))
            with self.assertRaises(InvalidTransition, msg=label):
                apply_recover(self.workflow, state)
            self.assertEqual(state, snapshot, label)

    def test_repeated_recover_is_409(self) -> None:
        recovered = apply_recover(self.workflow, self._dead_lettered(), now_ms=7000)
        with self.assertRaises(InvalidTransition):
            apply_recover(self.workflow, recovered, now_ms=8000)


class RecoverEngineTests(unittest.TestCase):
    """Engine-level recovery: revisioning, ledgers, audit, cursors and preconditions."""

    def setUp(self) -> None:
        self.engine = Engine()
        # A plain first step and a timeout-awaiting second step, so progress is
        # visible when the instance dead-letters mid-workflow.
        self.engine.define("mid", {"steps": [
            {"name": "a", "compensation": "undo-a"},
            {"name": "b", "compensation": "undo-b",
             "await": {"event": "go", "timeoutMs": 1}},
        ]})

    def tearDown(self) -> None:
        self.engine.close()

    def _dead(self, event_id: str = "t-1", advance_id: str = "e-0") -> str:
        iid = self.engine.start("mid", {"k": "v"})["id"]
        self.engine.advance(iid, "succeeded", event_id=advance_id)  # a -> b (waiting)
        time.sleep(0.02)  # let the 1ms deadline pass
        self.engine.advance(iid, "timed_out", {"why": "late"}, event_id=event_id)
        return iid

    def test_recover_re_arms_wait_bumps_revision_and_writes_no_history(self) -> None:
        iid = self._dead()
        dead = self.engine.get(iid)
        self.assertEqual(dead["state"]["status"], "dead_lettered")
        self.assertEqual(dead.revision, 3)
        t0 = int(time.time() * 1000)
        recovered = self.engine.recover(iid)
        state = recovered["state"]
        self.assertEqual(recovered["workflowVersion"], dead["workflowVersion"])
        self.assertEqual(state["status"], "running")
        self.assertIsNone(state["failure"])
        self.assertEqual(state["attempt"], 1)
        self.assertEqual((state["step"], state["index"]), ("b", 1))
        self.assertEqual(state["completed"], ["a"])
        self.assertEqual(state["context"], {"k": "v"})
        self.assertEqual(state["waitingFor"], "go")
        self.assertIsInstance(state["deadlineAt"], int)
        self.assertGreaterEqual(state["deadlineAt"], t0 + 1)
        self.assertEqual(recovered.revision, 4)
        self.assertEqual(self.engine.get(iid).revision, 4)
        # No ledger row and no audit record for the recovery.
        self.assertEqual(
            self.engine._db.execute(
                "SELECT COUNT(*) FROM instance_events WHERE instance_id = ?", (iid,)
            ).fetchone()[0],
            2,
        )
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["eventId"] for h in history], ["e-0", "t-1"])
        self.assertEqual(history[-1]["response"]["state"]["status"], "dead_lettered")
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
        )

    def test_recover_deadline_uses_injected_now(self) -> None:
        iid = self._dead()
        recovered = self.engine.recover(iid, now_ms=100_000)
        self.assertEqual(recovered["state"]["deadlineAt"], 100_001)

    def test_recovered_instance_continues_with_signal_then_normal_events(self) -> None:
        iid = self._dead()
        # Recover against an injected clock so the fresh 1ms deadline is still in
        # the future when the immediate timed_out is attempted.
        self.engine.recover(iid, now_ms=100_000)
        # While re-waiting, ordinary events and an early timed_out stay refused.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded")
        with self.assertRaises(InvalidTransition):
            apply_outcome(self.engine.workflow("mid"),
                          self.engine.get(iid)["state"], "timed_out", now_ms=100_000)
        result = self.engine.signal(iid, "go", event_id="s-1")  # b is the last step
        self.assertEqual(result["state"]["status"], "completed")
        self.assertEqual(result["state"]["completed"], ["a", "b"])
        # The audit chain simply continues; the recovery inserted nothing.
        history = self.engine.audit(iid)["history"]
        self.assertEqual([h["seq"] for h in history], [1, 2, 3])
        self.assertEqual([h["kind"] for h in history], ["event", "event", "signal"])

    def test_can_dead_letter_recover_then_dead_letter_and_recover_again(self) -> None:
        iid = self._dead()  # revision 3
        first = self.engine.recover(iid)
        self.assertEqual(first.revision, 4)
        time.sleep(0.02)
        again_dead = self.engine.advance(iid, "timed_out", {"why": "late-2"}, event_id="t-2")
        self.assertEqual(again_dead["state"]["status"], "dead_lettered")
        self.assertEqual(again_dead.revision, 5)
        second = self.engine.recover(iid)
        self.assertEqual(second["state"]["status"], "running")
        self.assertEqual(second["state"]["waitingFor"], "go")
        self.assertEqual(second.revision, 6)
        self.assertEqual(self.engine.get(iid).revision, 6)
        self.assertEqual([h["eventId"] for h in self.engine.audit(iid)["history"]],
                         ["e-0", "t-1", "t-2"])

    def test_recover_non_dead_states_is_409_without_a_second_change(self) -> None:
        running = self.engine.start("mid", {})["id"]
        with self.assertRaises(InvalidTransition):
            self.engine.recover(running)
        self.assertEqual(self.engine.get(running).revision, 1)
        completed = self.engine.start("order", {})["id"]
        for _ in range(3):
            self.engine.advance(completed, "succeeded")
        with self.assertRaises(InvalidTransition):
            self.engine.recover(completed)
        self.assertEqual(self.engine.get(completed)["state"]["status"], "completed")
        compensated = self.engine.start("order", {})["id"]
        self.engine.advance(compensated, "failed")
        with self.assertRaises(InvalidTransition):
            self.engine.recover(compensated)
        # A recovered instance refuses a second recovery and changes nothing.
        iid = self._dead()
        self.engine.recover(iid)
        snapshot = self.engine.get(iid)
        with self.assertRaises(InvalidTransition):
            self.engine.recover(iid)
        current = self.engine.get(iid)
        self.assertEqual(current["state"], snapshot["state"])
        self.assertEqual(current.revision, snapshot.revision)

    def test_recover_unknown_instance_is_404(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.recover("missing")

    def test_recover_if_match_matching_and_stale(self) -> None:
        iid = self._dead()  # revision 3
        with self.assertRaises(InvalidTransition):
            self.engine.recover(iid, if_match=2)
        self.assertEqual(self.engine.get(iid)["state"]["status"], "dead_lettered")
        self.assertEqual(self.engine.get(iid).revision, 3)
        recovered = self.engine.recover(iid, if_match=3)
        self.assertEqual(recovered.revision, 4)

    def test_timed_out_replay_after_recover_returns_historical_response(self) -> None:
        iid = self._dead()
        first = self.engine.advance(iid, "timed_out", {"why": "late"}, event_id="t-1")
        self.assertEqual(first["state"]["status"], "dead_lettered")
        self.engine.recover(iid)
        # The historical ledger response is still replayed verbatim and does not
        # move the now-running instance.
        replay = self.engine.advance(iid, "timed_out", {"why": "late"}, event_id="t-1")
        self.assertEqual(replay, first)
        self.assertEqual(replay["state"]["status"], "dead_lettered")
        self.assertEqual(self.engine.get(iid)["state"]["status"], "running")
        # A different payload for the same eventId is still a conflict.
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "timed_out", {"why": "other"}, event_id="t-1")

    def test_recover_preserves_compensated_list_and_does_not_compensate(self) -> None:
        iid = self._dead()
        # A dead-lettered row happens to carry a compensated list; recovery must
        # leave it exactly as persisted and must not run or rebuild anything.
        raw = self.engine._db.execute("SELECT state FROM instances WHERE id = ?", (iid,)).fetchone()[0]
        state = json.loads(raw)
        state["compensated"] = ["undo-b", "undo-a"]
        self.engine._db.execute("UPDATE instances SET state = ? WHERE id = ?",
                                (json.dumps(state), iid))
        self.engine._db.commit()
        recovered = self.engine.recover(iid)
        self.assertEqual(recovered["state"]["compensated"], ["undo-b", "undo-a"])
        self.assertEqual(recovered["state"]["completed"], ["a"])

    def test_recover_does_not_rebuild_partition_cursor(self) -> None:
        iid = self._dead()
        # Replay the dead-letter as an ordered call? No: the dead-letter event was
        # unordered. Open a partition by re-driving via an ordered retry is not
        # possible, so instead order the signal after recovery: the cursor starts
        # fresh at 1 and recovery must not have fabricated a row for it.
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
        )
        self.engine.recover(iid)
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM partition_cursors").fetchone()[0], 0
        )
        result = self.engine.signal(iid, "go", event_id="s-1", partition_key="p", sequence=1)
        self.assertEqual(result["ordering"], {"partitionKey": "p", "sequence": 1})
        # A gap is still refused: the cursor semantics are untouched.
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "go", event_id="s-2", partition_key="p", sequence=3)

    def test_legacy_null_version_instance_backfills_on_recover(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            legacy = sqlite3.connect(path)
            legacy.execute(
                "CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)"
            )
            # A dead-lettered built-in order instance at its first (plain) step.
            state = dict(initial_state(WORKFLOWS["order"], {"old": True}))
            state["status"] = "dead_lettered"
            state["failure"] = {"step": "reserve-stock", "reason": "timeout", "detail": None}
            state["waitingFor"] = None
            state["deadlineAt"] = None
            legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                           ("legacy-id", "order", json.dumps(state)))
            legacy.commit()
            legacy.close()
            engine = Engine(path)
            try:
                self.assertIsNone(engine.get("legacy-id")["workflowVersion"])
                recovered = engine.recover("legacy-id")
                self.assertEqual(recovered["workflowVersion"], 1)  # backfilled like an advance
                self.assertEqual(recovered["state"]["status"], "running")
                self.assertIsNone(recovered["state"]["waitingFor"])
                self.assertIsNone(recovered["state"]["deadlineAt"])
                self.assertEqual(recovered["state"]["context"], {"old": True})
                self.assertEqual(recovered.revision, 2)
                stored = engine._db.execute(
                    "SELECT version, instance_version FROM instances WHERE id = ?", ("legacy-id",)
                ).fetchone()
                self.assertEqual(stored, (1, 2))
                # The recovered legacy instance advances normally afterwards.
                advanced = engine.advance("legacy-id", "succeeded", event_id="e-1")
                self.assertEqual(advanced["state"]["completed"], ["reserve-stock"])
            finally:
                engine.close()
        finally:
            os.unlink(path)


class RecoverPersistenceTests(unittest.TestCase):
    """Recovery and the state it writes survive reopen; legacy rows stay readable."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    @staticmethod
    def _define(engine: Engine) -> None:
        engine.define("mid", {"steps": [
            {"name": "a", "compensation": "undo-a"},
            {"name": "b", "await": {"event": "go", "timeoutMs": 1}},
        ]})

    def _dead(self, engine: Engine) -> str:
        iid = engine.start("mid", {"k": "v"})["id"]
        engine.advance(iid, "succeeded", event_id="e-0")
        time.sleep(0.02)
        engine.advance(iid, "timed_out", {"why": "late"}, event_id="t-1")
        return iid

    def test_recover_and_follow_up_changes_consistent_across_reopen(self) -> None:
        engine = Engine(self.path)
        self._define(engine)
        iid = self._dead(engine)
        first_deadline = engine.get(iid)["state"]["deadlineAt"]
        self.assertIsNone(first_deadline)
        recovered = engine.recover(iid)
        deadline = recovered["state"]["deadlineAt"]
        self.assertIsInstance(deadline, int)
        engine.close()

        engine = Engine(self.path)
        try:
            self._define(engine)
            current = engine.get(iid)
            self.assertEqual(current["state"]["status"], "running")
            self.assertEqual(current["state"]["deadlineAt"], deadline)
            self.assertEqual(current["state"]["failure"], None)
            self.assertEqual(current["state"]["attempt"], 1)
            self.assertEqual(current.revision, 4)
            # The accepted history is unchanged by the recovery.
            history = engine.audit(iid)["history"]
            self.assertEqual([h["eventId"] for h in history], ["e-0", "t-1"])
            # Historical replay still works; a new matching signal finishes it.
            replayed = engine.advance(iid, "timed_out", {"why": "late"}, event_id="t-1")
            self.assertEqual(replayed["state"]["status"], "dead_lettered")
            time.sleep(0.02)  # the recovered deadline is 1ms out too
            done = engine.signal(iid, "go", event_id="s-1")
            self.assertEqual(done["state"]["status"], "completed")
        finally:
            engine.close()

        engine = Engine(self.path)
        try:
            self._define(engine)
            self.assertEqual(engine.get(iid)["state"]["status"], "completed")
        finally:
            engine.close()

    def test_legacy_dead_state_missing_wait_fields_recovers(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute(
            "CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)"
        )
        state = initial_state(WORKFLOWS["order"], {})
        state["status"] = "dead_lettered"
        state["failure"] = {"step": "reserve-stock", "reason": "timeout", "detail": None}
        del state["waitingFor"]
        del state["deadlineAt"]
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            current = engine.get("legacy-id")["state"]
            self.assertIsNone(current["waitingFor"])
            self.assertIsNone(current["deadlineAt"])
            recovered = engine.recover("legacy-id")
            self.assertEqual(recovered["state"]["status"], "running")
            self.assertIsNone(recovered["state"]["waitingFor"])
            self.assertIsNone(recovered["state"]["deadlineAt"])
            # Back on a plain step: an ordinary event advances it.
            result = engine.advance("legacy-id", "succeeded", event_id="e-1")
            self.assertEqual(result["state"]["completed"], ["reserve-stock"])
        finally:
            engine.close()

    def test_legacy_dead_state_on_awaiting_step_re_arms_with_new_definition(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute(
            "CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)"
        )
        state = {
            "status": "dead_lettered", "step": "ask", "index": 0, "attempt": 4,
            "completed": [], "compensated": [], "context": {},
            "failure": {"step": "ask", "reason": "timeout", "detail": None},
        }  # no waitingFor/deadlineAt fields at all
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "expiring", json.dumps(state)))
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            engine.define("expiring", {"steps": [
                {"name": "ask", "await": {"event": "go", "timeoutMs": 5000}},
            ]})
            t0 = int(time.time() * 1000)
            recovered = engine.recover("legacy-id")
            self.assertEqual(recovered["state"]["status"], "running")
            self.assertEqual(recovered["state"]["attempt"], 1)
            self.assertEqual(recovered["state"]["waitingFor"], "go")
            self.assertIsInstance(recovered["state"]["deadlineAt"], int)
            self.assertGreaterEqual(recovered["state"]["deadlineAt"], t0 + 5000)
        finally:
            engine.close()


class RecoverHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from saga import serve

        cls.server = serve(port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()

    def call(self, method: str, path: str, body=b"", headers: dict | None = None):
        if body == b"":
            data = b""
        else:
            data = json.dumps(body).encode()
        hdrs = {"Content-Type": "application/json"}
        if headers:
            hdrs.update(headers)
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), response.headers
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), error.headers

    def _dead(self) -> str:
        self.call("PUT", "/v1/workflows/rflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go", "timeoutMs": 1}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body, _ = self.call("POST", "/v1/workflows/rflow/instances", {"context": {"k": "v"}})
        self.assertEqual(status, 201)
        iid = body["id"]
        time.sleep(0.02)
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "timed_out", "detail": {"why": "late"},
                                     "eventId": "t-1"})
        self.assertEqual((status, body["state"]["status"]), (200, "dead_lettered"))
        return iid

    def test_recover_end_to_end(self) -> None:
        iid = self._dead()
        t0 = int(time.time() * 1000)
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/recover", {})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')  # 1 -> 2 (timed_out) -> 3 (recover)
        self.assertEqual(set(body), {"id", "workflow", "workflowVersion", "state"})
        self.assertEqual(body["workflow"], "rflow")
        state = body["state"]
        self.assertEqual(state["status"], "running")
        self.assertIsNone(state["failure"])
        self.assertEqual(state["attempt"], 1)
        self.assertEqual((state["step"], state["index"]), ("s1", 0))
        self.assertEqual(state["completed"], [])
        self.assertEqual(state["compensated"], [])
        self.assertEqual(state["context"], {"k": "v"})
        self.assertEqual(state["waitingFor"], "go")
        self.assertIsInstance(state["deadlineAt"], int)
        self.assertGreaterEqual(state["deadlineAt"], t0 + 1)
        # GET reflects the recovery and carries the new ETag.
        status, current, headers = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual(status, 200)
        self.assertEqual(current["state"], state)
        self.assertEqual(headers.get("ETag"), '"3"')
        # Audit history still contains only the accepted timed_out call.
        status, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual([h["eventId"] for h in audit["history"]], ["t-1"])
        self.assertEqual(audit["history"][0]["response"]["state"]["status"], "dead_lettered")
        # The re-armed wait blocks events but accepts the matching signal, then
        # the plain second step completes via a normal event.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded"})[0], 409)
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/signals", {"event": "go"})
        self.assertEqual(status, 200)
        self.assertEqual(body["state"]["completed"], ["s1"])
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "succeeded"})
        self.assertEqual(body["state"]["status"], "completed")

    def test_recover_rejections_and_no_second_change(self) -> None:
        iid = self._dead()
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/recover", {})[0], 200)
        # Repeated recovery is 409 and carries no ETag.
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/recover", {})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        self.assertIsNone(headers.get("ETag"))
        # A running instance and a completed instance are 409 too.
        running = self.call("POST", "/v1/workflows/order/instances", {})[1]["id"]
        self.assertEqual(self.call("POST", f"/v1/instances/{running}/recover", {})[0], 409)
        done = self.call("POST", "/v1/workflows/order/instances", {})[1]["id"]
        for _ in range(3):
            self.call("POST", f"/v1/instances/{done}/events", {"outcome": "succeeded"})
        self.assertEqual(self.call("POST", f"/v1/instances/{done}/recover", {})[0], 409)
        # Unknown instance is 404.
        self.assertEqual(self.call("POST", "/v1/instances/missing/recover", {})[0], 404)
        # The successfully recovered instance kept exactly one revision bump.
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2].get("ETag"), '"3"')

    def test_recover_requires_empty_object(self) -> None:
        iid = self._dead()
        raw_call = lambda raw: self._raw_post(f"/v1/instances/{iid}/recover", raw)
        for raw, label in ((b"", "empty body"), (b"null", "null"), (b"[]", "array"),
                           (b'{"x":1}', "field"), (b'{"detail":null}', "detail"),
                           (b'"{}"', "string"), (b"42", "number"),
                           (b"{not json}", "malformed")):
            status, body, _ = raw_call(raw)
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), label)
        # The rejections changed nothing: still dead_lettered at revision 2.
        status, body, headers = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual(body["state"]["status"], "dead_lettered")
        self.assertEqual(headers.get("ETag"), '"2"')

    def _raw_post(self, path: str, raw: bytes):
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=raw,
                                         method="POST",
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), response.headers
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), error.headers

    def test_recover_if_match_validation_and_precondition(self) -> None:
        iid = self._dead()  # revision 2
        for raw in ("", "1", 'W/"1"', "*", '"01"', '"-1"'):
            status, body, _ = self.call("POST", f"/v1/instances/{iid}/recover", {},
                                        {"If-Match": raw})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), repr(raw))
        # Stale precondition: 409, no change.
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/recover", {},
                                          {"If-Match": '"1"'})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        self.assertIsNone(headers.get("ETag"))
        self.assertEqual(self.call("GET", f"/v1/instances/{iid}")[2].get("ETag"), '"2"')
        # Matching precondition succeeds.
        status, _, headers = self.call("POST", f"/v1/instances/{iid}/recover", {},
                                       {"If-Match": '"2"'})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')

    def test_recover_then_timeout_again_end_to_end(self) -> None:
        # Dedicated workflow with a wider deadline: the initial wait is allowed to
        # expire, yet the deadline re-armed by recovery is still comfortably in the
        # future for an immediate timed_out.
        self.call("PUT", "/v1/workflows/rflow2", {"steps": [
            {"name": "s1", "await": {"event": "go", "timeoutMs": 200}},
            {"name": "s2"},
        ]})
        iid = self.call("POST", "/v1/workflows/rflow2/instances", {})[1]["id"]
        time.sleep(0.25)
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "timed_out", "eventId": "t-1"})
        self.assertEqual((status, body["state"]["status"]), (200, "dead_lettered"))
        status, _, headers = self.call("POST", f"/v1/instances/{iid}/recover", {})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')
        # An immediate timed_out is refused: the fresh deadline has not passed.
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "timed_out"})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        time.sleep(0.25)
        status, body, _ = self.call("POST", f"/v1/instances/{iid}/events",
                                    {"outcome": "timed_out", "eventId": "t-2"})
        self.assertEqual((status, body["state"]["status"]), (200, "dead_lettered"))
        status, body, headers = self.call("POST", f"/v1/instances/{iid}/recover", {})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"5"')
        self.assertEqual(body["state"]["status"], "running")


class ListInstancesEngineTests(unittest.TestCase):
    """Engine-level coverage of the read-only instance listing."""

    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "order") -> str:
        return self.engine.start(workflow, {})["id"]

    def test_empty_store_returns_empty_page_and_null_cursor(self) -> None:
        self.assertEqual(self.engine.list_instances(),
                         {"instances": [], "nextCursor": None})

    def test_instances_sorted_by_id_with_stable_pagination(self) -> None:
        ids = sorted(self._start() for _ in range(5))
        first = self.engine.list_instances(limit="2")
        self.assertEqual([i["id"] for i in first["instances"]], ids[:2])
        self.assertEqual(first["nextCursor"], ids[1])
        second = self.engine.list_instances(after_id=first["nextCursor"], limit="2")
        self.assertEqual([i["id"] for i in second["instances"]], ids[2:4])
        self.assertEqual(second["nextCursor"], ids[3])
        third = self.engine.list_instances(after_id=second["nextCursor"], limit="2")
        self.assertEqual([i["id"] for i in third["instances"]], ids[4:])
        self.assertIsNone(third["nextCursor"])

    def test_page_ending_exactly_at_the_last_match_has_null_cursor(self) -> None:
        ids = sorted(self._start() for _ in range(4))
        page = self.engine.list_instances(limit="4")
        self.assertEqual([i["id"] for i in page["instances"]], ids)
        self.assertIsNone(page["nextCursor"])
        second = self.engine.list_instances(after_id=ids[1], limit="2")
        self.assertEqual([i["id"] for i in second["instances"]], ids[2:])
        self.assertIsNone(second["nextCursor"])

    def test_default_limit_is_50(self) -> None:
        ids = sorted(self._start() for _ in range(3))
        page = self.engine.list_instances()
        self.assertEqual([i["id"] for i in page["instances"]], ids)
        self.assertIsNone(page["nextCursor"])

    def test_filters_by_workflow_and_status(self) -> None:
        self.engine.define("mini", {"steps": [{"name": "a", "compensation": "undo-a"}]})
        running = self._start("order")
        completed = self._start("mini")
        self.engine.advance(completed, "succeeded")
        compensated = self._start("order")
        self.engine.advance(compensated, "failed")

        page = self.engine.list_instances(workflow="order")
        self.assertEqual([i["id"] for i in page["instances"]], sorted([running, compensated]))
        page = self.engine.list_instances(status="completed")
        self.assertEqual([i["id"] for i in page["instances"]], [completed])
        page = self.engine.list_instances(workflow="order", status="compensated")
        self.assertEqual([i["id"] for i in page["instances"]], [compensated])
        page = self.engine.list_instances(workflow="order", status="dead_lettered")
        self.assertEqual(page, {"instances": [], "nextCursor": None})

        # Item shape: exactly the four public fields, state as GET /v1/instances/{id}.
        item = self.engine.list_instances(workflow="mini")["instances"][0]
        self.assertEqual(set(item), {"id", "workflow", "workflowVersion", "state"})
        self.assertEqual(item["workflow"], "mini")
        self.assertEqual(item["workflowVersion"], 1)
        self.assertEqual(item["state"], self.engine.get(completed)["state"])

    def test_after_id_is_a_pure_ordering_boundary(self) -> None:
        ids = sorted(self._start() for _ in range(3))
        page = self.engine.list_instances(after_id=ids[0])
        self.assertEqual([i["id"] for i in page["instances"]], ids[1:])
        # Boundaries that name no instance still split the byte ordering:
        # "-" sorts before every hex digit, "~" after every uuid character.
        page = self.engine.list_instances(after_id="-")
        self.assertEqual([i["id"] for i in page["instances"]], ids)
        page = self.engine.list_instances(after_id="~")
        self.assertEqual(page, {"instances": [], "nextCursor": None})

    def test_invalid_query_shapes_are_400(self) -> None:
        self._start()
        bad_calls = [
            {"workflow": ""}, {"workflow": "x" * 101},
            {"status": ""}, {"status": "Running"}, {"status": "dead"}, {"status": "done"},
            {"after_id": ""}, {"after_id": "x" * 201},
            {"limit": ""}, {"limit": "0"}, {"limit": "01"}, {"limit": "007"},
            {"limit": "1.5"}, {"limit": "abc"}, {"limit": "-1"}, {"limit": "+1"},
            {"limit": " 1"}, {"limit": "1 "}, {"limit": "101"}, {"limit": "100000"},
        ]
        for kwargs in bad_calls:
            with self.assertRaises(InvalidRequest, msg=repr(kwargs)):
                self.engine.list_instances(**kwargs)
        # Boundary values are accepted.
        self.engine.list_instances(workflow="x" * 100, after_id="x" * 200, limit="1")
        self.engine.list_instances(limit="100")

    def test_legacy_instance_lists_with_null_version_and_backfilled_state(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            legacy = sqlite3.connect(path)
            legacy.execute(
                "CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)"
            )
            state = initial_state(WORKFLOWS["order"], {})
            del state["waitingFor"]
            del state["deadlineAt"]
            legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                           ("legacy-id", "order", json.dumps(state)))
            legacy.commit()
            legacy.close()

            engine = Engine(path)
            try:
                page = engine.list_instances()
                self.assertIsNone(page["nextCursor"])
                item = page["instances"][0]
                self.assertEqual(item["id"], "legacy-id")
                self.assertIsNone(item["workflowVersion"])
                self.assertIsNone(item["state"]["waitingFor"])
                self.assertIsNone(item["state"]["deadlineAt"])
            finally:
                engine.close()
        finally:
            os.unlink(path)


class ListInstancesHttpTests(unittest.TestCase):
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

    def test_list_filters_and_paginates_end_to_end(self) -> None:
        self.call("PUT", "/v1/workflows/lw", {"steps": [
            {"name": "a", "compensation": "undo-a"},
            {"name": "b", "compensation": "undo-b"},
        ]})
        created = [self.call("POST", "/v1/workflows/lw/instances", {})[1]["id"] for _ in range(3)]
        ids = sorted(created)
        # One instance compensated, the rest still running.
        self.call("POST", f"/v1/instances/{ids[0]}/events", {"outcome": "failed"})

        status, body = self.call("GET", "/v1/instances?workflow=lw")
        self.assertEqual(status, 200)
        self.assertEqual([i["id"] for i in body["instances"]], ids)
        self.assertIsNone(body["nextCursor"])
        item = body["instances"][0]
        self.assertEqual(set(item), {"id", "workflow", "workflowVersion", "state"})
        self.assertEqual(item["workflowVersion"], 1)
        self.assertEqual(item["state"]["status"], "compensated")

        status, page1 = self.call("GET", "/v1/instances?workflow=lw&limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([i["id"] for i in page1["instances"]], ids[:2])
        self.assertEqual(page1["nextCursor"], ids[1])
        status, page2 = self.call(
            "GET", f"/v1/instances?workflow=lw&limit=2&afterId={page1['nextCursor']}")
        self.assertEqual([i["id"] for i in page2["instances"]], ids[2:])
        self.assertIsNone(page2["nextCursor"])

        status, body = self.call("GET", "/v1/instances?workflow=lw&status=running")
        self.assertEqual([i["id"] for i in body["instances"]], ids[1:])
        status, body = self.call("GET", "/v1/instances?workflow=lw&status=compensated")
        self.assertEqual([i["id"] for i in body["instances"]], ids[:1])
        status, body = self.call("GET", "/v1/instances?workflow=lw&status=dead_lettered")
        self.assertEqual((body["instances"], body["nextCursor"]), ([], None))
        # afterId is a boundary, not a lookup: an unknown id value is fine.
        status, body = self.call("GET", "/v1/instances?workflow=lw&afterId=~")
        self.assertEqual((status, body["instances"], body["nextCursor"]), (200, [], None))

    def test_list_query_validation_errors_are_400(self) -> None:
        bad_paths = [
            "/v1/instances?workflow=",
            "/v1/instances?workflow=" + "x" * 101,
            "/v1/instances?status=",
            "/v1/instances?status=Running",
            "/v1/instances?status=dead",
            "/v1/instances?afterId=",
            "/v1/instances?afterId=" + "x" * 201,
            "/v1/instances?limit=",
            "/v1/instances?limit=0",
            "/v1/instances?limit=01",
            "/v1/instances?limit=1.5",
            "/v1/instances?limit=abc",
            "/v1/instances?limit=-1",
            "/v1/instances?limit=101",
            "/v1/instances?limit=1&limit=2",
            "/v1/instances?workflow=a&workflow=a",
            "/v1/instances?status=running&status=running",
            "/v1/instances?bogus=1",
            "/v1/instances?limit=50&bogus=1",
        ]
        for path in bad_paths:
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request", path)

    def test_list_accepts_boundary_values_and_ignores_nothing_existing(self) -> None:
        status, body = self.call("GET", "/v1/instances?limit=1")
        self.assertEqual(status, 200)
        self.assertLessEqual(len(body["instances"]), 1)
        status, body = self.call("GET", "/v1/instances?limit=100&workflow=no-such-workflow")
        self.assertEqual((status, body), (200, {"instances": [], "nextCursor": None}))
        # Unknown deeper paths are still 404.
        self.assertEqual(self.call("GET", "/v1/instances/x/y/z")[0], 404)


class MigrationPlanEngineTests(unittest.TestCase):
    """Read-only migration preview: plan content, reason ordering, no side effects."""

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

    def test_compatible_plan_reports_both_definitions(self) -> None:
        self._v2()
        iid = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(iid, "approved", event_id="s-1")  # now at index 1 ("do")
        plan = self.engine.migration_plan(iid, "2")
        self.assertEqual(plan["id"], iid)
        self.assertEqual(plan["workflow"], "flow")
        self.assertEqual((plan["currentVersion"], plan["targetVersion"]), (1, 2))
        self.assertEqual(plan["currentIndex"], 1)
        self.assertEqual(plan["state"], self.engine.get(iid)["state"])
        self.assertEqual(plan["currentSteps"], self.engine.workflow("flow", 1)["steps"])
        self.assertEqual(plan["targetSteps"], self.engine.workflow("flow", 2)["steps"])
        self.assertEqual(plan["targetSteps"][2]["compensation"], "abort-ship")
        self.assertIs(plan["compatible"], True)
        self.assertIsNone(plan["reason"])
        # The plan carries the same revision the instance query would report.
        self.assertEqual(plan.revision, self.engine.get(iid).revision)

    def test_plan_is_read_only_and_repeatable(self) -> None:
        self._v2()
        iid = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(iid, "approved", event_id="s-1")
        before = self.engine.get(iid)
        history_before = self.engine.audit(iid)["history"]
        first = self.engine.migration_plan(iid, "2")
        second = self.engine.migration_plan(iid, "2")
        self.assertEqual(dict(first), dict(second))
        after = self.engine.get(iid)
        # Nothing advanced: revision, pinned version, state and audit are untouched.
        self.assertEqual(after.revision, before.revision)
        self.assertEqual(after["workflowVersion"], 1)
        self.assertEqual(after["state"], before["state"])
        self.assertEqual(self.engine.audit(iid)["history"], history_before)
        # The ledger still replays the first signal response verbatim.
        replay = self.engine.signal(iid, "approved", event_id="s-1")
        self.assertEqual(replay["state"]["index"], 1)

    def test_reason_ordering_and_each_block(self) -> None:
        iid = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(iid, "approved")  # index 1
        # Different step count.
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}]})
        plan = self.engine.migration_plan(iid, "2")
        self.assertEqual((plan["compatible"], plan["reason"]), (False, "step_count_changed"))
        # Same count, renamed step.
        self.engine.define("flow", {"steps": [{"name": "ask"}, {"name": "do"}, {"name": "deliver"}]})
        plan = self.engine.migration_plan(iid, "3")
        self.assertEqual(plan["reason"], "step_name_changed")
        # Entered step (index 0, "ask") changed its await.
        self.engine.define("flow", {"steps": [
            {"name": "ask", "compensation": "cancel-ask", "await": {"event": "ok"}},
            {"name": "do", "compensation": "undo-do"},
            {"name": "ship", "compensation": "cancel-ship"},
        ]})
        plan = self.engine.migration_plan(iid, "4")
        self.assertEqual(plan["reason"], "entered_step_changed")
        # Only a not-yet-entered step differs: compatible.
        self._v2()  # v5
        plan = self.engine.migration_plan(iid, "5")
        self.assertEqual((plan["compatible"], plan["reason"]), (True, None))
        # Terminal instance: instance_not_running wins over any step difference.
        done = self.engine.start("flow", {}, version=1)["id"]
        self.engine.signal(done, "approved")
        self.engine.advance(done, "succeeded")
        self.engine.advance(done, "succeeded")
        self.assertEqual(self.engine.get(done)["state"]["status"], "completed")
        plan = self.engine.migration_plan(done, "2")  # v2 also has a different step count
        self.assertEqual((plan["compatible"], plan["reason"]), (False, "instance_not_running"))

    def test_legacy_instance_without_recorded_version(self) -> None:
        state = initial_state(self.engine.workflow("flow", 1), {})
        self.engine._db.execute(
            "INSERT INTO instances (id, workflow, state, version) VALUES (?, ?, ?, NULL)",
            ("legacy", "flow", json.dumps(state)),
        )
        self.engine._db.commit()
        self._v2()
        plan = self.engine.migration_plan("legacy", "2")
        self.assertIsNone(plan["currentVersion"])
        self.assertIsNone(plan["currentSteps"])
        self.assertEqual(plan["targetSteps"], self.engine.workflow("flow", 2)["steps"])
        self.assertEqual((plan["compatible"], plan["reason"]), (False, "missing_recorded_version"))
        # A terminal legacy instance reports instance_not_running first.
        state["status"] = "completed"
        self.engine._db.execute("UPDATE instances SET state = ? WHERE id = ?",
                                (json.dumps(state), "legacy"))
        self.engine._db.commit()
        plan = self.engine.migration_plan("legacy", "2")
        self.assertEqual(plan["reason"], "instance_not_running")

    def test_validation_precedes_lookup_and_not_found(self) -> None:
        iid = self.engine.start("flow", {}, version=1)["id"]
        # 400: missing / empty / non-canonical / non-string raw values, even for
        # an instance that does not exist (validation comes first).
        for bad in (None, "", "0", "01", "1.5", "abc", "-1", "+1", " 1", 2, True):
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.migration_plan(iid, bad)
            with self.assertRaises(InvalidRequest, msg=repr(bad)):
                self.engine.migration_plan("missing", bad)
        # 404: unknown instance, unknown target version.
        with self.assertRaises(InstanceNotFound):
            self.engine.migration_plan("missing", "1")
        with self.assertRaises(NotFound):
            self.engine.migration_plan(iid, "99")


class MigrationPlanHttpTests(unittest.TestCase):
    """The migration-plan endpoint over HTTP: query shape, ETag, statuses."""

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
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}"), response.headers
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), error.headers

    def _workflow_with_two_versions(self) -> None:
        self.call("PUT", "/v1/workflows/pflow", {"steps": [
            {"name": "s1", "compensation": "c1"},
            {"name": "s2", "compensation": "c2"},
        ]})
        self.call("PUT", "/v1/workflows/pflow", {"steps": [
            {"name": "s1", "compensation": "c1"},
            {"name": "s2", "compensation": "c2-new"},
        ]})

    def test_plan_happy_path_and_etag(self) -> None:
        self._workflow_with_two_versions()
        status, start, _ = self.call("POST", "/v1/workflows/pflow/instances", {"version": 1})
        self.assertEqual(status, 201)
        iid = start["id"]
        status, plan, headers = self.call(
            "GET", f"/v1/instances/{iid}/migration-plan?targetVersion=2")
        self.assertEqual(status, 200)
        self.assertEqual(
            {k: plan[k] for k in ("id", "workflow", "currentVersion", "targetVersion",
                                   "currentIndex", "compatible", "reason")},
            {"id": iid, "workflow": "pflow", "currentVersion": 1, "targetVersion": 2,
             "currentIndex": 0, "compatible": True, "reason": None},
        )
        self.assertEqual(plan["state"], start["state"])
        self.assertEqual([s["name"] for s in plan["currentSteps"]], ["s1", "s2"])
        self.assertEqual(plan["targetSteps"][1]["compensation"], "c2-new")
        # Same ETag as the instance query, and the plan query advances nothing.
        _, _, get_headers = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual(headers.get("ETag"), '"1"')
        self.assertEqual(get_headers.get("ETag"), '"1"')
        _, _, again = self.call("GET", f"/v1/instances/{iid}/migration-plan?targetVersion=2")
        self.assertEqual(again.get("ETag"), '"1"')

    def test_incompatible_and_terminal_are_200_with_reason(self) -> None:
        self._workflow_with_two_versions()
        self.call("PUT", "/v1/workflows/pflow", {"steps": [
            {"name": "s1", "compensation": "c1"},
            {"name": "renamed", "compensation": "c2"},
        ]})
        iid = self.call("POST", "/v1/workflows/pflow/instances", {"version": 1})[1]["id"]
        status, plan, _ = self.call("GET", f"/v1/instances/{iid}/migration-plan?targetVersion=3")
        self.assertEqual(status, 200)
        self.assertEqual((plan["compatible"], plan["reason"]), (False, "step_name_changed"))
        # Drive the instance to completion: still 200, now instance_not_running.
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded"})
        status, plan, headers = self.call(
            "GET", f"/v1/instances/{iid}/migration-plan?targetVersion=2")
        self.assertEqual(status, 200)
        self.assertEqual((plan["compatible"], plan["reason"]), (False, "instance_not_running"))
        self.assertEqual(headers.get("ETag"), '"3"')  # two accepted events, plan added nothing

    def test_query_validation_and_not_found(self) -> None:
        self._workflow_with_two_versions()
        iid = self.call("POST", "/v1/workflows/pflow/instances", {"version": 1})[1]["id"]
        bad_paths = [
            f"/v1/instances/{iid}/migration-plan",
            f"/v1/instances/{iid}/migration-plan?targetVersion=",
            f"/v1/instances/{iid}/migration-plan?targetVersion=0",
            f"/v1/instances/{iid}/migration-plan?targetVersion=01",
            f"/v1/instances/{iid}/migration-plan?targetVersion=1.5",
            f"/v1/instances/{iid}/migration-plan?targetVersion=abc",
            f"/v1/instances/{iid}/migration-plan?targetVersion=-1",
            f"/v1/instances/{iid}/migration-plan?targetVersion=2&targetVersion=2",
            f"/v1/instances/{iid}/migration-plan?bogus=1",
            f"/v1/instances/{iid}/migration-plan?targetVersion=2&bogus=1",
        ]
        for path in bad_paths:
            status, body, _ = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request", path)
        # Validation precedes the instance lookup: 400 even for a missing instance.
        status, _, _ = self.call("GET", "/v1/instances/missing/migration-plan?targetVersion=01")
        self.assertEqual(status, 400)
        # Unknown instance and unknown target version are 404.
        self.assertEqual(
            self.call("GET", "/v1/instances/missing/migration-plan?targetVersion=2")[0], 404)
        self.assertEqual(
            self.call("GET", f"/v1/instances/{iid}/migration-plan?targetVersion=99")[0], 404)
        # The rejections wrote nothing: the instance is still at revision 1.
        _, _, headers = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual(headers.get("ETag"), '"1"')


class TimelineEngineTests(unittest.TestCase):
    """Engine-level coverage of the read-only timeline projection."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _history(self) -> str:
        """Three accepted calls: signal, event, signal -> completed instance."""
        iid = self.engine.start("approval", {"customer": "c1"})["id"]
        self.engine.signal(iid, "approved", event_id="s-1")          # seq 1 signal
        self.engine.advance(iid, "succeeded", event_id="e-2")        # seq 2 event
        self.engine.signal(iid, "shipped", event_id="s-3")           # seq 3 signal
        return iid

    def test_empty_instance_returns_empty_timeline_and_null_next_seq(self) -> None:
        iid = self.engine.start("approval", {})["id"]
        page = self.engine.timeline(iid)
        self.assertEqual(page["id"], iid)
        self.assertEqual(page["workflow"], "approval")
        self.assertEqual(page["workflowVersion"], 1)
        self.assertEqual(page["state"]["status"], "running")
        self.assertEqual(page["timeline"], [])
        self.assertIsNone(page["nextSeq"])

    def test_items_project_response_state_without_context_or_response(self) -> None:
        iid = self._history()
        page = self.engine.timeline(iid)
        self.assertEqual([item["seq"] for item in page["timeline"]], [1, 2, 3])
        self.assertEqual([item["kind"] for item in page["timeline"]],
                         ["signal", "event", "signal"])
        self.assertEqual([item["eventId"] for item in page["timeline"]],
                         ["s-1", "e-2", "s-3"])
        first = page["timeline"][0]
        self.assertEqual(
            list(first),
            ["seq", "kind", "eventId", "request", "status", "step", "index",
             "attempt", "completed", "compensated", "waitingFor", "deadlineAt",
             "failure"],
        )
        # The item is the audit record's state snapshot at that seq.
        self.assertEqual(first["status"], "running")
        self.assertEqual(first["step"], "do")
        self.assertEqual(first["index"], 1)
        self.assertEqual(first["attempt"], 1)
        self.assertEqual(first["completed"], ["ask"])
        self.assertEqual(first["compensated"], [])
        self.assertIsNone(first["waitingFor"])
        self.assertIsNone(first["deadlineAt"])
        self.assertIsNone(first["failure"])
        self.assertEqual(first["request"],
                         {"eventId": "s-1", "event": "approved", "detail": None})
        # context and the full response are never projected.
        for item in page["timeline"]:
            self.assertNotIn("context", item)
            self.assertNotIn("response", item)
        last = page["timeline"][-1]
        self.assertEqual(last["status"], "completed")
        self.assertIsNone(last["step"])
        self.assertEqual(last["completed"], ["ask", "do", "ship"])
        self.assertIsNone(page["nextSeq"])

    def test_items_match_audit_records_one_to_one(self) -> None:
        iid = self._history()
        audit = self.engine.audit(iid)
        timeline = self.engine.timeline(iid)["timeline"]
        self.assertEqual([h["seq"] for h in audit["history"]],
                         [item["seq"] for item in timeline])
        for record, item in zip(audit["history"], timeline):
            self.assertEqual(item["kind"], record["kind"])
            self.assertEqual(item["eventId"], record["eventId"])
            self.assertEqual(item["request"], record["request"])
            state = record["response"]["state"]
            for key in ("status", "step", "index", "attempt", "completed",
                        "compensated", "waitingFor", "deadlineAt", "failure"):
                self.assertEqual(item[key], state[key], key)

    def test_pagination_cursor_and_bounds(self) -> None:
        iid = self._history()
        page1 = self.engine.timeline(iid, limit="2")
        self.assertEqual([item["seq"] for item in page1["timeline"]], [1, 2])
        self.assertEqual(page1["nextSeq"], 2)
        page2 = self.engine.timeline(iid, limit="2", after_seq=str(page1["nextSeq"]))
        self.assertEqual([item["seq"] for item in page2["timeline"]], [3])
        self.assertIsNone(page2["nextSeq"])
        # A page ending exactly at the last record has a null cursor.
        page = self.engine.timeline(iid, limit="3")
        self.assertEqual(len(page["timeline"]), 3)
        self.assertIsNone(page["nextSeq"])
        # Beyond the end: empty page, null cursor.
        empty = self.engine.timeline(iid, after_seq="99")
        self.assertEqual(empty["timeline"], [])
        self.assertIsNone(empty["nextSeq"])
        # The timeline-specific bound is 1..200 (audit allows only 1..100).
        self.assertEqual(len(self.engine.timeline(iid, limit="200")["timeline"]), 3)

    def test_default_limit_is_50(self) -> None:
        # A retrying step accumulates one audit record per accepted failure.
        self.engine.define("flaky", {"steps": [
            {"name": "try", "compensation": "undo", "retry": {"maxAttempts": 10}},
        ]})
        iid = self.engine.start("flaky", {})["id"]
        for n in range(9):
            self.engine.advance(iid, "failed", event_id=f"f-{n}")
        page = self.engine.timeline(iid, after_seq="0")
        self.assertEqual(len(page["timeline"]), 9)
        self.assertIsNone(page["nextSeq"])

    def test_anonymous_calls_project_null_event_id(self) -> None:
        iid = self.engine.start("order", {})["id"]
        self.engine.advance(iid, "succeeded")
        item = self.engine.timeline(iid)["timeline"][0]
        self.assertIsNone(item["eventId"])
        self.assertEqual(item["request"],
                         {"eventId": None, "outcome": "succeeded", "detail": None})

    def test_invalid_params_raise_before_instance_lookup(self) -> None:
        bad_calls = [
            {"limit": "0"}, {"limit": "201"}, {"limit": "01"}, {"limit": ""},
            {"limit": "1.5"}, {"limit": "true"}, {"limit": "abc"}, {"limit": "-1"},
            {"after_seq": "-1"}, {"after_seq": "00"}, {"after_seq": "1.0"},
            {"after_seq": ""}, {"after_seq": "x"}, {"after_seq": "true"},
        ]
        for params in bad_calls:
            with self.subTest(params=params):
                with self.assertRaises(InvalidRequest):
                    self.engine.timeline("no-such-instance", **params)

    def test_valid_params_on_missing_instance_raise_not_found(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.timeline("no-such-instance")
        with self.assertRaises(InstanceNotFound):
            self.engine.timeline("no-such-instance", limit="10", after_seq="0")

    def test_query_is_read_only(self) -> None:
        iid = self._history()
        before = self.engine.audit(iid)
        before_revision = before.revision
        self.engine.timeline(iid, limit="1")
        self.engine.timeline(iid, after_seq="2")
        after = self.engine.audit(iid)
        self.assertEqual(before, after)
        self.assertEqual(after.revision, before_revision)
        self.assertEqual(self.engine.get(iid)["state"]["status"], "completed")


class TimelinePersistenceTests(unittest.TestCase):
    """Timeline projection survives reopen; legacy records get read-time defaults."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_timeline_consistent_across_reopen(self) -> None:
        engine = Engine(self.path)
        engine.define("approval", _await_workflow())
        iid = engine.start("approval", {})["id"]
        engine.signal(iid, "approved", event_id="s-1")
        engine.advance(iid, "succeeded", event_id="e-1")
        engine.close()

        engine = Engine(self.path)
        try:
            page = engine.timeline(iid)
            self.assertEqual([item["seq"] for item in page["timeline"]], [1, 2])
            self.assertEqual(page["state"]["waitingFor"], "shipped")
            # New accepted calls extend the same sequence after reopen.
            engine.signal(iid, "shipped", event_id="s-2")
            page = engine.timeline(iid, after_seq="2")
            self.assertEqual([item["seq"] for item in page["timeline"]], [3])
            self.assertEqual(page["timeline"][0]["status"], "completed")
            self.assertIsNone(page["nextSeq"])
        finally:
            engine.close()

    def test_legacy_records_project_defaults_without_rewriting_history(self) -> None:
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
        legacy.execute(
            "CREATE TABLE instance_audit (instance_id TEXT NOT NULL, seq INTEGER NOT NULL, "
            "kind TEXT NOT NULL, event_id TEXT, request TEXT NOT NULL, response TEXT NOT NULL, "
            "PRIMARY KEY (instance_id, seq))"
        )
        state = initial_state(WORKFLOWS["order"], {})
        # A pre-attempt/pre-wait snapshot: no attempt, waitingFor or deadlineAt.
        old_state = {"status": "running", "step": "charge-payment", "index": 1,
                     "completed": ["reserve-stock"], "compensated": [],
                     "context": {"k": "v"}, "failure": None}
        legacy.execute("INSERT INTO instances VALUES (?, ?, ?)",
                       ("legacy-id", "order", json.dumps(state)))
        legacy.execute(
            "INSERT INTO instance_audit VALUES (?, ?, ?, ?, ?, ?)",
            ("legacy-id", 1, "event", "e-1",
             json.dumps({"eventId": "e-1", "outcome": "succeeded", "detail": None}),
             json.dumps({"id": "legacy-id", "workflow": "order",
                         "workflowVersion": None, "state": old_state})),
        )
        legacy.commit()
        legacy.close()

        engine = Engine(self.path)
        try:
            page = engine.timeline("legacy-id")
            item = page["timeline"][0]
            # Read-time defaults: attempt 1, waitingFor/deadlineAt null.
            self.assertEqual(item["attempt"], 1)
            self.assertIsNone(item["waitingFor"])
            self.assertIsNone(item["deadlineAt"])
            self.assertEqual(item["step"], "charge-payment")
            self.assertNotIn("context", item)
            # The stored record is not supplemented: audit returns it verbatim.
            record = engine.audit("legacy-id")["history"][0]
            self.assertNotIn("attempt", record["response"]["state"])
            self.assertNotIn("waitingFor", record["response"]["state"])
            self.assertNotIn("deadlineAt", record["response"]["state"])
        finally:
            engine.close()
        # And the file itself was not rewritten either.
        check = sqlite3.connect(self.path)
        stored = json.loads(check.execute(
            "SELECT response FROM instance_audit WHERE instance_id = 'legacy-id'"
        ).fetchone()[0])
        check.close()
        self.assertNotIn("attempt", stored["state"])


class TimelineHttpTests(unittest.TestCase):
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
                return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), dict(error.headers)

    def _instance_with_history(self) -> tuple[str, int]:
        self.call("PUT", "/v1/workflows/tl", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "go"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body, _ = self.call("POST", "/v1/workflows/tl/instances", {"context": {"k": "v"}})
        iid = body["id"]
        self.call("POST", f"/v1/instances/{iid}/signals", {"event": "go", "eventId": "sig-1"})
        self.call("POST", f"/v1/instances/{iid}/events", {"outcome": "succeeded", "eventId": "evt-2"})
        return iid, body["workflowVersion"]

    def test_timeline_end_to_end_with_etag(self) -> None:
        iid, version = self._instance_with_history()
        status, page, headers = self.call("GET", f"/v1/instances/{iid}/timeline")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("ETag"), '"3"')
        self.assertEqual(page["id"], iid)
        self.assertEqual(page["workflow"], "tl")
        self.assertEqual(page["workflowVersion"], version)
        self.assertEqual(page["state"]["status"], "completed")
        self.assertEqual([item["seq"] for item in page["timeline"]], [1, 2])
        self.assertIsNone(page["nextSeq"])
        first = page["timeline"][0]
        self.assertEqual(first["kind"], "signal")
        self.assertEqual(first["eventId"], "sig-1")
        self.assertEqual(first["step"], "s2")
        self.assertNotIn("context", first)
        self.assertNotIn("response", first)
        # Paging through with the cursor.
        status, page1, _ = self.call("GET", f"/v1/instances/{iid}/timeline?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual([item["seq"] for item in page1["timeline"]], [1])
        self.assertEqual(page1["nextSeq"], 1)
        status, page2, _ = self.call("GET", f"/v1/instances/{iid}/timeline?limit=1&afterSeq=1")
        self.assertEqual(status, 200)
        self.assertEqual([item["seq"] for item in page2["timeline"]], [2])
        self.assertIsNone(page2["nextSeq"])

    def test_timeline_seq_set_and_cursor_match_audit(self) -> None:
        iid, _version = self._instance_with_history()
        _, audit, _ = self.call("GET", f"/v1/instances/{iid}/audit?limit=1")
        _, timeline, _ = self.call("GET", f"/v1/instances/{iid}/timeline?limit=1")
        self.assertEqual([h["seq"] for h in audit["history"]],
                         [item["seq"] for item in timeline["timeline"]])
        self.assertEqual(audit["nextSeq"], timeline["nextSeq"])

    def test_invalid_query_params_are_400(self) -> None:
        iid, _version = self._instance_with_history()
        bad_paths = [
            f"/v1/instances/{iid}/timeline?limit=0",
            f"/v1/instances/{iid}/timeline?limit=201",
            f"/v1/instances/{iid}/timeline?limit=01",
            f"/v1/instances/{iid}/timeline?limit=",
            f"/v1/instances/{iid}/timeline?limit=1.5",
            f"/v1/instances/{iid}/timeline?limit=true",
            f"/v1/instances/{iid}/timeline?limit=-1",
            f"/v1/instances/{iid}/timeline?afterSeq=-1",
            f"/v1/instances/{iid}/timeline?afterSeq=00",
            f"/v1/instances/{iid}/timeline?afterSeq=1.0",
            f"/v1/instances/{iid}/timeline?afterSeq=",
            f"/v1/instances/{iid}/timeline?limit=1&limit=1",
            f"/v1/instances/{iid}/timeline?afterSeq=0&afterSeq=0",
            f"/v1/instances/{iid}/timeline?kind=event",
            f"/v1/instances/{iid}/timeline?bogus=1",
        ]
        for path in bad_paths:
            status, body, _ = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertEqual(body["error"]["code"], "invalid_request", path)
        # Validation precedes the instance lookup: 400 even for a missing id.
        status, _, _ = self.call("GET", "/v1/instances/missing/timeline?limit=01")
        self.assertEqual(status, 400)
        # The rejections wrote nothing: the instance is still at revision 3.
        _, _, headers = self.call("GET", f"/v1/instances/{iid}")
        self.assertEqual(headers.get("ETag"), '"3"')

    def test_valid_query_on_missing_instance_is_404(self) -> None:
        status, body, _ = self.call("GET", "/v1/instances/missing/timeline")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "not_found")
        status, _, _ = self.call("GET", "/v1/instances/missing/timeline?limit=10&afterSeq=0")
        self.assertEqual(status, 404)

    def test_limit_boundaries_accepted(self) -> None:
        iid, _version = self._instance_with_history()
        for value in ("1", "200"):
            status, _, _ = self.call("GET", f"/v1/instances/{iid}/timeline?limit={value}")
            self.assertEqual(status, 200, value)


if __name__ == "__main__":
    unittest.main()