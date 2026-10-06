"""Baseline tests for the saga orchestrator."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from saga import Engine, InstanceNotFound, InvalidRequest, InvalidTransition, WORKFLOWS, apply_outcome, apply_signal, initial_state


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
    """Instance-level audit trail: one record per accepted, state-advancing call."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define("approval", _await_workflow())

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self, workflow: str = "approval") -> str:
        return self.engine.start(workflow, {})["id"]

    def test_history_records_accepted_calls_in_order_with_full_payloads(self) -> None:
        iid = self._start()
        first = self.engine.signal(iid, "approved", {"by": "boss"}, event_id="shared-1")
        # The same eventId string may back an event and a signal; kind tells them apart.
        second = self.engine.advance(iid, "succeeded", event_id="shared-1")
        third = self.engine.signal(iid, "shipped")  # no eventId
        audit = self.engine.audit(iid)
        self.assertEqual(audit["id"], iid)
        self.assertEqual(audit["workflow"], "approval")
        self.assertEqual(audit["state"], self.engine.get(iid)["state"])
        self.assertEqual(audit["state"]["status"], "completed")
        history = audit["history"]
        self.assertEqual([r["seq"] for r in history], [1, 2, 3])
        self.assertEqual([r["kind"] for r in history], ["signal", "event", "signal"])
        self.assertEqual([r["eventId"] for r in history], ["shared-1", "shared-1", None])
        self.assertEqual(history[0]["request"], {"event": "approved", "detail": {"by": "boss"}})
        self.assertEqual(history[1]["request"], {"outcome": "succeeded", "detail": None})
        self.assertEqual(history[2]["request"], {"event": "shipped", "detail": None})
        self.assertEqual([r["response"] for r in history], [first, second, third])

    def test_omitted_and_null_detail_both_recorded_as_null(self) -> None:
        iid = self._start("order")
        self.engine.advance(iid, "succeeded")
        self.engine.advance(iid, "succeeded", None)
        history = self.engine.audit(iid)["history"]
        self.assertEqual([r["request"] for r in history],
                         [{"outcome": "succeeded", "detail": None}] * 2)

    def test_rejections_and_replays_append_nothing(self) -> None:
        iid = self._start()
        plain = self._start("order")
        with self.assertRaises(InvalidRequest):
            self.engine.signal(iid, "", event_id="bad")
        with self.assertRaises(InvalidRequest):
            self.engine.advance(plain, "bogus", event_id="bad")
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "rejected", event_id="nope")  # name mismatch
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="nope")  # waiting for signal
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("missing", "succeeded", event_id="nope")
        self.assertEqual(self.engine.audit(iid)["history"], [])
        self.assertEqual(self.engine.audit(plain)["history"], [])
        # One accepted call, then replays and conflicts still add nothing.
        first = self.engine.signal(iid, "approved", event_id="sig-1")
        self.assertEqual(self.engine.signal(iid, "approved", event_id="sig-1"), first)
        with self.assertRaises(InvalidTransition):
            self.engine.signal(iid, "approved", {"x": 1}, event_id="sig-1")
        history = self.engine.audit(iid)["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["response"], first)
        # Terminal instance: fresh eventIds are rejected, old replays still add nothing.
        self.engine.advance(iid, "succeeded")
        self.engine.signal(iid, "shipped", event_id="sig-2")
        self.assertEqual(self.engine.get(iid)["state"]["status"], "completed")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="late")
        self.assertEqual(self.engine.signal(iid, "approved", event_id="sig-1"), first)
        self.assertEqual(len(self.engine.audit(iid)["history"]), 3)

    def test_audit_unknown_instance_is_not_found(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.audit("no-such-instance")

    def test_concurrent_accepted_calls_get_contiguous_seqs(self) -> None:
        iid = self._start("order")
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
        self.assertEqual([r["seq"] for r in history], [1, 2, 3])
        self.assertEqual(sorted(r["eventId"] for r in history), ["ev-0", "ev-1", "ev-2"])
        # The recorded responses chain into one consistent serial order.
        self.assertEqual(history[-1]["response"]["state"], self.engine.get(iid)["state"])


class AuditPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_history_survives_reopen(self) -> None:
        engine = Engine(self.path)
        engine.define("approval", _await_workflow())
        iid = engine.start("approval", {})["id"]
        first = engine.signal(iid, "approved", event_id="s-1")
        engine.advance(iid, "succeeded", event_id="e-1")
        engine.close()

        engine = Engine(self.path)
        try:
            engine.define("approval", _await_workflow())
            audit = engine.audit(iid)
            self.assertEqual([r["seq"] for r in audit["history"]], [1, 2])
            self.assertEqual(audit["history"][0]["response"], first)
            self.assertEqual(audit["state"], engine.get(iid)["state"])
            # The next accepted call continues the sequence, and replaying an old
            # eventId after reopen still appends nothing.
            engine.signal(iid, "shipped", event_id="s-2")
            engine.advance(iid, "succeeded", event_id="e-1")  # replay
            audit = engine.audit(iid)
            self.assertEqual([r["seq"] for r in audit["history"]], [1, 2, 3])
            self.assertEqual(audit["history"][2]["kind"], "signal")
            self.assertEqual(audit["state"]["status"], "completed")
        finally:
            engine.close()

    def test_legacy_file_without_audit_table_starts_empty_then_accumulates(self) -> None:
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
            # No fabricated history for the pre-upgrade advance.
            audit = engine.audit("legacy-id")
            self.assertEqual(audit["history"], [])
            self.assertEqual(audit["state"]["completed"], ["reserve-stock"])
            # From the next successful advance the instance accumulates history at seq 1.
            result = engine.advance("legacy-id", "succeeded", event_id="new-era-1")
            engine.advance("legacy-id", "succeeded", event_id="new-era-1")  # replay
            audit = engine.audit("legacy-id")
            self.assertEqual(len(audit["history"]), 1)
            self.assertEqual(audit["history"][0]["seq"], 1)
            self.assertEqual(audit["history"][0]["response"], result)
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
        self.call("PUT", "/v1/workflows/audflow", {"steps": [
            {"name": "s1", "compensation": "c1", "await": {"event": "approved"}},
            {"name": "s2", "compensation": "c2"},
        ]})
        status, body = self.call("POST", "/v1/workflows/audflow/instances", {})
        iid = body["id"]
        # Rejected calls leave no trace.
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded"})[0], 409)  # waiting
        self.assertEqual(self.call("POST", f"/v1/instances/{iid}/signals",
                                   {"event": "denied"})[0], 409)  # mismatch
        status, first = self.call("POST", f"/v1/instances/{iid}/signals",
                                  {"event": "approved", "eventId": "sig-1"})
        self.assertEqual(status, 200)
        status, second = self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "succeeded", "detail": None})
        self.assertEqual(status, 200)
        # Replay after completion returns the first response and appends nothing.
        status, replay = self.call("POST", f"/v1/instances/{iid}/signals",
                                   {"event": "approved", "eventId": "sig-1"})
        self.assertEqual((status, replay), (200, first))

        status, audit = self.call("GET", f"/v1/instances/{iid}/audit")
        self.assertEqual(status, 200)
        self.assertEqual(audit["id"], iid)
        self.assertEqual(audit["workflow"], "audflow")
        self.assertEqual(audit["state"]["status"], "completed")
        self.assertEqual([r["seq"] for r in audit["history"]], [1, 2])
        self.assertEqual([r["kind"] for r in audit["history"]], ["signal", "event"])
        self.assertEqual(audit["history"][0]["eventId"], "sig-1")
        self.assertIsNone(audit["history"][1]["eventId"])
        self.assertEqual(audit["history"][0]["request"],
                         {"event": "approved", "detail": None})
        self.assertEqual(audit["history"][1]["request"],
                         {"outcome": "succeeded", "detail": None})
        self.assertEqual(audit["history"][0]["response"], first)
        self.assertEqual(audit["history"][1]["response"], second)
        # Unknown instance is 404 not_found.
        status, body = self.call("GET", "/v1/instances/missing/audit")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))


if __name__ == "__main__":
    unittest.main()
