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

from saga import Engine, InstanceNotFound, InvalidRequest, InvalidTransition, WORKFLOWS, apply_outcome, initial_state


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


if __name__ == "__main__":
    unittest.main()
