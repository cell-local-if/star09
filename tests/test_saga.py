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


class RetryTransitionTests(unittest.TestCase):
    """Pure state-machine semantics for step-level retry."""

    def _workflow(self, *retries):
        steps = [
            {"name": "a", "compensation": "undo-a"},
            {"name": "b", "compensation": "undo-b"},
            {"name": "c", "compensation": "undo-c"},
        ]
        for index, retry in enumerate(retries):
            if retry is not None:
                steps[index]["retry"] = {"maxAttempts": retry}
        return {"steps": steps}

    def test_initial_state_starts_at_attempt_one(self) -> None:
        state = initial_state(self._workflow(3), {})
        self.assertEqual(state["attempt"], 1)

    def test_failure_below_max_keeps_running_and_increments_attempt(self) -> None:
        wf = self._workflow(3)
        state = initial_state(wf, {})
        state = apply_outcome(wf, state, "failed", {"reason": "transient"})
        self.assertEqual(state["status"], "running")
        self.assertEqual((state["step"], state["index"]), ("a", 0))
        self.assertEqual(state["attempt"], 2)
        self.assertEqual(state["failure"], {"step": "a", "detail": {"reason": "transient"}})
        self.assertEqual(state["compensated"], [])
        self.assertEqual(state["completed"], [])
        state = apply_outcome(wf, state, "failed", {"reason": "again"})
        self.assertEqual(state["attempt"], 3)
        self.assertEqual(state["failure"]["detail"], {"reason": "again"})
        self.assertEqual(state["status"], "running")

    def test_failure_at_max_attempt_compensates_in_reverse_order(self) -> None:
        wf = self._workflow(2, 2)
        state = initial_state(wf, {})
        state = apply_outcome(wf, state, "succeeded")  # a done, attempt reset on b
        self.assertEqual(state["step"], "b")
        self.assertEqual(state["attempt"], 1)
        state = apply_outcome(wf, state, "failed")  # b attempt 2
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["attempt"], 2)
        state = apply_outcome(wf, state, "failed", {"reason": "dead"})  # exhausted
        self.assertEqual(state["status"], "compensated")
        self.assertIsNone(state["step"])
        self.assertEqual(state["compensated"], ["undo-b", "undo-a"])
        self.assertEqual(state["failure"], {"step": "b", "detail": {"reason": "dead"}})

    def test_retry_then_success_advances_clears_failure_and_resets_attempt(self) -> None:
        wf = self._workflow(3)
        state = initial_state(wf, {})
        state = apply_outcome(wf, state, "failed", {"reason": "x"})
        self.assertEqual(state["attempt"], 2)
        state = apply_outcome(wf, state, "succeeded")
        self.assertEqual(state["completed"], ["a"])
        self.assertIsNone(state["failure"])
        self.assertEqual((state["step"], state["index"], state["attempt"]), ("b", 1, 1))

    def test_step_without_retry_compensates_on_first_failure(self) -> None:
        wf = self._workflow(None)
        state = initial_state(wf, {})
        state = apply_outcome(wf, state, "failed", {"reason": "nope"})
        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["compensated"], ["undo-a"])

    def test_legacy_state_without_attempt_is_treated_as_attempt_one(self) -> None:
        wf = self._workflow(2)
        state = initial_state(wf, {})
        del state["attempt"]
        state = apply_outcome(wf, state, "failed")
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["attempt"], 2)
        # Same legacy state on a workflow without retry compensates immediately.
        wf_no_retry = self._workflow(None)
        state = initial_state(wf_no_retry, {})
        del state["attempt"]
        state = apply_outcome(wf_no_retry, state, "failed")
        self.assertEqual(state["status"], "compensated")

    def test_retry_validation_rejects_bad_shapes(self) -> None:
        engine = Engine()
        try:
            base_step = {"name": "a", "compensation": "undo-a"}
            bad_retries = [
                {"maxAttempts": 1}, {"maxAttempts": 11}, {"maxAttempts": 2.0},
                {"maxAttempts": "2"}, {"maxAttempts": True}, {"maxAttempts": None},
                {}, {"maxAttempts": 2, "extra": 1}, "nope",
            ]
            for retry in bad_retries:
                step = dict(base_step, retry=retry)
                with self.assertRaises(InvalidRequest):
                    engine.define("bad", {"steps": [step]})
            # A rejected definition must not replace an existing valid one.
            engine.define("keep", {"steps": [dict(base_step)]})
            with self.assertRaises(InvalidRequest):
                engine.define("keep", {"steps": [dict(base_step, retry={"maxAttempts": 1})]})
            self.assertEqual(engine.workflow("keep")["steps"], [dict(base_step)])
            # Valid boundaries are accepted and echoed back.
            for value in (2, 10):
                wf = engine.define(f"ok-{value}", {"steps": [dict(base_step, retry={"maxAttempts": value})]})
                self.assertEqual(wf["steps"][0]["retry"], {"maxAttempts": value})
        finally:
            engine.close()


class RetryEngineTests(unittest.TestCase):
    """Engine/ledger interaction with retries."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.engine.define(
            "retried",
            {"steps": [
                {"name": "a", "compensation": "undo-a", "retry": {"maxAttempts": 3}},
                {"name": "b", "compensation": "undo-b"},
            ]},
        )

    def tearDown(self) -> None:
        self.engine.close()

    def _start(self) -> str:
        return self.engine.start("retried", {})["id"]

    def test_each_failed_attempt_with_distinct_event_id_persists(self) -> None:
        iid = self._start()
        first = self.engine.advance(iid, "failed", {"n": 1}, event_id="fail-1")
        self.assertEqual((first["state"]["attempt"], first["state"]["status"]), (2, "running"))
        second = self.engine.advance(iid, "failed", {"n": 2}, event_id="fail-2")
        self.assertEqual(second["state"]["attempt"], 3)
        self.assertEqual(second["state"]["failure"]["detail"], {"n": 2})
        # Replaying the first failure returns the historical attempt=2 state even
        # though the instance has since moved on to attempt 3.
        replay = self.engine.advance(iid, "failed", {"n": 1}, event_id="fail-1")
        self.assertEqual(replay, first)
        self.assertEqual(self.engine.get(iid)["state"]["attempt"], 3)

    def test_replay_of_retry_failure_does_not_increment_attempt_twice(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "failed", event_id="fail-1")
        replay = self.engine.advance(iid, "failed", event_id="fail-1")
        self.assertEqual(replay["state"]["attempt"], 2)
        self.assertEqual(self.engine.get(iid)["state"]["attempt"], 2)
        rows = self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0]
        self.assertEqual(rows, 1)

    def test_replayed_failure_with_different_detail_is_409(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "failed", {"n": 1}, event_id="fail-1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "failed", {"n": 2}, event_id="fail-1")
        self.assertEqual(self.engine.get(iid)["state"]["attempt"], 2)

    def test_exhaustion_then_new_event_on_terminal_is_409_and_writes_nothing(self) -> None:
        iid = self._start()
        self.engine.advance(iid, "failed", event_id="f1")  # attempt 2
        self.engine.advance(iid, "failed", event_id="f2")  # attempt 3
        final = self.engine.advance(iid, "failed", {"reason": "last"}, event_id="f3")
        self.assertEqual(final["state"]["status"], "compensated")
        self.assertEqual(final["state"]["compensated"], ["undo-a"])
        with self.assertRaises(InvalidTransition):
            self.engine.advance(iid, "succeeded", event_id="after")
        rows = {r[0] for r in self.engine._db.execute(
            "SELECT event_id FROM instance_events WHERE instance_id = ?", (iid,)).fetchall()}
        self.assertEqual(rows, {"f1", "f2", "f3"})

    def test_attempt_survives_reopen(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            engine = Engine(path)
            engine.define(
                "retried",
                {"steps": [{"name": "a", "compensation": "undo-a", "retry": {"maxAttempts": 2}}]},
            )
            iid = engine.start("retried", {})["id"]
            engine.advance(iid, "failed", event_id="f1")
            engine.close()

            engine = Engine(path)
            try:
                # Definitions are in-memory (baseline behavior); re-register before advancing.
                engine.define(
                    "retried",
                    {"steps": [{"name": "a", "compensation": "undo-a", "retry": {"maxAttempts": 2}}]},
                )
                state = engine.get(iid)["state"]
                self.assertEqual((state["attempt"], state["status"]), (2, "running"))
                exhausted = engine.advance(iid, "failed", event_id="f2")
                self.assertEqual(exhausted["state"]["status"], "compensated")
            finally:
                engine.close()
        finally:
            os.unlink(path)

    def test_legacy_instance_gets_attempt_on_read_and_first_advance(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            legacy = sqlite3.connect(path)
            legacy.execute("CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)")
            # A "retried" workflow definition is registered at runtime, so seed an
            # instance mid-step on the built-in order workflow (no retry): the
            # backfill must still present attempt=1 and behave as before.
            state = initial_state(WORKFLOWS["order"], {})
            state = apply_outcome(WORKFLOWS["order"], state, "succeeded")
            legacy.execute("INSERT INTO instances VALUES (?, ?, ?)", ("old", "order", json.dumps(state)))
            legacy.commit()
            legacy.close()

            engine = Engine(path)
            try:
                self.assertEqual(engine.get("old")["state"]["attempt"], 1)
                result = engine.advance("old", "failed", event_id="e1")
                self.assertEqual(result["state"]["status"], "compensated")
                self.assertEqual(result["state"]["compensated"], ["refund-payment", "release-stock"])
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

    def test_retry_definition_and_failure_loop_over_http(self) -> None:
        definition = {"steps": [
            {"name": "a", "compensation": "undo-a", "retry": {"maxAttempts": 2}},
            {"name": "b", "compensation": "undo-b"},
        ]}
        self.assertEqual(self.call("PUT", "/v1/workflows/flaky", definition)[0], 200)
        status, body = self.call("POST", "/v1/workflows/flaky/instances", {})
        self.assertEqual(status, 201)
        self.assertEqual(body["state"]["attempt"], 1)
        iid = body["id"]
        status, body = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "failed", "detail": {"why": "blip"}, "eventId": "f1"})
        self.assertEqual(status, 200)
        self.assertEqual((body["state"]["status"], body["state"]["attempt"]), ("running", 2))
        # Verbatim replay of the not-yet-retried historical response.
        status, replay = self.call("POST", f"/v1/instances/{iid}/events",
                                   {"outcome": "failed", "detail": {"why": "blip"}, "eventId": "f1"})
        self.assertEqual(status, 200)
        self.assertEqual(replay, body)
        status, body = self.call("POST", f"/v1/instances/{iid}/events",
                                 {"outcome": "succeeded", "eventId": "ok"})
        self.assertEqual(status, 200)
        self.assertEqual((body["state"]["step"], body["state"]["attempt"]), ("b", 1))
        self.assertIsNone(body["state"]["failure"])

    def test_invalid_retry_definitions_are_400(self) -> None:
        step = {"name": "a", "compensation": "undo-a"}
        for bad_retry in ({"maxAttempts": 1}, {"maxAttempts": 11}, {"maxAttempts": True},
                          {"maxAttempts": 2.5}, {}, {"maxAttempts": 2, "x": 1}):
            status, body = self.call("PUT", "/v1/workflows/bad-retry",
                                    {"steps": [dict(step, retry=bad_retry)]})
            self.assertEqual(status, 400, bad_retry)
            self.assertEqual(body["error"]["code"], "invalid_request")


if __name__ == "__main__":
    unittest.main()
