"""Baseline tests for the saga orchestrator."""
from __future__ import annotations

import json
import os
import shutil
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


class IdempotentEventEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = Engine()

    def tearDown(self) -> None:
        self.engine.close()

    def start(self, workflow: str = "order") -> str:
        return self.engine.start(workflow, {})["id"]

    def ledger(self, instance_id: str) -> list[tuple]:
        rows = self.engine._db.execute(
            "SELECT event_id, outcome, detail, response FROM instance_events WHERE instance_id = ? ORDER BY rowid",
            (instance_id,),
        ).fetchall()
        return [(eid, outcome, json.loads(detail), json.loads(response)) for eid, outcome, detail, response in rows]

    def test_replay_returns_original_response_and_does_not_advance(self) -> None:
        instance = self.start()
        first = self.engine.advance(instance, "succeeded", event_id="e1")
        self.assertEqual(first["state"]["completed"], ["reserve-stock"])
        replayed = self.engine.advance(instance, "succeeded", event_id="e1")
        self.assertEqual(replayed, first)
        current = self.engine.get(instance)["state"]
        self.assertEqual(current["completed"], ["reserve-stock"])
        self.assertEqual(current["step"], "charge-payment")

    def test_replay_after_instance_moved_on_returns_historical_snapshot(self) -> None:
        instance = self.start()
        first_a = self.engine.advance(instance, "succeeded", event_id="a")
        self.engine.advance(instance, "succeeded", event_id="b")
        current = self.engine.get(instance)["state"]
        self.assertEqual(current["step"], "create-shipment")
        # The replay is the exact 200 response from the first processing, frozen at that state.
        self.assertEqual(self.engine.advance(instance, "succeeded", event_id="a"), first_a)
        self.assertEqual(first_a["state"]["step"], "charge-payment")
        # Current state is untouched by the replay.
        self.assertEqual(self.engine.get(instance)["state"]["step"], "create-shipment")

    def test_replay_after_failure_does_not_reaccumulate_compensation(self) -> None:
        instance = self.start()
        self.engine.advance(instance, "succeeded", event_id="a")
        failed = self.engine.advance(instance, "failed", {"reason": "card declined"}, event_id="b")
        self.assertEqual(failed["state"]["compensated"], ["refund-payment", "release-stock"])
        replayed = self.engine.advance(instance, "failed", {"reason": "card declined"}, event_id="b")
        self.assertEqual(replayed, failed)
        state = self.engine.get(instance)["state"]
        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["compensated"], ["refund-payment", "release-stock"])
        self.assertEqual(state["failure"], {"step": "charge-payment", "detail": {"reason": "card declined"}})

    def test_missing_detail_and_explicit_null_are_equivalent(self) -> None:
        instance = self.start()
        first = self.engine.advance(instance, "succeeded", event_id="e1")
        # Omitted on the first call, explicit null on retry.
        self.assertEqual(self.engine.advance(instance, "succeeded", None, "e1"), first)
        instance2 = self.start()
        first2 = self.engine.advance(instance2, "failed", None, event_id="e2")
        # Reverse direction: explicit null first, omitted on retry.
        self.assertEqual(self.engine.advance(instance2, "failed", event_id="e2"), first2)

    def test_detail_compared_as_json_value(self) -> None:
        instance = self.start()
        first = self.engine.advance(instance, "failed", {"b": 2, "a": 1}, event_id="e1")
        self.assertEqual(self.engine.advance(instance, "failed", {"a": 1, "b": 2}, event_id="e1"), first)
        with self.assertRaises(InvalidTransition):
            self.engine.advance(instance, "failed", {"a": 1, "b": 3}, event_id="e1")

    def test_same_event_id_different_outcome_or_detail_conflicts(self) -> None:
        instance = self.start()
        self.engine.advance(instance, "succeeded", event_id="e1")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(instance, "failed", event_id="e1")
        instance2 = self.start()
        self.engine.advance(instance2, "failed", {"reason": "x"}, event_id="e2")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(instance2, "failed", {"reason": "y"}, event_id="e2")
        # Conflicts must not advance state or add ledger rows.
        self.assertEqual(len(self.ledger(instance)), 1)
        self.assertEqual(len(self.ledger(instance2)), 1)
        self.assertEqual(self.engine.get(instance)["state"]["completed"], ["reserve-stock"])

    def test_event_id_namespace_is_per_instance(self) -> None:
        one, two = self.start(), self.start()
        first_one = self.engine.advance(one, "succeeded", event_id="shared")
        first_two = self.engine.advance(two, "succeeded", event_id="shared")
        self.assertEqual(self.engine.advance(one, "succeeded", event_id="shared"), first_one)
        self.assertEqual(self.engine.advance(two, "succeeded", event_id="shared"), first_two)

    def test_invalid_event_id_shapes_are_rejected(self) -> None:
        instance = self.start()
        for bad in ["", "x" * 101, 42, True, ["e"], {"e": 1}]:
            with self.assertRaises(InvalidRequest):
                self.engine.advance(instance, "succeeded", event_id=bad)  # type: ignore[arg-type]
        # Boundary: exactly 100 characters is accepted.
        response = self.engine.advance(instance, "succeeded", event_id="x" * 100)
        self.assertEqual(response["state"]["status"], "running")
        self.assertEqual(self.ledger(instance)[0][0], "x" * 100)

    def test_invalid_outcome_with_event_id_is_not_ledgered(self) -> None:
        instance = self.start()
        with self.assertRaises(InvalidRequest):
            self.engine.advance(instance, "maybe", event_id="e1")
        self.assertEqual(self.ledger(instance), [])
        # The rejected eventId can be reused by a legal event.
        response = self.engine.advance(instance, "succeeded", event_id="e1")
        self.assertEqual(response["state"]["completed"], ["reserve-stock"])

    def test_terminal_transition_with_event_id_is_not_ledgered(self) -> None:
        instance = self.start()
        self.engine.advance(instance, "failed", event_id="terminal")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(instance, "succeeded", event_id="after-terminal")
        rows = self.ledger(instance)
        self.assertEqual([row[0] for row in rows], ["terminal"])

    def test_missing_instance_with_event_id_is_not_ledgered(self) -> None:
        with self.assertRaises(InstanceNotFound):
            self.engine.advance("does-not-exist", "succeeded", event_id="e1")
        self.assertEqual(
            self.engine._db.execute("SELECT COUNT(*) FROM instance_events").fetchone()[0], 0
        )

    def test_ledger_persists_normalized_request_and_full_response(self) -> None:
        instance = self.start()
        self.engine.advance(instance, "failed", {"b": 2, "a": 1}, event_id="e1")
        event_id, outcome, detail, response = self.ledger(instance)[0]
        self.assertEqual((event_id, outcome), ("e1", "failed"))
        self.assertEqual(detail, {"a": 1, "b": 2})
        self.assertEqual(response["id"], instance)
        self.assertEqual(response["workflow"], "order")
        self.assertEqual(response["state"], self.engine.get(instance)["state"])

    def test_ledger_survives_close_and_reopen(self) -> None:
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "saga.sqlite")
        try:
            first_engine = Engine(path)
            try:
                instance = first_engine.start("order", {})["id"]
                original = first_engine.advance(instance, "failed", {"reason": "down"}, event_id="e1")
            finally:
                first_engine.close()
            second_engine = Engine(path)
            try:
                replayed = second_engine.advance(instance, "failed", {"reason": "down"}, event_id="e1")
                self.assertEqual(replayed, original)
                with self.assertRaises(InvalidTransition):
                    second_engine.advance(instance, "failed", {"reason": "other"}, event_id="e1")
                # State was not re-applied during replay.
                self.assertEqual(len(second_engine.get(instance)["state"]["compensated"]), 1)
            finally:
                second_engine.close()
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def test_legacy_sqlite_file_without_ledger_table_still_opens(self) -> None:
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "legacy.sqlite")
        try:
            legacy_db = sqlite3.connect(path)
            try:
                legacy_db.execute(
                    "CREATE TABLE instances (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, state TEXT NOT NULL)"
                )
                state = json.dumps(initial_state(WORKFLOWS["order"], {}))
                legacy_db.execute("INSERT INTO instances VALUES (?, ?, ?)", ("legacy-1", "order", state))
                legacy_db.commit()
            finally:
                legacy_db.close()
            engine = Engine(path)
            try:
                self.assertEqual(engine.get("legacy-1")["state"]["index"], 0)
                response = engine.advance("legacy-1", "succeeded", event_id="migrated")
                self.assertEqual(response["state"]["completed"], ["reserve-stock"])
                self.assertEqual(
                    engine.advance("legacy-1", "succeeded", event_id="migrated"), response
                )
            finally:
                engine.close()
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def test_events_without_event_id_keep_legacy_behavior(self) -> None:
        instance = self.start()
        self.engine.advance(instance, "succeeded")
        self.engine.advance(instance, "succeeded")
        # A third undecorated event still enters the state machine and completes the workflow.
        self.assertEqual(self.engine.advance(instance, "succeeded")["state"]["status"], "completed")
        with self.assertRaises(InvalidTransition):
            self.engine.advance(instance, "succeeded")
        self.assertEqual(self.ledger(instance), [])

    def test_concurrent_duplicate_event_id_processed_once(self) -> None:
        instance = self.start()
        results: list[dict] = []
        errors: list[Exception] = []
        barrier = threading.Barrier(8)

        def submit() -> None:
            try:
                barrier.wait()
                results.append(self.engine.advance(instance, "succeeded", event_id="once"))
            except Exception as error:  # pragma: no cover - failure path for the test thread
                errors.append(error)

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 8)
        self.assertTrue(all(result == results[0] for result in results))
        state = self.engine.get(instance)["state"]
        self.assertEqual(state["completed"], ["reserve-stock"])
        self.assertEqual(len(self.ledger(instance)), 1)

    def test_concurrent_distinct_events_equivalent_to_a_serial_order(self) -> None:
        self.engine.define("long", {"steps": [
            {"name": f"step-{index}", "compensation": f"undo-{index}"} for index in range(20)
        ]})
        instance = self.engine.start("long", {})["id"]
        errors: list[Exception] = []
        barrier = threading.Barrier(20)

        def submit(event_id: str) -> None:
            try:
                barrier.wait()
                self.engine.advance(instance, "succeeded", event_id=event_id)
            except Exception as error:  # pragma: no cover - failure path for the test thread
                errors.append(error)

        threads = [threading.Thread(target=submit, args=(f"e{index}",)) for index in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        state = self.engine.get(instance)["state"]
        self.assertEqual(state["status"], "completed")
        self.assertEqual(len(state["completed"]), 20)
        self.assertEqual(len(self.ledger(instance)), 20)


class IdempotentEventHttpTests(unittest.TestCase):
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

    def start(self) -> str:
        return self.call("POST", "/v1/workflows/order/instances", {})[1]["id"]

    def test_replay_returns_identical_json_with_historical_state(self) -> None:
        instance = self.start()
        status, first = self.call("POST", f"/v1/instances/{instance}/events",
                                  {"outcome": "succeeded", "eventId": "e1"})
        self.assertEqual(status, 200)
        self.call("POST", f"/v1/instances/{instance}/events", {"outcome": "succeeded", "eventId": "e2"})
        status, replayed = self.call("POST", f"/v1/instances/{instance}/events",
                                     {"outcome": "succeeded", "eventId": "e1"})
        self.assertEqual(status, 200)
        self.assertEqual(replayed, first)
        self.assertEqual(replayed["state"]["step"], "charge-payment")
        status, current = self.call("GET", f"/v1/instances/{instance}")
        self.assertEqual(current["state"]["step"], "create-shipment")

    def test_missing_detail_matches_explicit_null_over_http(self) -> None:
        instance = self.start()
        status, first = self.call("POST", f"/v1/instances/{instance}/events",
                                  {"outcome": "failed", "eventId": "e1"})
        self.assertEqual(status, 200)
        status, replayed = self.call("POST", f"/v1/instances/{instance}/events",
                                     {"outcome": "failed", "detail": None, "eventId": "e1"})
        self.assertEqual((status, replayed), (200, first))

    def test_invalid_event_id_shapes_return_400(self) -> None:
        instance = self.start()
        for bad_event_id in (None, "", 123, ["e"], {"e": 1}, "x" * 101):
            status, body = self.call("POST", f"/v1/instances/{instance}/events",
                                     {"outcome": "succeeded", "eventId": bad_event_id})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        # State must remain at the first step; unknown fields stay rejected.
        status, body = self.call("POST", f"/v1/instances/{instance}/events",
                                 {"outcome": "succeeded", "eventID": "e1"})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.call("GET", f"/v1/instances/{instance}")[1]["state"]["index"], 0)

    def test_conflicting_replay_returns_409_without_state_change(self) -> None:
        instance = self.start()
        self.call("POST", f"/v1/instances/{instance}/events", {"outcome": "succeeded", "eventId": "e1"})
        status, body = self.call("POST", f"/v1/instances/{instance}/events",
                                 {"outcome": "failed", "eventId": "e1"})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        status, body = self.call("POST", f"/v1/instances/{instance}/events",
                                 {"outcome": "succeeded", "detail": {"x": 1}, "eventId": "e1"})
        self.assertEqual((status, body["error"]["code"]), (409, "invalid_transition"))
        self.assertEqual(self.call("GET", f"/v1/instances/{instance}")[1]["state"]["completed"],
                         ["reserve-stock"])

    def test_event_id_is_scoped_per_instance_over_http(self) -> None:
        one, two = self.start(), self.start()
        _, first_one = self.call("POST", f"/v1/instances/{one}/events",
                                 {"outcome": "succeeded", "eventId": "shared"})
        _, first_two = self.call("POST", f"/v1/instances/{two}/events",
                                 {"outcome": "succeeded", "eventId": "shared"})
        _, replay_one = self.call("POST", f"/v1/instances/{one}/events",
                                  {"outcome": "succeeded", "eventId": "shared"})
        _, replay_two = self.call("POST", f"/v1/instances/{two}/events",
                                  {"outcome": "succeeded", "eventId": "shared"})
        self.assertEqual((replay_one, replay_two), (first_one, first_two))

    def test_event_id_on_missing_instance_returns_404(self) -> None:
        status, body = self.call("POST", "/v1/instances/missing/events",
                                 {"outcome": "succeeded", "eventId": "e1"})
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_concurrent_duplicate_requests_share_one_processing(self) -> None:
        instance = self.start()
        responses: list[tuple[int, dict]] = []
        barrier = threading.Barrier(6)

        def submit() -> None:
            barrier.wait()
            responses.append(self.call("POST", f"/v1/instances/{instance}/events",
                                       {"outcome": "succeeded", "eventId": "once"}))

        threads = [threading.Thread(target=submit) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(responses), 6)
        self.assertTrue(all(response == responses[0] for response in responses))
        self.assertEqual(self.call("GET", f"/v1/instances/{instance}")[1]["state"]["completed"],
                         ["reserve-stock"])


if __name__ == "__main__":
    unittest.main()
