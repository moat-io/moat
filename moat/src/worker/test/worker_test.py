from unittest.mock import MagicMock, patch

import pytest

from ..src.worker import Worker


class _StopLoop(Exception):
    """Raised inside a mocked dependency to break out of the worker's infinite loop."""


@pytest.fixture
def worker() -> Worker:
    """Build a Worker with every external dependency mocked out.

    The real __init__ loads config and opens a database connection, so we patch
    all of its collaborators before construction. This keeps the test focused on
    the orchestration logic in start() rather than any external system.
    """
    with patch("worker.src.worker.WorkerConfig") as mock_config_cls, patch(
        "worker.src.worker.Database"
    ) as mock_database_cls, patch(
        "worker.src.worker.EventLogger"
    ) as mock_event_logger_cls, patch(
        "worker.src.worker.PolicyEvaluator"
    ) as mock_policy_evaluator_cls:
        mock_config_cls.return_value.load.return_value = MagicMock(interval_s=60)

        worker = Worker()

        # expose the mocked collaborators for assertions
        worker.config = mock_config_cls.return_value.load.return_value
        return worker


def test_init_wires_up_dependencies() -> None:
    """The constructor loads config, connects the database and builds collaborators."""
    with patch("worker.src.worker.WorkerConfig") as mock_config_cls, patch(
        "worker.src.worker.Database"
    ) as mock_database_cls, patch("worker.src.worker.EventLogger"), patch(
        "worker.src.worker.PolicyEvaluator"
    ):
        worker = Worker()

        mock_config_cls.return_value.load.assert_called_once()
        mock_database_cls.return_value.connect.assert_called_once()
        assert worker.event_logger is not None
        assert worker.policy_evaluator is not None


def test_start_logs_start_event(worker: Worker) -> None:
    """start() emits a 'start' event before entering the loop."""
    worker._refresh_bundle = MagicMock(return_value=[])
    worker._clean_up_bundle_storage = MagicMock()
    worker._wait = MagicMock(side_effect=_StopLoop)

    with pytest.raises(_StopLoop):
        worker.start()

    worker.event_logger.log_event.assert_any_call(asset="worker", action="start")


def test_start_refreshes_and_cleans_up_every_iteration(worker: Worker) -> None:
    """Each loop iteration refreshes the bundle and cleans up storage."""
    worker._refresh_bundle = MagicMock(return_value=[])
    worker._clean_up_bundle_storage = MagicMock()
    worker._assure_opa_bundles = MagicMock()
    worker._evaluate_policies = MagicMock()
    # stop after the first iteration
    worker._wait = MagicMock(side_effect=_StopLoop)

    with pytest.raises(_StopLoop):
        worker.start()

    worker._refresh_bundle.assert_called_once()
    worker._clean_up_bundle_storage.assert_called_once()


def test_start_assures_and_evaluates_when_bundles_updated(worker: Worker) -> None:
    """When the refresh returns updated platforms, OPA assurance and policy evaluation run."""
    worker._refresh_bundle = MagicMock(return_value=["trino"])
    worker._clean_up_bundle_storage = MagicMock()
    worker._assure_opa_bundles = MagicMock()
    worker._evaluate_policies = MagicMock()
    worker._wait = MagicMock(side_effect=_StopLoop)

    with pytest.raises(_StopLoop):
        worker.start()

    worker._assure_opa_bundles.assert_called_once()
    worker._evaluate_policies.assert_called_once()


def test_start_skips_assure_and_evaluate_when_no_updates(worker: Worker) -> None:
    """When the refresh returns no updated platforms, OPA assurance and policy evaluation are skipped."""
    worker._refresh_bundle = MagicMock(return_value=[])
    worker._clean_up_bundle_storage = MagicMock()
    worker._assure_opa_bundles = MagicMock()
    worker._evaluate_policies = MagicMock()
    worker._wait = MagicMock(side_effect=_StopLoop)

    with pytest.raises(_StopLoop):
        worker.start()

    worker._assure_opa_bundles.assert_not_called()
    worker._evaluate_policies.assert_not_called()


def test_start_runs_multiple_iterations(worker: Worker) -> None:
    """The loop keeps running until interrupted; behaviour adapts per iteration."""
    # first iteration: updates present -> evaluate; second: no updates -> skip; then stop
    worker._refresh_bundle = MagicMock(side_effect=[["trino"], []])
    worker._clean_up_bundle_storage = MagicMock()
    worker._assure_opa_bundles = MagicMock()
    worker._evaluate_policies = MagicMock()
    worker._wait = MagicMock(side_effect=[None, _StopLoop])

    with pytest.raises(_StopLoop):
        worker.start()

    assert worker._refresh_bundle.call_count == 2
    assert worker._clean_up_bundle_storage.call_count == 2
    # only the first iteration had updates
    worker._assure_opa_bundles.assert_called_once()
    worker._evaluate_policies.assert_called_once()


def test_refresh_bundle_returns_updated_platforms(worker: Worker) -> None:
    """_refresh_bundle delegates to BundleService and returns its result."""
    with patch("worker.src.worker.BundleService") as mock_bundle_service:
        mock_bundle_service.refresh_bundle.return_value = ["trino", "spark"]

        result = worker._refresh_bundle()

        assert result == ["trino", "spark"]
        mock_bundle_service.refresh_bundle.assert_called_once_with(
            database=worker.database, event_logger=worker.event_logger
        )


def test_refresh_bundle_swallows_errors(worker: Worker) -> None:
    """A failure in BundleService.refresh_bundle is logged and an empty list returned."""
    with patch("worker.src.worker.BundleService") as mock_bundle_service:
        mock_bundle_service.refresh_bundle.side_effect = RuntimeError("boom")

        result = worker._refresh_bundle()

        assert result == []


def test_clean_up_bundle_storage_commits(worker: Worker) -> None:
    """_clean_up_bundle_storage opens a session, delegates and commits."""
    session = MagicMock()
    worker.database.Session.return_value.__enter__.return_value = session

    with patch("worker.src.worker.BundleService") as mock_bundle_service:
        worker._clean_up_bundle_storage()

        mock_bundle_service.clean_up_bundle_storage.assert_called_once_with(
            session=session, event_logger=worker.event_logger
        )
        session.commit.assert_called_once()


def test_clean_up_bundle_storage_swallows_errors(worker: Worker) -> None:
    """An exception during clean up is caught and does not propagate."""
    worker.database.Session.side_effect = RuntimeError("db down")

    # should not raise
    worker._clean_up_bundle_storage()


def test_evaluate_policies_invokes_evaluator(worker: Worker) -> None:
    """_evaluate_policies runs the policy evaluator within a session."""
    session = MagicMock()
    worker.database.Session.return_value.__enter__.return_value = session

    worker._evaluate_policies()

    worker.policy_evaluator.evaluate.assert_called_once_with(session=session)


def test_evaluate_policies_swallows_errors(worker: Worker) -> None:
    """An exception during evaluation is caught and does not propagate."""
    worker.policy_evaluator.evaluate.side_effect = RuntimeError("evaluator failed")
    session = MagicMock()
    worker.database.Session.return_value.__enter__.return_value = session

    # should not raise
    worker._evaluate_policies()


def test_wait_sleeps_and_logs_heartbeat(worker: Worker) -> None:
    """_wait sleeps for the configured interval and logs a heartbeat event."""
    worker.config.interval_s = 5

    with patch("worker.src.worker.time") as mock_time:
        worker._wait()

        mock_time.sleep.assert_called_once_with(5)
        worker.event_logger.log_event.assert_called_once_with(
            asset="worker", action="heartbeat"
        )
