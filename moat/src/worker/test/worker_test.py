from unittest import mock

import pytest

from ..src import worker as worker_module
from ..src.worker import Worker


@pytest.fixture
def worker() -> Worker:
    with mock.patch.object(worker_module, "Database"), mock.patch.object(
        worker_module, "EventLogger"
    ):
        worker = Worker()
    worker.policy_evaluator_config.evaluate_on_bundle_change = True
    return worker


@pytest.mark.parametrize(
    "enabled, hashes, expected_calls",
    [
        (True, ["a", "b"], 1),  # new bundle
        (True, [None, "a"], 1),  # first bundle
        (True, ["a", "a"], 0),  # unchanged
        (False, ["a", "b"], 0),  # disabled
    ],
)
def test_refresh_bundle_evaluates_on_new_bundle(
    worker: Worker, enabled: bool, hashes: list, expected_calls: int
) -> None:
    worker.policy_evaluator_config.evaluate_on_bundle_change = enabled
    with mock.patch.object(worker_module, "BundleService") as bundle_service, mock.patch.object(
        worker_module, "PolicyEvaluator"
    ) as policy_evaluator, mock.patch.object(
        Worker, "_get_bundle_e_tag", side_effect=hashes
    ):
        worker._refresh_bundle()

    bundle_service.refresh_bundle.assert_called_once()
    assert policy_evaluator.return_value.evaluate.call_count == expected_calls


def test_refresh_bundle_failure_skips_evaluation(worker: Worker) -> None:
    with mock.patch.object(worker_module, "BundleService") as bundle_service, mock.patch.object(
        worker_module, "PolicyEvaluator"
    ) as policy_evaluator, mock.patch.object(
        Worker, "_get_bundle_e_tag", side_effect=["a", "b"]
    ):
        bundle_service.refresh_bundle.side_effect = RuntimeError("boom")
        worker._refresh_bundle()

    policy_evaluator.return_value.evaluate.assert_not_called()
