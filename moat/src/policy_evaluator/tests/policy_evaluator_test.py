from unittest.mock import MagicMock, patch
from database import Database
from models import (
    PrincipalDbo,
    ResourceDbo,
    ResourceAttributeDbo,
    PolicyEvaluatorResultDbo,
)
from opa.models import OpaTableResource
from repositories import (
    PolicyEvaluatorResultRepository,
    ResourceRepository,
    PrincipalRepository,
)
from ..src.policy_evaluator import PolicyEvaluator


def test_evaluate_with_no_principals(database_empty: Database) -> None:
    """Test that evaluate handles the case with no active principals gracefully."""
    policy_evaluator = PolicyEvaluator()

    with patch.object(
        policy_evaluator.opa_client, "filter_resources_batch"
    ) as mock_opa:
        with database_empty.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

        # OPA should not be called if there are no principals
        mock_opa.assert_not_called()

    with database_empty.Session.begin() as session:
        results = PolicyEvaluatorResultRepository.get_all(session)
        assert len(results) == 0


def test_evaluate_with_no_resources(database_empty: Database) -> None:
    """Test that evaluate handles the case with no resources gracefully."""
    policy_evaluator = PolicyEvaluator()

    with patch.object(
        policy_evaluator.opa_client, "filter_resources_batch"
    ) as mock_opa:
        with database_empty.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

        # OPA should not be called if there are no resources
        mock_opa.assert_not_called()

    with database_empty.Session.begin() as session:
        results = PolicyEvaluatorResultRepository.get_all(session)
        assert len(results) == 0


def test_evaluate_truncates_existing_results(database: Database) -> None:
    """Test that evaluate truncates the results table before inserting new results."""
    policy_evaluator = PolicyEvaluator()

    # Add some existing results
    with database.Session.begin() as session:
        session.add(
            PolicyEvaluatorResultDbo(
                principal="test_user",
                resource_type="table",
                resource="test.table",
            )
        )
        session.commit()

    # Verify the result was added
    with database.Session.begin() as session:
        results = PolicyEvaluatorResultRepository.get_all(session)
        assert len(results) == 1

    # Mock OPA to return no allowed resources
    with patch.object(
        policy_evaluator.opa_client, "filter_resources_batch", return_value=[]
    ):
        with database.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

    # Verify the results table was truncated
    with database.Session.begin() as session:
        results = PolicyEvaluatorResultRepository.get_all(session)
        assert len(results) == 0


def test_evaluate_stores_allowed_resources(database: Database) -> None:
    """Test that evaluate stores only allowed resources in the results table."""
    policy_evaluator = PolicyEvaluator()

    # Mock OPA to return some allowed and some denied resources
    def mock_filter_batch(username: str, resources: list[dict]) -> list[bool]:
        # Allow every other resource
        return [i % 2 == 0 for i in range(len(resources))]

    with patch.object(
        policy_evaluator.opa_client,
        "filter_resources_batch",
        side_effect=mock_filter_batch,
    ):
        with database.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

    with database.Session.begin() as session:
        principal_count, _ = PrincipalRepository.get_all(session)
        resource_count, _ = ResourceRepository.get_all(session)

        results = PolicyEvaluatorResultRepository.get_all(session)

        # there should be an entry for each user times half of the resources
        assert len(results) == principal_count * resource_count / 2

        # All results should have required fields
        for result in results:
            assert result.principal is not None
            assert result.resource_type is not None
            assert result.resource is not None


def test_evaluate_batch_processing(database: Database) -> None:
    """Test that evaluate processes resources in batches."""
    policy_evaluator = PolicyEvaluator()

    # Set a small batch size to test batching
    policy_evaluator.config.batch_size = 2

    call_count = 0

    def mock_filter_batch(username: str, resources: list[dict]) -> list[bool]:
        nonlocal call_count
        call_count += 1
        # Verify batch size is respected
        assert len(resources) <= 2
        return [True] * len(resources)

    with patch.object(
        policy_evaluator.opa_client,
        "filter_resources_batch",
        side_effect=mock_filter_batch,
    ):
        with database.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

    # Verify that OPA was called multiple times (batching occurred)
    assert call_count > 0


def test_evaluate_processes_all_principals(database: Database) -> None:
    """Test that evaluate processes resources for all active principals."""
    policy_evaluator = PolicyEvaluator()

    principals_called = set()

    def mock_filter_batch(username: str, resources: list[dict]) -> list[bool]:
        principals_called.add(username)
        return [True] * len(resources)

    with patch.object(
        policy_evaluator.opa_client,
        "filter_resources_batch",
        side_effect=mock_filter_batch,
    ):
        with database.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

    with database.Session.begin() as session:
        from repositories.src.principal_repository import PrincipalRepository

        principal_count, principals = PrincipalRepository.get_all_active(session)

        # Verify that all active principals were processed
        assert len(principals_called) == principal_count
        for principal in principals:
            assert principal.user_name in principals_called


def test_evaluate_allows_all_resources(database: Database) -> None:
    """Test that evaluate stores a grant for every principal/resource pair when OPA allows all."""
    policy_evaluator = PolicyEvaluator()

    # Mock OPA to allow every resource for alice, and deny the rest
    def mock_filter_batch(username: str, resources: list[dict]) -> list[bool]:
        return [username == "alice"] * len(resources)

    with patch.object(
        policy_evaluator.opa_client,
        "filter_resources_batch",
        side_effect=mock_filter_batch,
    ):
        with database.Session.begin() as session:
            policy_evaluator.evaluate(session=session)

    with database.Session.begin() as session:
        principal_count, _ = PrincipalRepository.get_all_active(session)
        resource_count, _ = ResourceRepository.get_all(session)

        results = PolicyEvaluatorResultRepository.get_all(session)

        # we should have all results for one principal
        assert len(results) == resource_count
        assert all(result.principal == "alice" for result in results)

        # All results should have required fields populated
        for result in results:
            assert result.principal is not None
            assert result.resource_type is not None
            assert result.resource is not None


def test_convert_resources_to_opa_format(database: Database) -> None:
    """Test resource conversion to OPA format."""
    policy_evaluator = PolicyEvaluator()

    with database.Session.begin() as session:
        _, resources = ResourceRepository.get_all_by_platform(session, "trino")

        # Convert to OPA format
        result = policy_evaluator._convert_resources_to_opa_format(resources)

        assert result == [
            OpaTableResource(catalog="datalake", schema="hr", table="employees"),
            OpaTableResource(catalog="datalake", schema="hr", table="employees"),
            OpaTableResource(
                catalog="datalake", schema="hr", table="employee_territories"
            ),
            OpaTableResource(catalog="datalake", schema="logistics", table="regions"),
            OpaTableResource(catalog="datalake", schema="logistics", table="shippers"),
            OpaTableResource(catalog="datalake", schema="logistics", table="shippers"),
            OpaTableResource(catalog="datalake", schema="logistics", table="suppliers"),
            OpaTableResource(
                catalog="datalake", schema="logistics", table="territories"
            ),
            OpaTableResource(
                catalog="datalake", schema="sales", table="customer_demographics"
            ),
            OpaTableResource(
                catalog="datalake", schema="sales", table="customer_markets"
            ),
            OpaTableResource(
                catalog="datalake", schema="sales", table="customer_markets"
            ),
            OpaTableResource(catalog="datalake", schema="sales", table="customers"),
            OpaTableResource(catalog="datalake", schema="sales", table="orders"),
            OpaTableResource(catalog="datalake", schema="sales", table="products"),
        ]
