import responses
from database import Database
from models import PolicyEvaluatorResultDbo
from repositories import PrincipalRepository, ResourceRepository

from ..src.policy_evaluator import PolicyEvaluator

OPA_BATCH_URL = "http://localhost:8181/v1/data/moat/trino/batch"


def _seed_facts(database: Database):
    """The active principals and sorted trino table fq_names from the seed data.

    The evaluator lists resources ordered by fq_name and drops non-table object
    types, so OPA batch indices line up with this sorted table list.
    """
    with database.Session.begin() as session:
        _, principals = PrincipalRepository.get_all_active(session=session)
        principal_fq_names = [p.fq_name for p in principals]

        _, resources = ResourceRepository.get_all_by_platform(
            session=session, platform="trino"
        )
        tables = [r.fq_name for r in resources if r.object_type == "table"]

    return principal_fq_names, tables


def _active_results(database: Database) -> list[PolicyEvaluatorResultDbo]:
    with database.Session.begin() as session:
        rows = (
            session.query(PolicyEvaluatorResultDbo)
            .filter(PolicyEvaluatorResultDbo.active == True)
            .all()
        )
        session.expunge_all()
        return rows


def _all_results(database: Database) -> list[PolicyEvaluatorResultDbo]:
    with database.Session.begin() as session:
        rows = session.query(PolicyEvaluatorResultDbo).all()
        session.expunge_all()
        return rows


def test_evaluate_merges_and_soft_deletes(database: Database) -> None:
    principal_fq_names, tables = _seed_facts(database)
    assert len(principal_fq_names) > 0
    assert len(tables) >= 3

    # --- first run: OPA allows the first three tables for every principal ---
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            OPA_BATCH_URL,
            json={"result": [0, 1, 2]},
            status=200,
        )
        PolicyEvaluator().evaluate()

    expected_first = {
        (principal_fq_name, table)
        for principal_fq_name in principal_fq_names
        for table in tables[:3]
    }

    active_after_first = _active_results(database)
    active_keys_first = {
        (r.principal_fq_name, r.resource_fq_name) for r in active_after_first
    }
    assert active_keys_first == expected_first
    assert all(r.platform == "trino" for r in active_after_first)
    assert all(r.action == "FilterTables" for r in active_after_first)
    assert all(r.record_created_date is not None for r in active_after_first)

    # --- second run: OPA now allows only the first table ---
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            OPA_BATCH_URL,
            json={"result": [0]},
            status=200,
        )
        PolicyEvaluator().evaluate()

    expected_second = {
        (principal_fq_name, tables[0]) for principal_fq_name in principal_fq_names
    }

    active_after_second = _active_results(database)
    active_keys_second = {
        (r.principal_fq_name, r.resource_fq_name) for r in active_after_second
    }
    assert active_keys_second == expected_second

    # the revoked grants are soft-deleted rather than removed, so the total row
    # count is unchanged from the first run
    all_rows = _all_results(database)
    assert len(all_rows) == len(expected_first)

    soft_deleted = [r for r in all_rows if not r.active]
    assert len(soft_deleted) == len(expected_first) - len(expected_second)
    # the soft-deleted rows are the tables that are no longer allowed
    assert {r.resource_fq_name for r in soft_deleted} == set(tables[1:3])
