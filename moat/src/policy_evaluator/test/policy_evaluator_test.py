from unittest import mock
from unittest.mock import Mock

import responses
from database import Database
from models import OpaBundleDbo, PolicyEvaluatorResultDbo, ResourceDbo
from repositories import PrincipalRepository, ResourceRepository
from services.bundle import BundleService
from services.bundle.src.bundler_config import BundlerConfig

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


def _generate_bundle(database: Database, tmp_path) -> OpaBundleDbo:
    with mock.patch.object(BundlerConfig, "bundle_directory", str(tmp_path)):
        with database.Session.begin() as session:
            opa_bundle = BundleService.generate_bundle(session=session, platform="trino")
            session.flush()
            session.expunge(opa_bundle)
    return opa_bundle


def test_evaluate_merges_and_soft_deletes(database: Database, tmp_path) -> None:
    opa_bundle = _generate_bundle(database, tmp_path)
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
    assert all(r.bundle_e_tag == opa_bundle.e_tag for r in active_after_first)
    assert all(
        r.bundle_policy_hash == opa_bundle.policy_hash for r in active_after_first
    )

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


def _principal(attributes: list[str]) -> dict:
    return {"attributes": attributes, "entitlements": [], "groups": []}


def test_evaluate_grants_by_cohort_with_special_users() -> None:
    # alice and bob share a profile (attribute order differs), carol differs,
    # dave shares it too but is special
    principals = {
        "alice": _principal(["Commercial::Sales", "Privacy::HR"]),
        "bob": _principal(["Privacy::HR", "Commercial::Sales"]),
        "carol": _principal(["Commercial::HR"]),
        "dave": _principal(["Commercial::Sales", "Privacy::HR"]),
        # in the bundle but no longer in the DB
        "erin": _principal(["Commercial::HR"]),
    }
    fq_names = {name: f"fq.{name}" for name in ["alice", "bob", "carol", "dave"]}
    tables = [ResourceDbo(fq_name=f"c.s.t{i}", object_type="table") for i in range(3)]

    evaluator = PolicyEvaluator()
    evaluator.config.batch_size = 2
    evaluator.config.max_workers = 4
    evaluator.config.special_users = " dave ,unknown"
    evaluator.opa_client = Mock()
    # every user is allowed only the first table of each batch
    evaluator.opa_client.filter_tables_batch.return_value = [0]

    grants = evaluator._evaluate_grants(
        principals=principals, fq_names=fq_names, tables=tables
    )

    called_users = [
        call.kwargs["username"]
        for call in evaluator.opa_client.filter_tables_batch.call_args_list
    ]
    # 3 cohorts (alice+bob, carol, dave) x 2 batches
    assert sorted(called_users) == ["alice", "alice", "carol", "carol", "dave", "dave"]
    assert sorted(grants) == sorted(
        (f"fq.{name}", table)
        for name in ["alice", "bob", "carol", "dave"]
        for table in ["c.s.t0", "c.s.t2"]
    )

