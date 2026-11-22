from database import Database
from models import PolicyEvaluatorResultDbo, RoleDbo
from repositories import RoleRepository


def test_reconcile_assigns_roles_within_limits_and_updates_deltas(
    database_empty: Database,
) -> None:
    with database_empty.Session.begin() as session:
        acl_rows = [
            PolicyEvaluatorResultDbo(
                principal_fq_name=f"role-repository-test/user-{user}",
                resource_fq_name=f"catalog.schema.role-repository-test-{resource}",
                platform="role-repository-test-platform",
                action="role-repository-test-action",
                active=True,
            )
            for user in range(3)
            for resource in range(4)
        ]
        acl_rows.extend(
            [
                PolicyEvaluatorResultDbo(
                    principal_fq_name="role-repository-test/user-0",
                    resource_fq_name="catalog.schema.role-repository-test-0",
                    platform="role-repository-other-platform",
                    action="role-repository-test-action",
                    active=True,
                ),
                PolicyEvaluatorResultDbo(
                    principal_fq_name="role-repository-test/user-0",
                    resource_fq_name="catalog.schema.role-repository-test-0",
                    platform="role-repository-test-platform",
                    action="role-repository-other-action",
                    active=True,
                ),
            ]
        )
        session.add_all(acl_rows)
        session.flush()
        acl_ids = [row.id for row in acl_rows]

        RoleRepository.reconcile(
            session=session, max_users_per_role=2, max_resources_per_role=2
        )
        mappings = (
            session.query(RoleDbo)
            # .filter(RoleDbo.policy_evaluator_result_id.in_(acl_ids))
            .all()
        )
        assert len(mappings) == len(acl_rows)

        acl_by_id = {row.id: row for row in acl_rows}
        grouped: dict[str, list[PolicyEvaluatorResultDbo]] = {}
        for mapping in mappings:
            grouped.setdefault(mapping.role_id, []).append(
                acl_by_id[mapping.policy_evaluator_result_id]
            )

        assert len(grouped) == 6
        for role_rows in grouped.values():
            assert len({row.platform for row in role_rows}) == 1
            assert len({row.action for row in role_rows}) == 1
            assert len({row.principal_fq_name for row in role_rows}) <= 2
            assert len({row.resource_fq_name for row in role_rows}) <= 2

        initial_assignments = {
            (row.policy_evaluator_result_id, row.role_id) for row in mappings
        }
        RoleRepository.reconcile(
            session=session, max_users_per_role=2, max_resources_per_role=2
        )
        assert {
            (row.policy_evaluator_result_id, row.role_id)
            for row in session.query(RoleDbo)
            .filter(RoleDbo.policy_evaluator_result_id.in_(acl_ids))
            .all()
        } == initial_assignments

        acl_rows[0].active = False
        session.flush()
        RoleRepository.reconcile(
            session=session, max_users_per_role=2, max_resources_per_role=2
        )
        assert (
            session.query(RoleDbo)
            .filter(RoleDbo.policy_evaluator_result_id == acl_rows[0].id)
            .count()
            == 0
        )
