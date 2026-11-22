from dataclasses import dataclass

from sqlalchemy import text


@dataclass
class RoleRepositoryResultCounts:
    inserted_or_updated: int = 0
    deleted: int = 0


class RoleRepository:
    @staticmethod
    def reconcile(
        session, max_users_per_role: int, max_resources_per_role: int
    ) -> RoleRepositoryResultCounts:
        """Rebuild active ACL role assignments in SQL, writing only mapping deltas."""
        is_mysql = session.get_bind().dialect.name == "mysql"
        role_id_expression = (
            f"""CONCAT(
                CAST(length(platform) AS CHAR), ':', platform,
                CAST(length(action) AS CHAR), ':', action, ':',
                CAST(FLOOR((principal_rank - 1) / :max_users) AS CHAR), ':',
                CAST(FLOOR((resource_rank - 1) / :max_resources) AS CHAR)
            )"""
            if is_mysql
            else """CAST(length(platform) AS TEXT) || ':' || platform ||
                CAST(length(action) AS TEXT) || ':' || action || ':' ||
                CAST(CAST(FLOOR((principal_rank - 1) / :max_users) AS BIGINT) AS TEXT) || ':' ||
                CAST(CAST(FLOOR((resource_rank - 1) / :max_resources) AS BIGINT) AS TEXT)"""
        )
        cte = f"""
            WITH ranked_acl AS (
                SELECT
                    id,
                    platform,
                    action,
                    principal_fq_name,
                    resource_fq_name,
                    dense_rank() OVER (
                        PARTITION BY platform, action
                        ORDER BY principal_fq_name
                    ) AS principal_rank,
                    dense_rank() OVER (
                        PARTITION BY platform, action
                        ORDER BY resource_fq_name
                    ) AS resource_rank
                FROM policy_evaluator_results
                WHERE active IS TRUE
                  AND platform IS NOT NULL
                  AND action IS NOT NULL
                  AND principal_fq_name IS NOT NULL
                  AND resource_fq_name IS NOT NULL
            ),
            assignments AS (
                SELECT
                    id AS policy_evaluator_result_id,
                    {role_id_expression} AS role_id
                FROM ranked_acl
            )
        """
        upsert_statement = (
            """
            ON DUPLICATE KEY UPDATE
                role_id = IF(roles.role_id <> VALUES(role_id), VALUES(role_id), roles.role_id)
            """
            if is_mysql
            else """
            ON CONFLICT (policy_evaluator_result_id) DO UPDATE
            SET role_id = excluded.role_id
            WHERE roles.role_id <> excluded.role_id
            """
        )
        assignment_query = (
            f"""
            INSERT INTO roles (policy_evaluator_result_id, role_id)
            {cte}
            SELECT policy_evaluator_result_id, role_id FROM assignments
            {upsert_statement}
            """
            if is_mysql
            else f"""
            {cte}
            INSERT INTO roles (policy_evaluator_result_id, role_id)
            SELECT policy_evaluator_result_id, role_id FROM assignments
            WHERE TRUE
            {upsert_statement}
            """
        )
        assignment_result = session.execute(
            text(assignment_query),
            {
                "max_users": max_users_per_role,
                "max_resources": max_resources_per_role,
            },
        )
        inserted_or_updated = assignment_result.rowcount or 0

        cleanup_statement = (
            """
            DELETE mapping FROM roles AS mapping
            WHERE NOT EXISTS (
                SELECT 1
                FROM policy_evaluator_results AS acl
                WHERE acl.id = mapping.policy_evaluator_result_id
                  AND acl.active IS TRUE
                  AND acl.platform IS NOT NULL
                  AND acl.action IS NOT NULL
                  AND acl.principal_fq_name IS NOT NULL
                  AND acl.resource_fq_name IS NOT NULL
            )
            """
            if is_mysql
            else """
            DELETE FROM roles AS mapping
            WHERE NOT EXISTS (
                SELECT 1
                FROM policy_evaluator_results AS acl
                WHERE acl.id = mapping.policy_evaluator_result_id
                  AND acl.active IS TRUE
                  AND acl.platform IS NOT NULL
                  AND acl.action IS NOT NULL
                  AND acl.principal_fq_name IS NOT NULL
                  AND acl.resource_fq_name IS NOT NULL
            )
            """
        )
        cleanup_result = session.execute(text(cleanup_statement))
        return RoleRepositoryResultCounts(
            inserted_or_updated=max(inserted_or_updated, 0),
            deleted=max(cleanup_result.rowcount or 0, 0),
        )
