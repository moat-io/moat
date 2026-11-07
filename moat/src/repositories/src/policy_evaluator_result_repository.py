from dataclasses import dataclass
from typing import Tuple

from models import PolicyEvaluatorResultDbo
from sqlalchemy.orm import Query

from .repository_base import RepositoryBase

# the columns a free text search looks at, and the ones a client may sort on
GRANT_SEARCH_COLUMNS: list[str] = [
    "principal_fq_name",
    "resource_fq_name",
    "platform",
    "action",
]
GRANT_SORT_COLUMNS: set[str] = set(GRANT_SEARCH_COLUMNS) | {"record_updated_date"}
DEFAULT_GRANT_SORT_COLUMN: str = "principal_fq_name"


@dataclass
class MergeResultCounts:
    inserted: int = 0
    reactivated: int = 0
    deactivated: int = 0


class PolicyEvaluatorResultRepository(RepositoryBase):

    @staticmethod
    def get_all_active(session) -> Tuple[int, list[PolicyEvaluatorResultDbo]]:
        query: Query = session.query(PolicyEvaluatorResultDbo).filter(
            PolicyEvaluatorResultDbo.active == True
        )
        return query.count(), query.all()

    @staticmethod
    def get_grants_paginated(
        session,
        page_number: int,
        page_size: int,
        search_term: str = "",
        sort_col_name: str | None = None,
        sort_ascending: bool = True,
        active: bool | None = True,
    ) -> Tuple[int, list[PolicyEvaluatorResultDbo]]:
        """
        One page of the evaluated ACL: (principal, resource, platform, action)
        grants. Shared by the ACL API and the UI table so the two cannot drift.

        `active=True` (the default) returns only the grants that currently hold -
        a grant the evaluator has since withdrawn is soft-deleted, not removed.
        `None` returns both.

        An unknown `sort_col_name` falls back to the default rather than raising,
        as it arrives straight off a query string.
        """
        query: Query = session.query(PolicyEvaluatorResultDbo)

        if active is not None:
            query = query.filter(PolicyEvaluatorResultDbo.active == active)

        if search_term:
            query = RepositoryBase._get_search_query(
                query=query,
                search_column_names=[
                    f"{PolicyEvaluatorResultDbo.__tablename__}.{column}"
                    for column in GRANT_SEARCH_COLUMNS
                ],
                search_term=search_term,
            )

        count: int = query.count()

        sort_column: str = (
            sort_col_name
            if sort_col_name in GRANT_SORT_COLUMNS
            else DEFAULT_GRANT_SORT_COLUMN
        )
        query = RepositoryBase._get_sort_query(
            query=query,
            sort_col_name=f"{PolicyEvaluatorResultDbo.__tablename__}.{sort_column}",
            sort_ascending=sort_ascending,
        )
        # tie break on the id, so paging through a non unique sort column such as
        # platform cannot repeat or skip a row
        query = query.order_by(PolicyEvaluatorResultDbo.id)

        query = RepositoryBase._get_pagination_query(
            query=query, page_number=page_number, page_size=page_size
        )

        return count, query.all()

    @staticmethod
    def merge_results(
        session,
        platform: str,
        action: str,
        grants: list[Tuple[str, str]],
    ) -> MergeResultCounts:
        """
        Reconcile the stored evaluation results for a given platform/action with
        the freshly computed set of allowed (principal_fq_name, resource_fq_name)
        grants.

        * grants not yet present are inserted (record_created_date defaults to now)
        * grants present but previously soft-deleted are reactivated
        * stored active grants no longer present are soft-deleted

        Any change to an existing row is a plain UPDATE, so the Postgres
        record_updated_date trigger stamps a fresh date on it.
        """
        counts: MergeResultCounts = MergeResultCounts()

        desired: set[Tuple[str, str]] = set(grants)

        existing: list[PolicyEvaluatorResultDbo] = (
            session.query(PolicyEvaluatorResultDbo)
            .filter(
                PolicyEvaluatorResultDbo.platform == platform,
                PolicyEvaluatorResultDbo.action == action,
            )
            .all()
        )
        existing_by_key: dict[Tuple[str, str], PolicyEvaluatorResultDbo] = {
            (row.principal_fq_name, row.resource_fq_name): row for row in existing
        }

        # insert / reactivate
        for key in desired:
            row: PolicyEvaluatorResultDbo | None = existing_by_key.get(key)
            if row is None:
                principal_fq_name, resource_fq_name = key
                session.add(
                    PolicyEvaluatorResultDbo(
                        principal_fq_name=principal_fq_name,
                        resource_fq_name=resource_fq_name,
                        platform=platform,
                        action=action,
                        active=True,
                    )
                )
                counts.inserted += 1
            elif not row.active:
                row.active = True
                counts.reactivated += 1

        # soft-delete grants that are no longer allowed
        for key, row in existing_by_key.items():
            if key not in desired and row.active:
                row.active = False
                counts.deactivated += 1

        return counts
