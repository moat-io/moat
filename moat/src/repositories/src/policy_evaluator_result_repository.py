from dataclasses import dataclass
from typing import Tuple

from models import PolicyEvaluatorResultDbo
from sqlalchemy.orm import Query

from .repository_base import RepositoryBase


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
