from models import PolicyEvaluatorResultDbo
from .repository_base import RepositoryBase


class PolicyEvaluatorResultRepository(RepositoryBase):
    @staticmethod
    def truncate_table(session) -> None:
        RepositoryBase.truncate_tables(
            session=session, models=[PolicyEvaluatorResultDbo]
        )

    @staticmethod
    def bulk_insert(session, results: list[PolicyEvaluatorResultDbo]) -> int:
        """
        Bulk insert policy evaluator results.

        Args:
            session: Database session
            results: List of PolicyEvaluatorResultDbo objects to insert

        Returns:
            Number of rows inserted
        """
        session.bulk_save_objects(results)
        return len(results)

    @staticmethod
    def get_all(session) -> list[PolicyEvaluatorResultDbo]:
        """Get all policy evaluator results."""
        return session.query(PolicyEvaluatorResultDbo).all()

    @staticmethod
    def get_by_principal(session, principal: str) -> list[PolicyEvaluatorResultDbo]:
        """Get all policy evaluator results for a specific principal."""
        return (
            session.query(PolicyEvaluatorResultDbo)
            .filter(PolicyEvaluatorResultDbo.principal == principal)
            .all()
        )
