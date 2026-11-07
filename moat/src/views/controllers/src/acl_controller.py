from models import PolicyEvaluatorResultDbo
from repositories import PolicyEvaluatorResultRepository


class AclController:

    @staticmethod
    def get_grants_paginated(
        session,
        sort_col_name: str,
        sort_ascending: bool,
        page_number: int,
        page_size: int,
        search_term: str,
    ) -> tuple[int, list[PolicyEvaluatorResultDbo]]:
        """
        The table only ever shows the grants that currently hold - a withdrawn
        grant is kept in the table but is no longer part of the ACL.
        """
        return PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session,
            page_number=page_number,
            page_size=page_size,
            search_term=search_term,
            sort_col_name=sort_col_name,
            sort_ascending=sort_ascending,
            active=True,
        )
