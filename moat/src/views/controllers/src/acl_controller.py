from typing import Tuple

from models import PolicyEvaluatorResultDbo
from repositories import PolicyEvaluatorResultRepository


class AclController:
    @staticmethod
    def get_acls_paginated(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
    ) -> Tuple[int, list[PolicyEvaluatorResultDbo]]:
        return PolicyEvaluatorResultRepository.get_all_with_search_and_pagination(
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            sort_ascending=sort_ascending,
            search_term=search_term,
        )
