from typing import Tuple

from models import OpaBundleDbo
from .repository_base import RepositoryBase


class OpaBundleRepository(RepositoryBase):

    def get_all_with_search_and_pagination(
        self,
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
    ) -> Tuple[int, list[OpaBundleDbo]]:
        return self._get_all_with_search_and_pagination(
            session=session,
            model=OpaBundleDbo,
            page_number=page_number,
            page_size=page_size,
            search_column_names=["platform", "e_tag", "bundle_filename", "policy_hash"],
            sort_ascending=sort_ascending,
            sort_col_name=sort_col_name,
            search_term=search_term,
        )

    def get_by_id(self, session, opa_bundle_id: int) -> OpaBundleDbo:
        return session.query(OpaBundleDbo).filter_by(opa_bundle_id=opa_bundle_id).first()
