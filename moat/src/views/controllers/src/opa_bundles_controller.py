from typing import Tuple

from app_logger import Logger, get_logger
from models import OpaBundleDbo
from repositories import OpaBundleRepository

logger: Logger = get_logger("controller.opa_bundles")


class OpaBundlesController:

    @staticmethod
    def get_all_opa_bundles_with_search_and_pagination(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
    ) -> Tuple[int, list[OpaBundleDbo]]:
        repo: OpaBundleRepository = OpaBundleRepository()

        return repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            sort_ascending=sort_ascending,
            search_term=search_term,
        )

    @staticmethod
    def get_opa_bundle_by_id(session, opa_bundle_id: int) -> OpaBundleDbo:
        repo: OpaBundleRepository = OpaBundleRepository()
        return repo.get_by_id(session=session, opa_bundle_id=opa_bundle_id)
