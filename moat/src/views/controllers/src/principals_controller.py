from typing import Tuple

from app_logger import Logger, get_logger
from models import AttributeDto, PrincipalDbo
from repositories import PrincipalRepository

logger: Logger = get_logger("controller.principals")


class PrincipalsController:

    @staticmethod
    def get_all_principals_with_search_pagination_and_attr_filter(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
        attributes: list[AttributeDto] = None,
    ) -> Tuple[int, list[PrincipalDbo]]:
        repo: PrincipalRepository = PrincipalRepository()

        # if we dont have attributes we can use the fast one
        # if attributes is None:
        return repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            sort_ascending=sort_ascending,
            search_term=search_term,
        )
        # sort_col_name = f"principals.{sort_col_name}"
        # return repo.get_all_with_search_and_pagination_and_attr_filter(
        #     session=session,
        #     sort_col_name=sort_col_name,
        #     page_number=page_number,
        #     page_size=page_size,
        #     sort_ascending=sort_ascending,
        #     search_term=search_term,
        #     attributes=attributes,
        # )

    # @staticmethod
    # def get_principal_attributes_by_username(
    #     session, user_name: str
    # ) -> list[AttributeDto]:
    #     repo: PrincipalRepository = PrincipalRepository()
    #     principal: PrincipalDbo = repo.get_principal_with_attributes(
    #         session=session, user_name=user_name
    #     )
    #     if principal is None:
    #         logger.error(f"User {user_name} could not be found")
    #         raise ValueError(f"User {user_name} could not be found")
    #
    #     return [
    #         AttributeDto(
    #             attribute_key=a.attribute_key, attribute_value=a.attribute_value
    #         )
    #         for a in principal.principal_attributes + principal.group_attributes
    #     ]
