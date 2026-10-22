from typing import Tuple

from app_logger import Logger, get_logger
from models import AttributeDto, ResourceDbo

# from models.src.dtos.schema_dto import SchemaDto
from repositories import ResourceRepository

from opa import OpaClient

logger: Logger = get_logger("controller.resources")


class ResourcesController:

    # @staticmethod
    # def get_table_by_id(session, table_id: int) -> TableDbo:
    #     table: TableDbo = ResourceRepository.get_table_by_id(
    #         session=session, table_id=table_id
    #     )
    #     return table

    @staticmethod
    def get_all_resources(session) -> Tuple[int, list[ResourceDbo]]:
        return ResourceRepository.get_all(session=session)

    @staticmethod
    def get_resources_matching_filter(
        session,
        sort_col_name: str = "fq_name",
        sort_ascending: bool = True,
        search_term: str = "",
        platform: list[str] = None,
        object_type: list[str] = None,
        active: bool | None = None,
        attributes: list[AttributeDto] = None,
    ) -> Tuple[int, list[ResourceDbo]]:
        """Unpaginated counterpart of the table query, for CSV download."""
        return ResourceRepository.get_all_with_search(
            session=session,
            sort_col_name=sort_col_name,
            sort_ascending=sort_ascending,
            search_term=search_term,
            platform=platform,
            object_type=object_type,
            active=active,
            attributes=attributes,
        )

    @staticmethod
    def get_filter_options(session) -> dict[str, list[str]]:
        return ResourceRepository.get_filter_options(session=session)

    @staticmethod
    def get_attribute_values(session, attribute_key: str) -> list[str]:
        return ResourceRepository.get_attribute_values(
            session=session, attribute_key=attribute_key
        )

    @staticmethod
    def get_tables_paginated_with_access(
        session,
        logged_in_user: str | None,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        search_term: str,
        attributes: list[AttributeDto] = None,
        platform: list[str] = None,
        object_type: list[str] = None,
        active: bool | None = None,
        sort_ascending: bool = True,
    ) -> Tuple[int, list[ResourceDbo]]:
        table_count, tables = ResourceRepository.get_all_with_search_and_pagination(
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            sort_ascending=sort_ascending,
            search_term=search_term,
            platform=platform,
            object_type=object_type,
            active=active,
            attributes=attributes,
        )

        # if logged_in_user:
        #     opa_client: OpaClient = OpaClient()
        #     table: TableDto
        #     for table in tables:
        #         try:
        #             table.accessible = opa_client.filter_table(
        #                 username=logged_in_user,
        #                 database=table.database_name,
        #                 schema=table.schema_name,
        #                 table=table.table_name,
        #             )
        #         except Exception as e:
        #             logger.exception(
        #                 f"Failed to get accessibility for {logged_in_user} on {table.f_q_table_name}"
        #             )
        # else:
        #     logger.info("No logged in user, skipping OPA requests")
        return table_count, tables

    # @staticmethod
    # def get_schemas_paginated_with_access(
    #     session,
    #     logged_in_user: str,
    #     sort_col_name: str,
    #     page_number: int,
    #     page_size: int,
    #     search_term: str,
    #     attributes: list[AttributeDto] = None,
    # ) -> Tuple[int, list[SchemaDto]]:
    #     schema_count, schemas = (
    #         ResourceRepository.get_all_schemas_with_search_and_pagination(
    #             session=session,
    #             sort_col_name=sort_col_name,
    #             page_number=page_number,
    #             page_size=page_size,
    #             search_term=search_term,
    #         )
    #     )
    #
    #     opa_client: OpaClient = OpaClient()
    #     schema: SchemaDto
    #     for schema in schemas:
    #         try:
    #             schema.accessible = opa_client.filter_schema(
    #                 username=logged_in_user,
    #                 database=schema.database_name,
    #                 schema=schema.schema_name,
    #             )
    #         except Exception as e:
    #             logger.exception(
    #                 f"Failed to get accessibility for {logged_in_user} on {schema.f_q_schema_name}"
    #             )
    #
    #     return schema_count, schemas
