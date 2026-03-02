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
    def get_tables_paginated_with_access(
        session,
        logged_in_user: str | None,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        search_term: str,
        attributes: list[AttributeDto] = None,
    ) -> Tuple[int, list[ResourceDbo]]:
        table_count, tables = ResourceRepository.get_all_with_search_and_pagination(
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            search_term=search_term,
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
