from datetime import datetime
from typing import Tuple

from models import (
    AttributeDto,
    ResourceAttributeDbo,
    ResourceAttributeHistoryDbo,
    ResourceAttributeStagingDbo,
    ResourceDbo,
    ResourceHistoryDbo,
    ResourceStagingDbo,
)
from sqlalchemy import desc
from sqlalchemy.orm import Query
from sqlalchemy.sql import func, text

from .repository_base import RepositoryBase

RESOURCE_SEARCH_COLUMNS = [ResourceDbo.fq_name]


class ResourceRepository(RepositoryBase):
    # TODO base class
    @staticmethod
    def get_all(session) -> Tuple[int, list[ResourceDbo]]:
        query: Query = session.query(ResourceDbo)
        return query.count(), query.all()

    @staticmethod
    def get_all_by_platform(session, platform: str) -> Tuple[int, list[ResourceDbo]]:
        query: Query = (
            session.query(ResourceDbo)
            .filter(ResourceDbo.platform == platform)
            .order_by(ResourceDbo.fq_name)
        )
        return query.count(), query.all()

    @staticmethod
    def _get_filtered_query(
        session,
        search_term: str = "",
        platform: list[str] | None = None,
        object_type: list[str] | None = None,
        active: bool | None = None,
        attributes: list[AttributeDto] | None = None,
    ) -> Query:
        """
        The single definition of 'which resources match the current filters'.
        Shared by the paginated table and the CSV download so the two can never
        drift apart.
        """
        query: Query = session.query(ResourceDbo)

        query = RepositoryBase._get_attribute_search_query(
            query=query,
            model=ResourceDbo,
            attribute_model=ResourceAttributeDbo,
            search_columns=RESOURCE_SEARCH_COLUMNS,
            search_term=search_term,
        )
        query = RepositoryBase._get_column_filter_query(
            query=query, column=ResourceDbo.platform, values=platform
        )
        query = RepositoryBase._get_column_filter_query(
            query=query, column=ResourceDbo.object_type, values=object_type
        )
        if active is not None:
            query = query.filter(ResourceDbo.active == active)
        query = RepositoryBase._get_attribute_filter_query(
            query=query,
            model=ResourceDbo,
            attribute_model=ResourceAttributeDbo,
            attributes=attributes,
        )
        return query

    @staticmethod
    def get_all_with_search_and_pagination(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
        platform: list[str] | None = None,
        object_type: list[str] | None = None,
        active: bool | None = None,
        attributes: list[AttributeDto] | None = None,
    ) -> Tuple[int, list[ResourceDbo]]:
        query: Query = ResourceRepository._get_filtered_query(
            session=session,
            search_term=search_term,
            platform=platform,
            object_type=object_type,
            active=active,
            attributes=attributes,
        )

        count: int = query.count()

        if sort_col_name:
            sort_column = RepositoryBase.get_column_by_name(
                table_name=ResourceDbo.__tablename__, column_name=sort_col_name
            )
            query = query.order_by(sort_column if sort_ascending else desc(sort_column))

        query = RepositoryBase._get_pagination_query(
            query=query, page_number=page_number, page_size=page_size
        )

        results: list[ResourceDbo] = query.all()
        return count, results

    @staticmethod
    def get_all_with_search(
        session,
        sort_col_name: str = "fq_name",
        sort_ascending: bool = True,
        search_term: str = "",
        platform: list[str] | None = None,
        object_type: list[str] | None = None,
        active: bool | None = None,
        attributes: list[AttributeDto] | None = None,
    ) -> Tuple[int, list[ResourceDbo]]:
        """Every resource matching the filters, unpaginated. Used by the CSV download."""
        query: Query = ResourceRepository._get_filtered_query(
            session=session,
            search_term=search_term,
            platform=platform,
            object_type=object_type,
            active=active,
            attributes=attributes,
        )

        sort_column = RepositoryBase.get_column_by_name(
            table_name=ResourceDbo.__tablename__, column_name=sort_col_name
        )
        query = query.order_by(sort_column if sort_ascending else desc(sort_column))

        results: list[ResourceDbo] = query.all()
        return len(results), results

    @staticmethod
    def get_filter_options(session) -> dict[str, list[str]]:
        """Distinct values used to populate the advanced filter controls."""
        return {
            "platforms": RepositoryBase._get_distinct_values(
                session=session, column=ResourceDbo.platform
            ),
            "object_types": RepositoryBase._get_distinct_values(
                session=session, column=ResourceDbo.object_type
            ),
            "attribute_keys": RepositoryBase._get_distinct_values(
                session=session, column=ResourceAttributeDbo.attribute_key
            ),
        }

    @staticmethod
    def get_attribute_values(session, attribute_key: str) -> list[str]:
        """Distinct values for one attribute key, for the dependent value dropdown."""
        rows = (
            session.query(ResourceAttributeDbo.attribute_value)
            .filter(ResourceAttributeDbo.attribute_key == attribute_key)
            .distinct()
            .order_by(ResourceAttributeDbo.attribute_value)
            .all()
        )
        return [row[0] for row in rows if row[0] is not None]

    # TODO base class
    @staticmethod
    def get_by_id(session, resource_id: int) -> ResourceDbo:
        resource: ResourceDbo = (
            session.query(ResourceDbo).filter(ResourceDbo.id == resource_id).first()
        )
        return resource

    @staticmethod
    def truncate_resource_staging_table(session) -> None:
        RepositoryBase.truncate_tables(session=session, models=[ResourceStagingDbo])

    @staticmethod
    def truncate_resource_attribute_staging_table(session) -> None:
        RepositoryBase.truncate_tables(
            session=session, models=[ResourceAttributeStagingDbo]
        )

    @staticmethod
    def get_latest_resource_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model(
            session=session, model=ResourceDbo
        )

    @staticmethod
    def get_latest_resource_attribute_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model(
            session=session, model=ResourceAttributeDbo
        )

    @staticmethod
    def merge_staging(session, ingestion_process_id: int) -> Tuple[int, int]:
        update_stmt: str = RepositoryBase._get_merge_update_statement(
            source_model=ResourceStagingDbo,
            target_model=ResourceDbo,
            merge_keys=ResourceStagingDbo.MERGE_KEYS,
            update_cols=ResourceStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        update_result = session.execute(text(update_stmt))

        insert_stmt: str = RepositoryBase._get_merge_insert_statement(
            source_model=ResourceStagingDbo,
            target_model=ResourceDbo,
            merge_keys=ResourceStagingDbo.MERGE_KEYS,
            update_cols=ResourceStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        insert_result = session.execute(text(insert_stmt))

        return insert_result.rowcount, update_result.rowcount

    @staticmethod
    def merge_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = RepositoryBase._get_merge_deactivate_statement(
            source_model=ResourceStagingDbo,
            target_model=ResourceDbo,
            merge_keys=ResourceStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_attributes_staging(session, ingestion_process_id: int) -> Tuple[int, int]:
        update_stmt: str = RepositoryBase._get_merge_update_statement(
            source_model=ResourceAttributeStagingDbo,
            target_model=ResourceAttributeDbo,
            merge_keys=ResourceAttributeStagingDbo.MERGE_KEYS,
            update_cols=ResourceAttributeStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        update_result = session.execute(text(update_stmt))

        insert_stmt: str = RepositoryBase._get_merge_insert_statement(
            source_model=ResourceAttributeStagingDbo,
            target_model=ResourceAttributeDbo,
            merge_keys=ResourceAttributeStagingDbo.MERGE_KEYS,
            update_cols=ResourceAttributeStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        insert_result = session.execute(text(insert_stmt))

        return insert_result.rowcount, update_result.rowcount

    @staticmethod
    def merge_attributes_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = RepositoryBase._get_merge_deactivate_statement(
            source_model=ResourceAttributeStagingDbo,
            target_model=ResourceAttributeDbo,
            merge_keys=ResourceAttributeStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount
