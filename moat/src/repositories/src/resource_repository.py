from typing import Tuple

from datetime import datetime
from models import (
    ResourceAttributeStagingDbo,
    ResourceDbo,
    ResourceStagingDbo,
    ResourceAttributeDbo,
    ResourceHistoryDbo,
    ResourceAttributeHistoryDbo,
)
from sqlalchemy.orm import Query
from sqlalchemy.sql import text, func

from .repository_base import RepositoryBase


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
    def get_all_with_search_and_pagination(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
    ) -> Tuple[int, list[ResourceDbo]]:
        return RepositoryBase._get_all_with_search_and_pagination(
            model=ResourceDbo,
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            sort_ascending=sort_ascending,
            search_term=search_term,
            search_column_names=["fq_name"],
        )

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
        return RepositoryBase.get_latest_timestamp_for_model_history(
            session=session, model=ResourceHistoryDbo
        )

    @staticmethod
    def get_latest_resource_attribute_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model_history(
            session=session, model=ResourceAttributeHistoryDbo
        )

    @staticmethod
    def merge_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = RepositoryBase._get_merge_statement(
            source_model=ResourceStagingDbo,
            target_model=ResourceDbo,
            merge_keys=ResourceStagingDbo.MERGE_KEYS,
            update_cols=ResourceStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = RepositoryBase._get_merge_deactivate_statement(
            source_model=ResourceStagingDbo,
            target_model=ResourceDbo,
            merge_keys=ResourceStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_attributes_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = RepositoryBase._get_merge_statement(
            source_model=ResourceAttributeStagingDbo,
            target_model=ResourceAttributeDbo,
            merge_keys=ResourceAttributeStagingDbo.MERGE_KEYS,
            update_cols=ResourceAttributeStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_attributes_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = RepositoryBase._get_merge_deactivate_statement(
            source_model=ResourceAttributeStagingDbo,
            target_model=ResourceAttributeDbo,
            merge_keys=ResourceAttributeStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount
