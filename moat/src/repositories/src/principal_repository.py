from typing import Tuple
from datetime import datetime
from models import (
    PrincipalAttributeDbo,
    PrincipalAttributeStagingDbo,
    PrincipalDbo,
    PrincipalStagingDbo,
    PrincipalHistoryDbo,
    PrincipalAttributeHistoryDbo,
)
from sqlalchemy import union_all
from sqlalchemy.orm import Query
from sqlalchemy.sql import text, func
from .repository_base import RepositoryBase


class PrincipalRepository(RepositoryBase):

    @staticmethod
    def truncate_principal_staging_table(session) -> None:
        RepositoryBase.truncate_tables(session=session, models=[PrincipalStagingDbo])

    @staticmethod
    def truncate_principal_attribute_staging_table(session) -> None:
        RepositoryBase.truncate_tables(
            session=session, models=[PrincipalAttributeStagingDbo]
        )

    @staticmethod
    def get_all(session) -> Tuple[int, list[PrincipalDbo]]:
        query: Query = session.query(PrincipalDbo)
        return query.count(), query.all()

    @staticmethod
    def get_all_active(session) -> Tuple[int, list[PrincipalDbo]]:
        query: Query = session.query(PrincipalDbo).filter(PrincipalDbo.active == True)
        return query.count(), query.all()

    @staticmethod
    def get_all_with_search_and_pagination(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        sort_ascending: bool = True,
        search_term: str = "",
    ) -> Tuple[int, list[PrincipalDbo]]:
        return RepositoryBase._get_all_with_search_and_pagination(
            model=PrincipalDbo,
            session=session,
            sort_col_name=sort_col_name,
            page_number=page_number,
            page_size=page_size,
            sort_ascending=sort_ascending,
            search_term=search_term,
            search_column_names=["user_name"],
        )

    @staticmethod
    def get_by_id(session, principal_id: int) -> PrincipalDbo:
        principal: PrincipalDbo = (
            session.query(PrincipalDbo)
            .filter(PrincipalDbo.principal_id == principal_id)
            .first()
        )
        return principal

    @staticmethod
    def get_by_username(session, user_name: str) -> PrincipalDbo:
        principal: PrincipalDbo = (
            session.query(PrincipalDbo)
            .filter(PrincipalDbo.user_name == user_name)
            .first()
        )
        return principal

    @staticmethod
    def get_by_source_uid(session, source_uid: int) -> PrincipalDbo:
        principal: PrincipalDbo = (
            session.query(PrincipalDbo)
            .filter(PrincipalDbo.source_uid == source_uid)
            .first()
        )
        return principal

    @staticmethod
    def get_latest_principal_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model_history(
            session=session, model=PrincipalHistoryDbo
        )

    @staticmethod
    def get_latest_principal_attribute_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model_history(
            session=session, model=PrincipalAttributeHistoryDbo
        )

    @staticmethod
    def merge_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = PrincipalRepository._get_merge_statement(
            source_model=PrincipalStagingDbo,
            target_model=PrincipalDbo,
            merge_keys=PrincipalStagingDbo.MERGE_KEYS,
            update_cols=PrincipalStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = PrincipalRepository._get_merge_deactivate_statement(
            source_model=PrincipalStagingDbo,
            target_model=PrincipalDbo,
            merge_keys=PrincipalStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_attributes_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = PrincipalRepository._get_merge_statement(
            source_model=PrincipalAttributeStagingDbo,
            target_model=PrincipalAttributeDbo,
            merge_keys=PrincipalAttributeStagingDbo.MERGE_KEYS,
            update_cols=PrincipalAttributeStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_attributes_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = PrincipalRepository._get_merge_deactivate_statement(
            source_model=PrincipalAttributeStagingDbo,
            target_model=PrincipalAttributeDbo,
            merge_keys=PrincipalAttributeStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount
