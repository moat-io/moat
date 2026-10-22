import re
from datetime import datetime
from typing import Tuple

from models import (
    AttributeDto,
    PrincipalAttributeDbo,
    PrincipalAttributeHistoryDbo,
    PrincipalAttributeStagingDbo,
    PrincipalDbo,
    PrincipalHistoryDbo,
    PrincipalStagingDbo,
)
from sqlalchemy import and_, desc, or_, union_all
from sqlalchemy.orm import Query
from sqlalchemy.sql import func, text

from .repository_base import RepositoryBase

PRINCIPAL_SEARCH_COLUMNS = [
    PrincipalDbo.user_name,
    PrincipalDbo.first_name,
    PrincipalDbo.last_name,
    PrincipalDbo.email,
]


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
    def _get_filtered_query(
        session,
        search_term: str = "",
        source_type: list[str] | None = None,
        active: bool | None = None,
        attributes: list[AttributeDto] | None = None,
    ) -> Query:
        """
        The single definition of 'which principals match the current filters'.
        Shared by the paginated table and the CSV download so the two can never
        drift apart.
        """
        query: Query = session.query(PrincipalDbo)

        query = RepositoryBase._get_attribute_search_query(
            query=query,
            model=PrincipalDbo,
            attribute_model=PrincipalAttributeDbo,
            search_columns=PRINCIPAL_SEARCH_COLUMNS,
            search_term=search_term,
        )
        query = RepositoryBase._get_column_filter_query(
            query=query, column=PrincipalDbo.source_type, values=source_type
        )
        if active is not None:
            query = query.filter(PrincipalDbo.active == active)
        query = RepositoryBase._get_attribute_filter_query(
            query=query,
            model=PrincipalDbo,
            attribute_model=PrincipalAttributeDbo,
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
        source_type: list[str] | str | None = None,
        active: bool | None = None,
        attributes: list[AttributeDto] | None = None,
    ) -> Tuple[int, list[PrincipalDbo]]:
        # callers with a single source (e.g. the entitlements API) may pass a string
        if isinstance(source_type, str):
            source_type = [source_type]

        query: Query = PrincipalRepository._get_filtered_query(
            session=session,
            search_term=search_term,
            source_type=source_type,
            active=active,
            attributes=attributes,
        )

        # Get count before pagination
        count: int = query.count()

        # Apply sorting
        sort_column = RepositoryBase.get_column_by_name(
            table_name=PrincipalDbo.__tablename__, column_name=sort_col_name
        )

        if sort_ascending:
            query = query.order_by(sort_column)
        else:
            query = query.order_by(desc(sort_column))

        # Apply pagination
        query = RepositoryBase._get_pagination_query(
            query=query, page_number=page_number, page_size=page_size
        )

        results: list[PrincipalDbo] = query.all()
        return count, results

    @staticmethod
    def get_all_with_search(
        session,
        sort_col_name: str = "user_name",
        sort_ascending: bool = True,
        search_term: str = "",
        source_type: list[str] | str | None = None,
        active: bool | None = None,
        attributes: list[AttributeDto] | None = None,
    ) -> Tuple[int, list[PrincipalDbo]]:
        """Every principal matching the filters, unpaginated. Used by the CSV download."""
        if isinstance(source_type, str):
            source_type = [source_type]

        query: Query = PrincipalRepository._get_filtered_query(
            session=session,
            search_term=search_term,
            source_type=source_type,
            active=active,
            attributes=attributes,
        )

        sort_column = RepositoryBase.get_column_by_name(
            table_name=PrincipalDbo.__tablename__, column_name=sort_col_name
        )
        query = query.order_by(sort_column if sort_ascending else desc(sort_column))

        results: list[PrincipalDbo] = query.all()
        return len(results), results

    @staticmethod
    def get_filter_options(session) -> dict[str, list[str]]:
        """Distinct values used to populate the advanced filter controls."""
        return {
            "source_types": RepositoryBase._get_distinct_values(
                session=session, column=PrincipalDbo.source_type
            ),
            "attribute_keys": RepositoryBase._get_distinct_values(
                session=session, column=PrincipalAttributeDbo.attribute_key
            ),
        }

    @staticmethod
    def get_attribute_values(session, attribute_key: str) -> list[str]:
        """
        Distinct values for one attribute key, for the dependent value dropdown.
        Multi valued attributes are offered one component at a time.
        """
        return RepositoryBase._get_attribute_values(
            session=session,
            attribute_model=PrincipalAttributeDbo,
            attribute_key=attribute_key,
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
        return RepositoryBase.get_latest_timestamp_for_model(
            session=session, model=PrincipalDbo
        )

    @staticmethod
    def get_latest_principal_attribute_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model(
            session=session, model=PrincipalAttributeDbo
        )

    @staticmethod
    def merge_staging(session, ingestion_process_id: int) -> Tuple[int, int]:
        update_stmt: str = PrincipalRepository._get_merge_update_statement(
            source_model=PrincipalStagingDbo,
            target_model=PrincipalDbo,
            merge_keys=PrincipalStagingDbo.MERGE_KEYS,
            update_cols=PrincipalStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        update_result = session.execute(text(update_stmt))

        insert_stmt: str = PrincipalRepository._get_merge_insert_statement(
            source_model=PrincipalStagingDbo,
            target_model=PrincipalDbo,
            merge_keys=PrincipalStagingDbo.MERGE_KEYS,
            update_cols=PrincipalStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        insert_result = session.execute(text(insert_stmt))

        return insert_result.rowcount, update_result.rowcount

    @staticmethod
    def merge_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = PrincipalRepository._get_merge_deactivate_statement(
            source_model=PrincipalStagingDbo,
            target_model=PrincipalDbo,
            merge_keys=PrincipalStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def merge_attributes_staging(session, ingestion_process_id: int) -> Tuple[int, int]:
        update_stmt: str = PrincipalRepository._get_merge_update_statement(
            source_model=PrincipalAttributeStagingDbo,
            target_model=PrincipalAttributeDbo,
            merge_keys=PrincipalAttributeStagingDbo.MERGE_KEYS,
            update_cols=PrincipalAttributeStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        update_result = session.execute(text(update_stmt))

        insert_stmt: str = PrincipalRepository._get_merge_insert_statement(
            source_model=PrincipalAttributeStagingDbo,
            target_model=PrincipalAttributeDbo,
            merge_keys=PrincipalAttributeStagingDbo.MERGE_KEYS,
            update_cols=PrincipalAttributeStagingDbo.UPDATE_COLS,
            ingestion_process_id=ingestion_process_id,
        )
        insert_result = session.execute(text(insert_stmt))

        return insert_result.rowcount, update_result.rowcount

    @staticmethod
    def merge_attributes_deactivate_staging(session, ingestion_process_id: int) -> int:
        merge_stmt: str = PrincipalRepository._get_merge_deactivate_statement(
            source_model=PrincipalAttributeStagingDbo,
            target_model=PrincipalAttributeDbo,
            merge_keys=PrincipalAttributeStagingDbo.MERGE_KEYS,
            ingestion_process_id=ingestion_process_id,
            dialect=session.bind.dialect.name,
        )
        result = session.execute(text(merge_stmt))
        return result.rowcount

    @staticmethod
    def get_principal_attribute_history(
        session, principal_id: int
    ) -> list[PrincipalAttributeHistoryDbo]:
        """
        Get all attribute history records for a principal, sorted by timestamp.

        Returns:
            List of PrincipalAttributeHistoryDbo records sorted by timestamp (oldest first)
        """
        history_records = (
            session.query(PrincipalAttributeHistoryDbo)
            .join(
                PrincipalDbo,
                PrincipalAttributeHistoryDbo.fq_name == PrincipalDbo.fq_name,
            )
            .filter(PrincipalDbo.principal_id == principal_id)
            .order_by(PrincipalAttributeHistoryDbo.history_record_created_date.desc())
            .all()
        )

        return history_records
