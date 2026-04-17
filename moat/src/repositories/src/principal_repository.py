from typing import Tuple
from datetime import datetime
import re
from models import (
    PrincipalAttributeDbo,
    PrincipalAttributeStagingDbo,
    PrincipalDbo,
    PrincipalStagingDbo,
    PrincipalHistoryDbo,
    PrincipalAttributeHistoryDbo,
)
from sqlalchemy import union_all, or_, and_, desc
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
        # Build subquery to get distinct principal_ids with search filter
        subquery = (
            session.query(PrincipalDbo.principal_id)
            .outerjoin(
                PrincipalAttributeDbo,
                PrincipalDbo.fq_name == PrincipalAttributeDbo.fq_name,
            )
            .distinct(PrincipalDbo.principal_id)
        )

        # Apply search filter across multiple fields
        # Split search_term on spaces and/or commas, then apply with AND relationship
        if search_term:
            # Split on spaces and/or commas and filter out empty strings
            search_terms = [
                term.strip()
                for term in re.split(r"[,\s]+", search_term)
                if term.strip()
            ]
            print(search_terms)

            # Apply each term with OR across fields, then AND all terms together
            term_filters = []
            for term in search_terms:
                term_filters.append(
                    or_(
                        PrincipalDbo.user_name.ilike(f"%{term}%"),
                        PrincipalAttributeDbo.attribute_key.ilike(f"%{term}%"),
                        PrincipalAttributeDbo.attribute_value.ilike(f"%{term}%"),
                    )
                )

            if term_filters:
                subquery = subquery.filter(and_(*term_filters))

        subquery = subquery.subquery()

        # Main query: join principals with the filtered subquery
        query: Query = session.query(PrincipalDbo).join(
            subquery, PrincipalDbo.principal_id == subquery.c.principal_id
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
