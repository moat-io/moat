from textwrap import dedent
from typing import Tuple, Type
from datetime import datetime
from database import BaseModel
from models import AttributeDto, MetadataDboMixin
from sqlalchemy import desc, or_
from sqlalchemy.sql import text, func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import ColumnProperty, Query, class_mapper
from sqlalchemy.sql.elements import NamedColumn

from models.src.history_dbos.history_mixin import HistoryMixin


class RepositoryBase:

    def __init__(self) -> None:
        pass

    @staticmethod
    def truncate_tables(session, models: list[type[BaseModel]]) -> None:
        for model in models:
            session.execute(text(f"truncate {model.__tablename__}"))
            session.execute(
                text(f"alter sequence {model.__tablename__}_id_seq restart with 1")
            )

    @staticmethod
    def get_attribute_dtos(
        model, attribute_list_property_name: str = "attributes"
    ) -> list[AttributeDto]:
        return [
            AttributeDto(
                attribute_key=a.attribute_key, attribute_value=a.attribute_value
            )
            for a in getattr(model, attribute_list_property_name)
        ]

    @staticmethod
    def get_model_by_name(table_name: str) -> Type[BaseModel]:
        matching_models: list[Type[BaseModel]] = [
            cls for cls in BaseModel.__subclasses__() if cls.__tablename__ == table_name
        ]
        if not matching_models:
            raise ValueError(f"No model found for table {table_name}")
        return matching_models[0]

    @staticmethod
    def get_column_by_name(
        table_name: str, column_name: str
    ) -> NamedColumn | ColumnProperty:
        model: Type[BaseModel] = RepositoryBase.get_model_by_name(table_name=table_name)

        matching_columns: list[ColumnProperty] = [
            c
            for c in [
                prop
                for prop in class_mapper(model).iterate_properties
                if isinstance(prop, ColumnProperty)
            ]
            if c.key == column_name
        ]
        if len(matching_columns) > 0:
            return matching_columns[0].columns[0]

        matching_hybrid_properties: list[hybrid_property] = [
            prop
            for prop in sa_inspect(model).all_orm_descriptors
            if type(prop) == hybrid_property and prop.__name__ == column_name
        ]
        if len(matching_hybrid_properties) > 0:
            return matching_hybrid_properties[0]

        raise KeyError(
            f"Column with name '{column_name}' does not exist in model: '{model}'"
        )

    @staticmethod
    def get_latest_timestamp_for_model_history(
        session, model: type[MetadataDboMixin | BaseModel]
    ) -> datetime:
        assert issubclass(model, HistoryMixin)
        latest_change_timestamp: datetime = session.query(
            func.max(model.history_record_created_date)
        ).scalar()
        return latest_change_timestamp

    @staticmethod
    def _get_all_with_search_and_pagination(
        session,
        model: Type[BaseModel],
        page_number: int,
        page_size: int,
        search_column_names: list[str],
        sort_ascending: bool = True,
        sort_col_name: str | None = None,
        search_term: str = "",
    ):
        query: Query = session.query(model)
        query = RepositoryBase._get_search_query(
            query=query,
            search_column_names=[
                f"{model.__tablename__}.{s}" for s in search_column_names
            ],
            search_term=search_term,
        )

        count: int = query.count()

        if sort_col_name:
            query = RepositoryBase._get_sort_query(
                query=query,
                sort_col_name=f"{model.__tablename__}.{sort_col_name}",
                sort_ascending=sort_ascending,
            )

        query: Query = RepositoryBase._get_pagination_query(
            query=query, page_number=page_number, page_size=page_size
        )

        results: list[BaseModel] = query.all()
        return count, results

    @staticmethod
    def _get_search_query(
        query: Query,
        search_column_names: list[str],
        search_term: str = "",
    ) -> Query:
        search_columns: list[NamedColumn] | list[ColumnProperty] = [
            RepositoryBase.get_column_by_name(
                table_name=search_column_name.split(".")[0],
                column_name=search_column_name.split(".")[1],
            )
            for search_column_name in search_column_names
        ]

        query = query.filter(
            or_(
                *[
                    search_column.ilike(f"%{search_term}%")
                    for search_column in search_columns
                ]
            )
        )
        return query

    @staticmethod
    def _get_sort_query(
        query: Query, sort_col_name: str, sort_ascending: bool = True
    ) -> Query:
        sort_column: NamedColumn | ColumnProperty = RepositoryBase.get_column_by_name(
            table_name=sort_col_name.split(".")[0],
            column_name=sort_col_name.split(".")[1],
        )
        if sort_ascending:
            query = query.order_by(sort_column)
        else:
            query = query.order_by(desc(sort_column))
        return query

    @staticmethod
    def _get_pagination_query(query: Query, page_number: int, page_size: int):
        return query.slice(page_number * page_size, (page_number + 1) * page_size)

    @staticmethod
    def _get_merge_statement(
        source_model: type[BaseModel],
        target_model: type[BaseModel],
        merge_keys: list[str],
        update_cols: list[str],
        ingestion_process_id: int,
    ) -> str:

        source_stmt_where_clause: str = " and ".join(
            [f"{c} is not null" for c in merge_keys]
        )
        source_stmt: str = (
            f"select * from {source_model.__tablename__} where {source_stmt_where_clause}"
        )

        matched_and_stmt: str = "and " + " or ".join(
            [f"src.{c} <> tgt.{c}" for c in update_cols]
        )

        update_stmt: str = (
            "update set "
            + ", ".join([f"{c} = src.{c}" for c in update_cols])
            + f", ingestion_process_id = {ingestion_process_id}"
        )

        insert_stmt: str = (
            "insert (" + ", ".join(merge_keys + update_cols) + ", ingestion_process_id)"
        )
        values_stmt: str = (
            "values ("
            + ", ".join([f"src.{c}" for c in merge_keys + update_cols])
            + f", {ingestion_process_id})"
        )

        on_clause: str = " and ".join([f"src.{c} = tgt.{c}" for c in merge_keys])

        merge_statement: str = dedent(
            f"""
                        merge into {target_model.__tablename__} as tgt
                        using (
                            {source_stmt}
                        ) src
                        on {on_clause}
                        when matched 
                            {matched_and_stmt}
                        then
                            {update_stmt}
                        when not matched then
                            {insert_stmt}
                            {values_stmt}
                    """
        )
        return merge_statement

    @staticmethod
    def _get_merge_insert_statement(
        source_model: type[BaseModel],
        target_model: type[BaseModel],
        merge_keys: list[str],
        update_cols: list[str],
        ingestion_process_id: int,
    ) -> str:
        """
        Selects the records in the source table whose 'merge_keys' do not exist in the target table
        Inserts these into the target table
        """
        all_cols_str: str = ", ".join(
            merge_keys + update_cols + ["ingestion_process_id"]
        )
        source_cols_str: str = ", ".join([f"src.{c}" for c in merge_keys + update_cols])
        join_condition_str: str = " and ".join(
            [f"tgt.{c} = src.{c}" for c in merge_keys]
        )

        where_clause: str = " and ".join([f"tgt.{c} is null" for c in merge_keys])

        return dedent(
            f"""
            insert into {target_model.__tablename__} ({all_cols_str})
            select {source_cols_str}, {ingestion_process_id}
            from {source_model.__tablename__} src
                left join {target_model.__tablename__} tgt on {join_condition_str}
                where {where_clause}
            """
        )

    @staticmethod
    def _get_merge_update_statement(
        source_model: type[BaseModel],
        target_model: type[BaseModel],
        merge_keys: list[str],
        update_cols: list[str],
        ingestion_process_id: int,
    ) -> str:

        set_stmt: str = ", ".join([f"{c} = src.{c}" for c in update_cols])
        join_stmt: str = " and ".join([f"tgt.{c} = src.{c}" for c in merge_keys])
        where_stmt: str = " or ".join([f"tgt.{c} <> src.{c}" for c in update_cols])

        return dedent(
            f"""
            update {target_model.__tablename__} tgt
            set {set_stmt}, ingestion_process_id = {str(ingestion_process_id)}
            from {source_model.__tablename__} src
            where {join_stmt} and ({where_stmt})
            """
        )

    @staticmethod
    def _get_merge_deactivate_statement(
        source_model: type[BaseModel],
        target_model: type[BaseModel],
        merge_keys: list[str],
        ingestion_process_id: int,
    ) -> str:
        """
        The merge delete statement actually executes an update
        The process ID is set and the record is marked as inactive

        When the trigger proc is called on the update, the function
        should insert a row in the history table with the new proc id
        and delete the record from the target table
        """
        merge_keys_str: str = ", ".join(merge_keys)
        where_clause: str = " and ".join([f"tgt.{c} = src.{c}" for c in merge_keys])

        return dedent(
            f"""
                update {target_model.__tablename__} tgt
                set ingestion_process_id = {ingestion_process_id}, active = false
                from (
                    select {merge_keys_str} from {target_model.__tablename__}
                    except
                    select {merge_keys_str} from {source_model.__tablename__}
                ) src
                where {where_clause}
                """
        )
