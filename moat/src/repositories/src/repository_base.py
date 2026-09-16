import re
from datetime import datetime
from textwrap import dedent
from typing import Tuple, Type

from database import BaseModel
from models import AttributeDto, MetadataDboMixin
from sqlalchemy import and_, desc, exists, or_
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import ColumnProperty, Query, class_mapper
from sqlalchemy.sql import func, text
from sqlalchemy.sql.elements import NamedColumn


class RepositoryBase:

    def __init__(self) -> None:
        pass

    @staticmethod
    def truncate_tables(session, models: list[type[BaseModel]]) -> None:
        for model in models:
            session.execute(text(f"truncate {model.__tablename__}"))
            # Postgres requires an explicit reset on the sequence
            if session.bind.dialect.name == "postgresql":
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
    def get_latest_timestamp_for_model(
        session, model: type[MetadataDboMixin | BaseModel]
    ) -> datetime:
        history_model: Type[BaseModel] = RepositoryBase.get_model_by_name(
            table_name=f"{model.__tablename__}_history"
        )
        latest_change_timestamp: datetime = session.query(
            func.max(history_model.history_record_created_date)
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
    def _split_search_terms(search_term: str) -> list[str]:
        """Search boxes accept several terms separated by spaces and/or commas."""
        if not search_term:
            return []
        return [
            term.strip() for term in re.split(r"[,\s]+", search_term) if term.strip()
        ]

    @staticmethod
    def _get_attribute_search_query(
        query: Query,
        model: Type[BaseModel],
        attribute_model: Type[BaseModel],
        search_columns: list[NamedColumn],
        search_term: str = "",
    ) -> Query:
        """
        Applies a free text search across the model's own columns and its
        attribute key/values.

        Each whitespace/comma separated term must match somewhere (AND between
        terms, OR across the columns and attributes). A correlated EXISTS is used
        rather than a join so that a row is not required to satisfy every term
        from a *single* attribute row, and so that no duplicate rows are produced.
        """
        for term in RepositoryBase._split_search_terms(search_term=search_term):
            query = query.filter(
                or_(
                    *[column.ilike(f"%{term}%") for column in search_columns],
                    exists().where(
                        and_(
                            attribute_model.fq_name == model.fq_name,
                            or_(
                                attribute_model.attribute_key.ilike(f"%{term}%"),
                                attribute_model.attribute_value.ilike(f"%{term}%"),
                            ),
                        )
                    ),
                )
            )
        return query

    @staticmethod
    def split_attribute_values(attribute_value: str | None) -> list[str]:
        """
        A single attribute row may carry several values joined by commas
        (`Commercial = 'Sales,HR,IT'`). Everything the user sees and filters on
        is one of those components, not the joined string, so this is the one
        place that decides where the boundaries are - matching how the OPA
        bundle flattens the same values.
        """
        if not attribute_value:
            return []
        return [value.strip() for value in attribute_value.split(",") if value.strip()]

    @staticmethod
    def _get_component_value_column(attribute_model: Type[BaseModel]) -> NamedColumn:
        """
        The attribute value wrapped in delimiters and stripped of the spaces that
        may surround them, so a single component can be matched with a `like`
        against ',component,' without a leading or trailing component being
        missed.
        """
        return func.replace(
            func.replace(
                func.concat(",", attribute_model.attribute_value, ","), ", ", ","
            ),
            " ,",
            ",",
        )

    @staticmethod
    def _get_component_like_pattern(attribute_value: str) -> str:
        escaped: str = (
            attribute_value.strip()
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        return f"%,{escaped},%"

    @staticmethod
    def _get_attribute_filter_query(
        query: Query,
        model: Type[BaseModel],
        attribute_model: Type[BaseModel],
        attributes: list[AttributeDto] | None = None,
    ) -> Query:
        """
        Restricts to rows carrying *every* one of the supplied attribute
        key/value pairs, including pairs sharing a key: filtering `Commercial`
        on both 'HR' and 'IT' returns the records carrying both, not either.

        A correlated EXISTS per pair, rather than one over a joined attribute
        row, so the pairs may be satisfied by different attribute rows of the
        same record.

        A value matches a single component of the stored attribute, so the filter
        offered for `Commercial = 'Sales,HR,IT'` is 'Sales', not the whole
        joined string.
        """
        if not attributes:
            return query

        component_value = RepositoryBase._get_component_value_column(
            attribute_model=attribute_model
        )

        for attribute in attributes:
            query = query.filter(
                exists().where(
                    and_(
                        attribute_model.fq_name == model.fq_name,
                        attribute_model.attribute_key == attribute.attribute_key,
                        component_value.like(
                            RepositoryBase._get_component_like_pattern(
                                attribute_value=attribute.attribute_value
                            ),
                            escape="\\",
                        ),
                    )
                )
            )
        return query

    @staticmethod
    def _get_attribute_values(
        session, attribute_model: Type[BaseModel], attribute_key: str
    ) -> list[str]:
        """
        Distinct values for one attribute key, for the dependent value dropdown.

        Multi valued attributes are broken into their components so the dropdown
        offers 'Sales', 'HR' and 'IT' rather than the single unusable entry
        'Sales,HR,IT'.
        """
        rows = (
            session.query(attribute_model.attribute_value)
            .filter(attribute_model.attribute_key == attribute_key)
            .distinct()
            .all()
        )

        values: set[str] = set()
        for row in rows:
            values.update(RepositoryBase.split_attribute_values(attribute_value=row[0]))

        return sorted(values)

    @staticmethod
    def _get_column_filter_query(
        query: Query, column: NamedColumn, values: list[str] | None
    ) -> Query:
        """Restricts a column to one of the supplied values. Empty means no filter."""
        if not values:
            return query
        return query.filter(column.in_(values))

    @staticmethod
    def _get_distinct_values(session, column: NamedColumn) -> list[str]:
        """Distinct non-null values for a column, for populating filter dropdowns."""
        rows = session.query(column).distinct().order_by(column).all()
        return [row[0] for row in rows if row[0] is not None]

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

        # where the staging table entry is not null and target table entry is null
        where_clause: str = " and ".join(
            [f"src.{c} is not null and tgt.{c} is null" for c in merge_keys]
        )

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
        dialect: str = "postgresql",
    ) -> str:

        set_stmt: str = ", ".join([f"{c} = src.{c}" for c in update_cols])
        join_stmt: str = " and ".join([f"tgt.{c} = src.{c}" for c in merge_keys])
        where_stmt: str = " or ".join(
            [f"tgt.{c} <> src.{c}" for c in update_cols] + ["tgt.active is not true"]
        )

        if dialect == "mysql":
            set_stmt: str = ", ".join([f"tgt.{c} = src.{c}" for c in update_cols])
            return dedent(
                f"""
                UPDATE {target_model.__tablename__} tgt
                JOIN {source_model.__tablename__} src ON {join_stmt}
                SET {set_stmt}, tgt.active = true, tgt.ingestion_process_id = {str(ingestion_process_id)}
                WHERE {where_stmt}
                """
            )

        # postgres
        return dedent(
            f"""
            update {target_model.__tablename__} tgt
            set {set_stmt}, active = true, ingestion_process_id = {str(ingestion_process_id)}
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
        dialect: str = "postgresql",
    ) -> str:
        """
        Deletes the records from the target table whose 'merge_keys' are no longer
        present in the source (staging) table.

        The record is hard deleted rather than flagged inactive. The history trigger
        on the target table writes a 'D' row before the record disappears, so the
        removal remains auditable - which is why the ingestion process id is not
        stamped on the target row first.

        'not exists' is used rather than 'except' as the 'except' set operator is
        not supported by MySQL, including RDS MySQL.
        """
        match_clause: str = " and ".join(
            [
                f"(src.{c} = tgt.{c} or (src.{c} is null and tgt.{c} is null))"
                for c in merge_keys
            ]
        )
        not_exists_clause: str = (
            f"not exists ("
            f"select 1 from {source_model.__tablename__} src where {match_clause}"
            f")"
        )

        if dialect == "mysql":
            # the multi table delete syntax is used as aliasing the target of a
            # single table delete is only supported from MySQL 8.0.16
            return dedent(
                f"""
                delete tgt from {target_model.__tablename__} tgt
                where {not_exists_clause}
                """
            )

        return dedent(
            f"""
            delete from {target_model.__tablename__} tgt
            where {not_exists_clause}
            """
        )
