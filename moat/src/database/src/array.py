import json
from typing import Optional

from sqlalchemy import JSON, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.engine import Dialect
from sqlalchemy.types import TypeDecorator

__all__ = ("StringArray",)


class StringArray(TypeDecorator[list[str]]):
    """
    String array type which supports both postgres ARRAY and mysql JSON

    On PostgreSQL: Uses native ARRAY(String) type
    On MySQL: Uses JSON type and stores as JSON array
    """

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect):
        """Use native ARRAY for PostgreSQL, JSON for MySQL"""
        if dialect.name == "postgresql":
            return dialect.type_descriptor(ARRAY(String()))
        else:
            return dialect.type_descriptor(JSON())

    @property
    def python_type(self) -> type[list[str]]:
        return list

    def process_bind_param(
        self, value: Optional[list[str]], dialect: Dialect
    ) -> Optional[list[str]]:
        """
        Convert Python list to database format
        PostgreSQL: Native array handling
        MySQL: JSON serialization (handled by JSON type)
        """
        if value is None:
            return None
        return value

    def process_result_value(
        self, value: Optional[list[str]], dialect: Dialect
    ) -> Optional[list[str]]:
        """
        Convert database value to Python list
        PostgreSQL: Native array handling
        MySQL: JSON deserialization (handled by JSON type)
        """
        if value is None:
            return None
        return value
