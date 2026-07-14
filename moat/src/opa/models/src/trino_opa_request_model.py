from enum import Enum
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class TrinoOperation(str, Enum):
    SELECT_FROM_COLUMNS = "SelectFromColumns"
    FILTER_CATALOGS = "FilterCatalogs"
    FILTER_SCHEMAS = "FilterSchemas"
    FILTER_TABLES = "FilterTables"


class Identity(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    user: str
    groups: list[str] = Field(default_factory=list)


class SoftwareStack(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    trino_version: str = Field(serialization_alias="trinoVersion", default="")


class Context(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    identity: Identity
    query_id: str = Field(serialization_alias="queryId", default="")
    software_stack: SoftwareStack = Field(
        serialization_alias="softwareStack", default_factory=SoftwareStack
    )


class OpaResource(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class OpaTableResource(OpaResource):
    catalog: str
    schema: str
    table: str


class TableResource(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog_name: str = Field(serialization_alias="catalogName")
    schema_name: str = Field(serialization_alias="schemaName")
    table_name: str = Field(serialization_alias="tableName")
    columns: list[str] = Field(default_factory=list)


class Resource(BaseModel):
    table: TableResource


class Action(BaseModel):
    operation: TrinoOperation
    resource: Resource


class TrinoOpaRequest(BaseModel):
    context: Context


class SelectTrinoOpaRequest(TrinoOpaRequest):
    action: Action
