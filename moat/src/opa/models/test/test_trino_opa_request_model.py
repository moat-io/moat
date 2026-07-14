import json
from moat.src.opa.models.src.trino_opa_request_model import (
    SelectTrinoOpaRequest,
    Context,
    Identity,
    Action,
    Resource,
    TableResource,
    TrinoOperation,
)


def test_select_trino_opa_request_renders_expected_json():
    request = SelectTrinoOpaRequest(
        context=Context(
            identity=Identity(user="foo"),
        ),
        action=Action(
            operation=TrinoOperation.SELECT_FROM_COLUMNS,
            resource=Resource(
                table=TableResource(
                    catalog_name="example_catalog",
                    schema_name="example_schema",
                    table_name="example_table",
                    columns=["column1", "column2", "column3"],
                )
            ),
        ),
    )

    expected_json = {
        "context": {
            "identity": {"user": "foo", "groups": []},
            "queryId": "",
            "softwareStack": {"trinoVersion": ""},
        },
        "action": {
            "operation": "SelectFromColumns",
            "resource": {
                "table": {
                    "catalogName": "example_catalog",
                    "schemaName": "example_schema",
                    "tableName": "example_table",
                    "columns": ["column1", "column2", "column3"],
                }
            },
        },
    }

    assert json.loads(request.model_dump_json(by_alias=True)) == expected_json


def test_filter_catalogs_operation():
    request = SelectTrinoOpaRequest(
        context=Context(
            identity=Identity(user="test_user"),
        ),
        action=Action(
            operation=TrinoOperation.FILTER_CATALOGS,
            resource=Resource(
                table=TableResource(
                    catalog_name="catalog1",
                    schema_name="",
                    table_name="",
                )
            ),
        ),
    )

    result = json.loads(request.model_dump_json(by_alias=True))
    assert result["action"]["operation"] == "FilterCatalogs"


def test_filter_schemas_operation():
    request = SelectTrinoOpaRequest(
        context=Context(
            identity=Identity(user="test_user"),
        ),
        action=Action(
            operation=TrinoOperation.FILTER_SCHEMAS,
            resource=Resource(
                table=TableResource(
                    catalog_name="catalog1",
                    schema_name="schema1",
                    table_name="",
                )
            ),
        ),
    )

    result = json.loads(request.model_dump_json(by_alias=True))
    assert result["action"]["operation"] == "FilterSchemas"


def test_filter_tables_operation():
    request = SelectTrinoOpaRequest(
        context=Context(
            identity=Identity(user="test_user"),
        ),
        action=Action(
            operation=TrinoOperation.FILTER_TABLES,
            resource=Resource(
                table=TableResource(
                    catalog_name="catalog1",
                    schema_name="schema1",
                    table_name="table1",
                )
            ),
        ),
    )

    result = json.loads(request.model_dump_json(by_alias=True))
    assert result["action"]["operation"] == "FilterTables"
