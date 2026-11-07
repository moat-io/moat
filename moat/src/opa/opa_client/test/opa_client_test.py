import responses
from pydantic import BaseModel
from responses import matchers

from ..src.opa_client import OpaClient


@responses.activate
def test_put_policy():
    def request_callback(request):
        assert request.body == "allow if {true}"
        return 200, {}, request.body

    responses.add_callback(
        responses.PUT,
        "http://localhost:8181/v1/policies/moat/authz",
        callback=request_callback,
    )

    opa_client: OpaClient = OpaClient()
    opa_client.put_policy(policy_name="moat/authz", policy_content="allow if {true}")

    # with a file
    opa_client.put_policy(
        policy_name="moat/authz",
        policy_file_path="moat/src/opa/opa_client/test/dummy.rego",
    )


@responses.activate
def test_authorise_request():
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/authz/allow",
        match=[matchers.json_params_matcher({"input": {"request_method": "GET"}})],
        json={"result": True},
        status=200,
    )

    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/authz/allow",
        match=[matchers.json_params_matcher({"input": {"request_method": "PUT"}})],
        json={},
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    class DummyOpaRequestModel(BaseModel):
        request_method: str

    result_get: bool = opa_client.authorise_request(
        DummyOpaRequestModel(request_method="GET")
    )
    assert result_get

    result_put: bool = opa_client.authorise_request(
        DummyOpaRequestModel(request_method="PUT")
    )
    assert not result_put


@responses.activate
def test_filter_table():
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        match=[
            matchers.json_params_matcher(
                {
                    "input": {
                        "action": {
                            "operation": "FilterTables",
                            "resource": {
                                "table": {
                                    "catalogName": "datalake",
                                    "schemaName": "hr",
                                    "tableName": "employees",
                                }
                            },
                        },
                        "context": {
                            "identity": {"user": "alice"},
                            "softwareStack": {"Version": "0.1.0"},
                        },
                    }
                }
            )
        ],
        json={"result": True},
        status=200,
    )

    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        match=[
            matchers.json_params_matcher(
                {
                    "input": {
                        "action": {
                            "operation": "FilterTables",
                            "resource": {
                                "table": {
                                    "catalogName": "datalake",
                                    "schemaName": "hr",
                                    "tableName": "employees",
                                }
                            },
                        },
                        "context": {
                            "identity": {"user": "bob"},
                            "softwareStack": {"Version": "0.1.0"},
                        },
                    }
                }
            )
        ],
        json={},
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    # alice is allowed
    assert opa_client.filter_table(
        username="alice", database="datalake", schema="hr", table="employees"
    )

    # bob is not allowed
    assert not opa_client.filter_table(
        username="bob", database="datalake", schema="hr", table="employees"
    )


@responses.activate
def test_filter_tables_batch():
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/batch",
        match=[
            matchers.json_params_matcher(
                {
                    "input": {
                        "action": {
                            "operation": "FilterTables",
                            "filterResources": [
                                {
                                    "table": {
                                        "catalogName": "datalake",
                                        "schemaName": "hr",
                                        "tableName": "employees",
                                    }
                                },
                                {
                                    "table": {
                                        "catalogName": "datalake",
                                        "schemaName": "sales",
                                        "tableName": "orders",
                                    }
                                },
                            ],
                        },
                        "context": {
                            "identity": {"user": "alice"},
                            "softwareStack": {"Version": "0.1.0"},
                        },
                    }
                }
            )
        ],
        json={"result": [1]},
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    # only the second table (index 1) is allowed
    allowed_indices = opa_client.filter_tables_batch(
        username="alice",
        tables=[
            ("datalake", "hr", "employees"),
            ("datalake", "sales", "orders"),
        ],
    )
    assert allowed_indices == [1]


@responses.activate
def test_filter_schema():
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        match=[
            matchers.json_params_matcher(
                {
                    "input": {
                        "action": {
                            "operation": "FilterSchemas",
                            "resource": {
                                "schema": {
                                    "catalogName": "datalake",
                                    "schemaName": "hr",
                                }
                            },
                        },
                        "context": {
                            "identity": {"user": "alice"},
                            "softwareStack": {"Version": "0.1.0"},
                        },
                    }
                }
            )
        ],
        json={"result": True},
        status=200,
    )

    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        match=[
            matchers.json_params_matcher(
                {
                    "input": {
                        "action": {
                            "operation": "FilterSchemas",
                            "resource": {
                                "schema": {
                                    "catalogName": "datalake",
                                    "schemaName": "hr",
                                }
                            },
                        },
                        "context": {
                            "identity": {"user": "bob"},
                            "softwareStack": {"Version": "0.1.0"},
                        },
                    }
                }
            )
        ],
        json={},
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    # alice is allowed
    assert opa_client.filter_schema(username="alice", database="datalake", schema="hr")

    # bob is not allowed
    assert not opa_client.filter_schema(
        username="bob", database="datalake", schema="hr"
    )
