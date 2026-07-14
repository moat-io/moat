import responses
from pydantic import BaseModel
from responses import matchers

from opa.models.src.opa_bundle_response_model import OpaBundleResponse
from opa.models.src.trino_opa_request_model import OpaTableResource
from ..src.opa_client import OpaClient


@responses.activate
def test_filter_resources_batch():
    """Test batch filtering of resources."""
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        match=[
            matchers.json_params_matcher(
                {
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
                                    "schemaName": "hr",
                                    "tableName": "salaries",
                                }
                            },
                            {
                                "table": {
                                    "catalogName": "datalake",
                                    "schemaName": "sales",
                                    "tableName": "products",
                                }
                            },
                        ],
                    },
                    "context": {"identity": {"user": "alice"}},
                }
            )
        ],
        json={"responses": [{"result": True}, {"result": False}, {"result": True}]},
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    resources = [
        OpaTableResource(catalog="datalake", schema="hr", table="employees"),
        OpaTableResource(catalog="datalake", schema="hr", table="salaries"),
        OpaTableResource(catalog="datalake", schema="sales", table="products"),
    ]

    results = opa_client.filter_resources_batch(username="alice", resources=resources)

    assert results == [True, False, True]


@responses.activate
def test_filter_resources_batch_single():
    """Test batch filtering with single resource."""
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        match=[
            matchers.json_params_matcher(
                {
                    "action": {
                        "operation": "FilterTables",
                        "filterResources": [
                            {
                                "table": {
                                    "catalogName": "datalake",
                                    "schemaName": "hr",
                                    "tableName": "employees",
                                }
                            }
                        ],
                    },
                    "context": {"identity": {"user": "alice"}},
                }
            )
        ],
        json={"responses": [{"result": False}]},
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    resources = [
        OpaTableResource(catalog="datalake", schema="hr", table="employees"),
    ]

    results = opa_client.filter_resources_batch(username="alice", resources=resources)

    assert results == [False]


@responses.activate
def test_filter_resources_batch_error_handling():
    """Test batch filtering error handling."""
    responses.add(
        responses.POST,
        "http://localhost:8181/v1/data/moat/trino/allow",
        json={"error": "Something went wrong"},
        status=500,
    )

    opa_client: OpaClient = OpaClient()

    resources = [
        OpaTableResource(catalog="datalake", schema="hr", table="employees"),
    ]

    # Should return False for all resources on error
    results = opa_client.filter_resources_batch(username="alice", resources=resources)

    assert results == [False]


def _batch_filter_payload(resource_count: int) -> dict:
    """Build a batch-filter request payload with the given number of resources."""
    return {
        "action": {
            "operation": "FilterTables",
            "filterResources": [
                {
                    "table": {
                        "catalogName": "datalake",
                        "schemaName": "hr",
                        "tableName": f"table_{i}",
                    }
                }
                for i in range(resource_count)
            ],
        },
        "context": {"identity": {"user": "alice"}},
    }


@responses.activate
def test_send_opa_batch_filter_all_allowed():
    """OPA returns the indices it allows; expand to one bool per resource."""
    url = "http://localhost:8181/v1/data/moat/trino/batch"
    responses.add(
        responses.POST,
        url,
        json={
            "decision_id": "fe59a709-8523-4e2f-a320-5bbc3a75e2ad",
            "result": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        },
        status=200,
    )

    opa_client: OpaClient = OpaClient()
    payload = _batch_filter_payload(resource_count=14)

    results = opa_client._send_opa_batch_filter(url=url, payload=payload)

    assert results == [True] * 14


@responses.activate
def test_send_opa_batch_filter_none_allowed():
    """An empty result means no resource is allowed."""
    url = "http://localhost:8181/v1/data/moat/trino/batch"
    responses.add(
        responses.POST,
        url,
        json={
            "decision_id": "d93ec89c-dd2b-452c-8dd1-317bbd9c49e1",
            "result": [],
        },
        status=200,
    )

    opa_client: OpaClient = OpaClient()
    payload = _batch_filter_payload(resource_count=14)

    results = opa_client._send_opa_batch_filter(url=url, payload=payload)

    assert results == [False] * 14


@responses.activate
def test_send_opa_batch_filter_partial():
    """Only the resources whose index is returned are allowed, in order."""
    url = "http://localhost:8181/v1/data/moat/trino/batch"
    responses.add(
        responses.POST,
        url,
        json={
            "decision_id": "fe59a709-8523-4e2f-a320-5bbc3a75e2ad",
            "result": [0, 2, 4],
        },
        status=200,
    )

    opa_client: OpaClient = OpaClient()
    payload = _batch_filter_payload(resource_count=5)

    results = opa_client._send_opa_batch_filter(url=url, payload=payload)

    assert results == [True, False, True, False, True]


@responses.activate
def test_get_bundle_metadata():
    """OPA exposes the current bundle state (manifest + etag) at
    data.system.bundles."""
    responses.add(
        responses.GET,
        "http://localhost:8181/v1/data/system/bundles",
        json={
            "result": {
                "authz": {
                    "manifest": {"revision": "abc123", "roots": [""]},
                    "etag": "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2",
                }
            }
        },
        status=200,
    )

    opa_client: OpaClient = OpaClient()

    metadata = opa_client.get_bundle_metadata()

    assert isinstance(metadata, OpaBundleResponse)
    authz = metadata.result["authz"]
    assert authz.etag == "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2"
    assert authz.manifest.revision == "abc123"
    assert authz.manifest.roots == [""]


@responses.activate
def test_get_bundle_metadata_error_handling():
    """A non-200 from OPA yields None rather than raising."""
    responses.add(
        responses.GET,
        "http://localhost:8181/v1/data/system/bundles",
        json={"error": "Something went wrong"},
        status=500,
    )

    opa_client: OpaClient = OpaClient()

    assert opa_client.get_bundle_metadata() is None


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
