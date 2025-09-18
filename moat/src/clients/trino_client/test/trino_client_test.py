from unittest import mock

from pytest import fixture

from ..src.trino_client import TrinoClient
from ..src.trino_client_config import TrinoClientConfig


@fixture(scope="session")
def config() -> TrinoClientConfig:
    return TrinoClientConfig.load()


@mock.patch.object(TrinoClient, "_get_cursor")
@mock.patch.object(TrinoClient, "_execute")
@mock.patch.object(
    TrinoClient,
    "_fetchmany",
    side_effect=[
        ["datalake.information_schema.columns"],
        [
            ["datalake.information_schema.tables"],
            ["datalake.information_schema.views"],
            ["datalake.information_schema.schemata"],
        ],
        [],
    ],
)
@mock.patch.object(TrinoClient, "_get_schema", return_value=["fq_name"])
def test_execute_query_single_column(
    mock_get_schema: mock.MagicMock,
    mock_fetchmany: mock.MagicMock,
    mock_get_cursor: mock.MagicMock,
    mock_execute: mock.MagicMock,
    config: TrinoClientConfig,
):
    trino_client: TrinoClient = TrinoClient()

    loop_counter: int = 0
    records: list[dict] = []

    for batch in trino_client.select_async(
        query="select fq_name from table", batch_size=2
    ):
        loop_counter += 1
        records.extend(batch)

    # expect 4 records in 2 batches
    assert loop_counter == 2

    assert records == [
        {"fq_name": "datalake.information_schema.columns"},
        {"fq_name": "datalake.information_schema.tables"},
        {"fq_name": "datalake.information_schema.views"},
        {"fq_name": "datalake.information_schema.schemata"},
    ]

    mock_execute.assert_called_once()
    mock_get_cursor.assert_called_once()
    mock_get_schema.assert_called_once()
    assert mock_fetchmany.call_count == 3


@mock.patch.object(TrinoClient, "_get_cursor")
@mock.patch.object(TrinoClient, "_execute")
@mock.patch.object(
    TrinoClient,
    "_fetchmany",
    side_effect=[
        [
            "datalake.logistics.shippers",
            "it",
            "commercial",
        ],
        [
            [
                "datalake.logistics.regions",
                "finance",
                "restricted",
            ],
            [
                "datalake.logistics.territories",
                "finance",
                "commercial",
            ],
        ],
        [],
    ],
)
@mock.patch.object(
    TrinoClient,
    "_get_schema",
    return_value=["fq_name", "attribute_key", "attribute_value"],
)
def test_execute_query_multi_column(
    mock_get_schema: mock.MagicMock,
    mock_fetchmany: mock.MagicMock,
    mock_get_cursor: mock.MagicMock,
    mock_execute: mock.MagicMock,
    config: TrinoClientConfig,
):
    trino_client: TrinoClient = TrinoClient()

    loop_counter: int = 0
    records: list[dict] = []

    for batch in trino_client.select_async(
        query="select fq_name, attribute_key, attribute_value", batch_size=2
    ):
        loop_counter += 1
        records.extend(batch)

    # expect 3 records in 2 batches
    assert loop_counter == 2

    assert records == [
        {
            "fq_name": "datalake.logistics.shippers",
            "attribute_key": "it",
            "attribute_value": "commercial",
        },
        {
            "fq_name": "datalake.logistics.regions",
            "attribute_key": "finance",
            "attribute_value": "restricted",
        },
        {
            "fq_name": "datalake.logistics.territories",
            "attribute_key": "finance",
            "attribute_value": "commercial",
        },
    ]

    mock_execute.assert_called_once()
    mock_get_cursor.assert_called_once()
    mock_get_schema.assert_called_once()
    assert mock_fetchmany.call_count == 3
