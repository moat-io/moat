import json
from unittest import mock
import os
import subprocess

from database import Database
from sqlalchemy import text

from ..src.bundle_generator import BundleGenerator


@mock.patch.object(
    BundleGenerator, "_get_revision", return_value="2025-10-19T00:11:45.494304+00:00"
)
def test_generate_bundle(
    mock_get_revision: mock.MagicMock, tmp_path, database: Database
):
    with database.Session() as session:
        with BundleGenerator(session=session, platform="trino") as bundle:
            subprocess.run(["tar", "-xf", bundle.path], cwd=tmp_path)
        bundle_files: list[str] = sorted(os.listdir(tmp_path))

        assert bundle_files == [".manifest", "common.rego", "data.json"]

        with open(os.path.join(tmp_path, ".manifest")) as f:
            assert json.load(f) == {
                "rego_version": 1,
                "revision": "2025-10-19T00:11:45.494304+00:00",
                "roots": [""],
                "metadata": {"policy_hash": "4a37e1dc809799e5b360f09fb95439ee"},
            }
        mock_get_revision.assert_called_once()


def test_get_policy_docs_hash():
    assert (
        BundleGenerator.get_policy_docs_hash(
            static_rego_file_path="moat/test/test_data/rego"
        )
        == "4a37e1dc809799e5b360f09fb95439ee"
    )


def test_generate_data_object(database: Database):
    with open(
        "moat/src/opa/bundle_generator/test/bundle_generator_test_data.json"
    ) as f:
        expected: dict = json.load(f)

    with database.Session() as session:
        actual = BundleGenerator.generate_data_object(session=session, platform="trino")
        assert list(actual.keys()) == ["data_objects", "principals"]

        # data objects
        assert sorted(actual.get("data_objects").keys()) == sorted(
            expected.get("data_objects").keys()
        )

        # principals
        assert sorted(actual.get("principals").keys()) == sorted(
            expected.get("principals").keys()
        )


def test_generate_principals_in_data_object(database: Database):
    with open(
        "moat/src/opa/bundle_generator/test/bundle_generator_test_data.json"
    ) as f:
        expected: dict = json.load(f).get("principals")

    with database.Session() as session:
        actual = BundleGenerator._generate_principals_in_data_object(session=session)

    assert sorted(actual.keys()) == sorted(expected.keys())


def test_generate_principals_in_data_object_with_deactivation(database: Database):
    with database.Session() as session:
        session.execute(
            text("UPDATE principals SET active = false WHERE user_name = 'alice'")
        )
        session.commit()

    with open(
        "moat/src/opa/bundle_generator/test/bundle_generator_test_data.json"
    ) as f:
        principals: dict = json.load(f).get("principals", {})
        principals.pop("alice", {})
        expected = principals

    with database.Session() as session:
        actual = BundleGenerator._generate_principals_in_data_object(session=session)

    assert sorted(actual.keys()) == sorted(expected.keys())


def test_generate_data_objects_in_data_object(database: Database):
    with open(
        "moat/src/opa/bundle_generator/test/bundle_generator_test_data.json"
    ) as f:
        expected: dict = json.load(f).get("data_objects")

    with database.Session() as session:
        actual = BundleGenerator._generate_data_objects_in_data_object(
            session=session, platform="trino"
        )

    assert sorted(actual.keys()) == sorted(expected.keys())
