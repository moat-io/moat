import datetime
import json
import os
import re
import shutil
import subprocess
import uuid
import hashlib
from dataclasses import dataclass

from app_logger import Logger, get_logger
from repositories import PrincipalRepository, ResourceRepository
from models import PrincipalAttributeDbo, ResourceAttributeDbo
from .bundle_generator_config import BundleGeneratorConfig

logger: Logger = get_logger("opa.bundle_generator")


@dataclass
class Bundle:
    @property
    def path(self) -> str:
        return os.path.join(self.directory, self.filename)

    directory: str
    filename: str
    policy_hash: str


class BundleGenerator:
    def _get_revision(self) -> str:
        return datetime.datetime.now(datetime.UTC).isoformat()

    def __init__(self, session, platform: str):
        self.session = session
        self.platform = platform
        self.bundle_filename = "bundle.tar.gz"

        config: BundleGeneratorConfig = BundleGeneratorConfig().load()
        self.bundle_directory: str = f"{config.temp_directory}/{uuid.uuid4()}"
        self.data_directory: str = f"{self.bundle_directory}/{platform}"
        self.static_rego_file_path: str = config.static_rego_file_path

        self.data_file_path: str = os.path.join(
            self.bundle_directory, f"{platform}", "data.json"
        )
        self.manifest_file_path: str = os.path.join(self.bundle_directory, ".manifest")

    def __enter__(self) -> Bundle:
        os.makedirs(os.path.join(self.data_directory), exist_ok=True)

        # Copy all .rego files from static_rego_file_path to bundle_directory
        [
            shutil.copy(
                os.path.join(self.static_rego_file_path, file), self.bundle_directory
            )
            for file in os.listdir(self.static_rego_file_path)
            if re.match(rf"(?!.*_test.rego).*\.rego", file)
        ]

        # write the data file
        with open(self.data_file_path, "w") as f:
            f.write(
                json.dumps(
                    BundleGenerator.generate_data_object(
                        session=self.session, platform=self.platform
                    )
                )
            )

        # write the manifest file to scope the bundle
        with open(self.manifest_file_path, "w") as f:
            f.write(
                json.dumps(
                    {
                        "rego_version": 1,  # TODO make configurable or load from disk
                        "revision": self._get_revision(),
                        "roots": [
                            ""
                        ],  # asserts that the bundle has all namespaces. no external data or policy
                        "metadata": {
                            "policy_hash": BundleGenerator.get_policy_docs_hash(
                                self.static_rego_file_path
                            )
                        },
                    }
                )
            )

        # build the bundle
        result = subprocess.run(
            ["opa", "build", "-b", "."],  # TODO optimise
            capture_output=True,
            text=True,
            cwd=self.bundle_directory,
        )
        if result.returncode != 0:
            raise ValueError(
                f"OPA bundler failed with exit code {result.returncode}, Output: {result.stdout}, Error: {result.stderr}"
            )

        logger.info(
            f"Generated bundle with output: {result.stdout}  Error: {result.stderr}"
        )
        policy_hash: str = BundleGenerator.get_policy_docs_hash(
            self.static_rego_file_path
        )
        return Bundle(
            directory=self.bundle_directory,
            filename=self.bundle_filename,
            policy_hash=policy_hash,
        )

    def __exit__(self, *args):
        shutil.rmtree(self.bundle_directory, ignore_errors=True)

    @staticmethod
    def get_policy_docs_hash(static_rego_file_path: str) -> str:
        """Returns the hash of the policy docs for the given platform."""
        rego_files: list[str] = [
            file
            for file in os.listdir(static_rego_file_path)
            if re.match(rf"(?!.*_test.rego).*\.rego", file)
        ]

        hasher = hashlib.md5()
        for file in rego_files:
            with open(os.path.join(static_rego_file_path, file), "rb") as f:
                hasher.update(f.read())

        hash_str: str = hasher.hexdigest()
        logger.info(
            f"Computing hash of policy docs in {static_rego_file_path} as {hash_str}"
        )
        return hash_str

    @staticmethod
    def generate_data_object(session, platform: str) -> dict:
        principals: dict = BundleGenerator._generate_principals_in_data_object(
            session=session
        )
        data_objects: dict = BundleGenerator._generate_data_objects_in_data_object(
            session=session, platform=platform
        )

        return {"data_objects": data_objects, "principals": principals}

    @staticmethod
    def _generate_principals_in_data_object(session) -> dict:
        principal_count, principals_db = PrincipalRepository.get_all_active(
            session=session
        )
        logger.info(f"Retrieved {principal_count} active principals from the DB")

        principals: dict = {}
        for principal in principals_db:
            principals[f"{principal.user_name}"] = {
                "attributes": BundleGenerator._flatten_attributes(principal.attributes),
                "entitlements": principal.entitlements,
                "groups": sorted([g.fq_name for g in principal.groups]),
            }

        return principals

    @staticmethod
    def _generate_data_objects_in_data_object(session, platform: str) -> dict:
        """
        Takes the resources of type "table" from the DB and returns a nested data object optimized for OPA
        """
        data_objects: dict = {}
        repo: ResourceRepository = ResourceRepository()

        count, resources = repo.get_all_by_platform(session=session, platform=platform)
        logger.info(f"Retrieved {count} resources for platform {platform}")

        # resources are ordered, so the first record will be a table, then columns for that table
        for resource in resources:
            # Split the fully qualified name to extract database, schema, and table
            if resource.object_type == "table":
                database, schema, table = resource.fq_name.split(".")
                data_objects[f"{database}.{schema}.{table}"] = {
                    "attributes": BundleGenerator._flatten_attributes(
                        resource.attributes
                    ),
                }

            if resource.object_type == "column":
                fq_name_split: dict = re.search(
                    r"(?P<table_name>.+)\.(?P<column_name>[^.]+)$", resource.fq_name
                ).groupdict()
                column_name: str = fq_name_split.get("column_name", "")
                table_name: str = fq_name_split.get("table_name", "")
                if not data_objects.get(table_name).get("columns"):
                    data_objects.get(table_name)["columns"] = {}

                data_objects.get(table_name)["columns"][f"{column_name}"] = {
                    "attributes": BundleGenerator._flatten_attributes(
                        resource.attributes
                    ),
                }

        return data_objects

    @staticmethod
    def _flatten_attributes(
        source_attributes: list[PrincipalAttributeDbo | ResourceAttributeDbo],
    ) -> list[str]:
        attributes: list[str] = []
        for a in source_attributes:
            # Split attribute values if they contain commas
            values = [v.strip() for v in a.attribute_value.split(",")]
            attributes.extend([f"{a.attribute_key}::{value}" for value in values])
        return attributes
