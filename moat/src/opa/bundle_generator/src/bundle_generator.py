import json
import os
import re
import shutil
import subprocess
import uuid
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


class BundleGenerator:
    def __init__(self, session, platform: str, bundle_name: str):
        self.session = session
        self.platform = platform
        self.bundle_name = bundle_name

        config: BundleGeneratorConfig = BundleGeneratorConfig().load()
        self.bundle_directory: str = f"{config.temp_directory}/{uuid.uuid4()}"
        self.data_directory: str = f"{self.bundle_directory}/{bundle_name}"
        self.static_rego_file_path: str = config.static_rego_file_path

        self.data_file_path: str = os.path.join(
            self.bundle_directory, f"{bundle_name}", "data.json"
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
            f.write(json.dumps({"roots": ["trino", "moat/trino"]}))

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
        return Bundle(directory=self.bundle_directory, filename="bundle.tar.gz")

    def __exit__(self, *args):
        shutil.rmtree(self.bundle_directory, ignore_errors=True)

    @staticmethod
    def generate_data_object(session, platform: str) -> dict:
        principals: list[dict] = BundleGenerator._generate_principals_in_data_object(
            session=session
        )
        data_objects: list[dict] = (
            BundleGenerator._generate_data_objects_in_data_object(
                session=session, platform=platform
            )
        )

        return {"data_objects": data_objects, "principals": principals}

    @staticmethod
    def _generate_principals_in_data_object(session) -> list[dict]:
        principal_count, principals = PrincipalRepository.get_all_active(
            session=session
        )
        logger.info(f"Retrieved {principal_count} active principals from the DB")

        principals_dict: list[dict] = []
        for principal in principals:
            principals_dict.append(
                {
                    "name": principal.user_name,
                    "attributes": BundleGenerator._flatten_attributes(
                        principal.attributes
                    ),
                    "entitlements": principal.entitlements,
                    "groups": sorted([g.fq_name for g in principal.groups]),
                }
            )
        return principals_dict

    @staticmethod
    def _generate_data_objects_in_data_object(session, platform: str) -> list[dict]:
        """
        Takes the resources of type "table" from the DB and returns a nested data object optimized for OPA
        """
        data_objects: list[dict] = []
        repo: ResourceRepository = ResourceRepository()

        count, resources = repo.get_all_by_platform(session=session, platform=platform)
        logger.info(f"Retrieved {count} resources for platform {platform}")

        # resources are ordered, so the first record will be a table, then columns for that table
        for resource in resources:
            # Split the fully qualified name to extract database, schema, and table
            if resource.object_type == "table":
                database, schema, table = resource.fq_name.split(".")
                data_objects.append(
                    {
                        "object": {
                            "database": database,
                            "schema": schema,
                            "table": table,
                        },
                        "attributes": BundleGenerator._flatten_attributes(
                            resource.attributes
                        ),
                    }
                )

            if resource.object_type == "column":
                # the last one will be the one we want to append to, due to ordering
                data_object: dict = data_objects[-1]
                column_name: str = re.search(r"([^.]*$)", resource.fq_name).group(1)

                if not data_object.get("columns"):
                    data_object["columns"] = []

                data_object["columns"].append(
                    {
                        "name": column_name,
                        "attributes": BundleGenerator._flatten_attributes(
                            resource.attributes
                        ),
                    }
                )

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
