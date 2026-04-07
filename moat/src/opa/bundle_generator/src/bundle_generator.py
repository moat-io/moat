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
    TABLE_LIKE_RESOURCE_TYPES: set[str] = {
        "table",
        "view",
        "materializedview",
        "materialized_view",
        "materialized view",
    }

    DEFAULT_POLICY_DIRECTORY: str = "_defaults"
    REGO_FILE_PATTERN = re.compile(r"(?!.*_test\.rego$).*\.rego$")
    SUPPORTED_STATIC_DATA_EXTENSIONS: set[str] = {".json", ".yaml", ".yml"}

    def _get_revision(self) -> str:
        return datetime.datetime.now(datetime.UTC).isoformat()

    def __init__(self, session=None, platform: str | None = None):
        self.session = session
        self.bundle_filename = "bundle.tar.gz"

        config: BundleGeneratorConfig = BundleGeneratorConfig().load()
        self.platform = platform or config.default_platform
        self.static_rego_root_path: str = config.static_rego_file_path
        self.source_directories: list[str] = BundleGenerator.resolve_source_directories(
            static_rego_root_path=self.static_rego_root_path,
            platform=self.platform,
        )

        self.bundle_directory: str = f"{config.temp_directory}/{uuid.uuid4()}"
        self.data_directory: str = f"{self.bundle_directory}/{self.platform}"
        self.static_rego_file_path: str = os.path.join(
            self.static_rego_root_path, self.platform
        )

        self.data_file_path: str = os.path.join(
            self.bundle_directory, f"{self.platform}", "data.json"
        )
        self.manifest_file_path: str = os.path.join(self.bundle_directory, ".manifest")

    def get_rego_policy_file_path_list(self) -> list[str]:
        logger.info(
            f"Getting rego policy file path list from {self.source_directories}"
        )
        rego_file_map: dict[str, str] = BundleGenerator._build_static_file_map(
            source_directories=self.source_directories,
            include_rego=True,
            include_static_data=False,
        )
        return sorted(rego_file_map.values())

    def __enter__(self) -> Bundle:
        os.makedirs(os.path.join(self.data_directory), exist_ok=True)

        static_file_map: dict[str, str] = BundleGenerator._build_static_file_map(
            source_directories=self.source_directories,
            include_rego=True,
            include_static_data=True,
        )

        reserved_data_file_path = os.path.join(self.platform, "data.json")
        if reserved_data_file_path in static_file_map:
            logger.warning(
                f"Ignoring static file '{reserved_data_file_path}' because it is reserved for generated runtime data for platform '{self.platform}'"
            )
            static_file_map.pop(reserved_data_file_path)

        # Copy all static policy docs to bundle_directory while preserving nested paths.
        for relative_path, file_path in static_file_map.items():
            target_file_path = os.path.join(self.bundle_directory, relative_path)
            os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
            shutil.copy(file_path, target_file_path)

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
                                static_rego_file_path=self.static_rego_root_path,
                                platform=self.platform,
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
            static_rego_file_path=self.static_rego_root_path,
            platform=self.platform,
        )
        return Bundle(
            directory=self.bundle_directory,
            filename=self.bundle_filename,
            policy_hash=policy_hash,
        )

    def __exit__(self, *args):
        shutil.rmtree(self.bundle_directory, ignore_errors=True)

    @staticmethod
    def get_supported_platforms(static_rego_root_path: str | None = None) -> list[str]:
        config: BundleGeneratorConfig = BundleGeneratorConfig().load()
        static_rego_root_path = static_rego_root_path or config.static_rego_file_path

        if not os.path.isdir(static_rego_root_path):
            logger.warning(
                f"Static rego root path '{static_rego_root_path}' does not exist"
            )
            return []

        platforms: list[str] = sorted(
            [
                entry
                for entry in os.listdir(static_rego_root_path)
                if os.path.isdir(os.path.join(static_rego_root_path, entry))
                and entry != BundleGenerator.DEFAULT_POLICY_DIRECTORY
                and not entry.startswith(".")
            ]
        )
        return platforms

    @staticmethod
    def resolve_source_directories(
        static_rego_root_path: str, platform: str
    ) -> list[str]:
        platform_directory = os.path.join(static_rego_root_path, platform)
        if not os.path.isdir(platform_directory):
            raise ValueError(
                f"No static policy directory found for platform '{platform}' in '{static_rego_root_path}'"
            )

        source_directories: list[str] = []
        default_policy_directory = os.path.join(
            static_rego_root_path, BundleGenerator.DEFAULT_POLICY_DIRECTORY
        )
        if os.path.isdir(default_policy_directory):
            source_directories.append(default_policy_directory)
        source_directories.append(platform_directory)
        return source_directories

    @staticmethod
    def _build_static_file_map(
        source_directories: list[str], include_rego: bool, include_static_data: bool
    ) -> dict[str, str]:
        static_file_map: dict[str, str] = {}

        for source_directory in source_directories:
            for root, _, files in os.walk(source_directory):
                for filename in files:
                    extension = os.path.splitext(filename)[1].lower()
                    include_file = False
                    if include_rego and BundleGenerator.REGO_FILE_PATTERN.match(
                        filename
                    ):
                        include_file = True
                    if (
                        include_static_data
                        and extension in BundleGenerator.SUPPORTED_STATIC_DATA_EXTENSIONS
                    ):
                        include_file = True

                    if not include_file:
                        continue

                    absolute_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(absolute_path, source_directory)
                    # Later directories override earlier ones (platform over defaults)
                    static_file_map[relative_path] = absolute_path

        return static_file_map

    @staticmethod
    def get_policy_docs_hash(static_rego_file_path: str, platform: str | None = None) -> str:
        """Returns the hash of the static policy docs for the given platform."""
        if platform:
            source_directories = BundleGenerator.resolve_source_directories(
                static_rego_root_path=static_rego_file_path,
                platform=platform,
            )
            static_file_map: dict[str, str] = BundleGenerator._build_static_file_map(
                source_directories=source_directories,
                include_rego=True,
                include_static_data=True,
            )
            static_file_map.pop(os.path.join(platform, "data.json"), None)
        else:
            static_file_map = BundleGenerator._build_static_file_map(
                source_directories=[static_rego_file_path],
                include_rego=True,
                include_static_data=True,
            )

        hasher = hashlib.md5()
        for relative_path in sorted(static_file_map.keys()):
            with open(static_file_map[relative_path], "rb") as f:
                hasher.update(f.read())

        hash_str: str = hasher.hexdigest()
        logger.info(
            f"Computing hash of policy docs in {static_rego_file_path} for platform '{platform}' as {hash_str}"
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
            resource_type: str = (resource.object_type or "").lower()
            if resource_type in BundleGenerator.TABLE_LIKE_RESOURCE_TYPES:
                fq_name_parts: list[str] = resource.fq_name.split(".")
                if len(fq_name_parts) < 3:
                    logger.warning(
                        "Skipping resource with invalid table-like fq_name: %s",
                        resource.fq_name,
                    )
                    continue
                database, schema, table = fq_name_parts[-3:]
                data_objects[f"{database}.{schema}.{table}"] = {
                    "attributes": BundleGenerator._flatten_attributes(
                        resource.attributes
                    ),
                }

            if resource_type == "column":
                fq_name_split: dict = re.search(
                    r"(?P<table_name>.+)\.(?P<column_name>[^.]+)$", resource.fq_name
                ).groupdict()
                column_name: str = fq_name_split.get("column_name", "")
                table_name: str = fq_name_split.get("table_name", "")
                table_name_parts: list[str] = table_name.split(".")
                table_key: str = (
                    ".".join(table_name_parts[-3:])
                    if len(table_name_parts) >= 3
                    else table_name
                )
                if not data_objects.get(table_key):
                    data_objects[table_key] = {"attributes": []}
                if not data_objects[table_key].get("columns"):
                    data_objects[table_key]["columns"] = {}

                data_objects[table_key]["columns"][f"{column_name}"] = {
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
