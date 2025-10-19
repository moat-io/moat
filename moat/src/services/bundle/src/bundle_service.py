import os
import shutil
import hashlib
from database import Database
from datetime import datetime, timedelta, UTC
from models import OpaBundleDbo
from opa.bundle_generator import BundleGenerator
from repositories import (
    PrincipalRepository,
    PrincipalGroupRepository,
    ResourceRepository,
)
from app_logger import Logger, get_logger
from services.bundle.src.bundler_config import BundlerConfig
from events import EventLogger

logger: Logger = get_logger("services.bundle")


class BundleService:
    @staticmethod
    def refresh_bundle(database: Database, platform: str = "trino"):
        event_logger: EventLogger = EventLogger()

        with database.Session() as session:
            if BundleService.bundle_requires_refresh(
                session=session, platform=platform
            ):
                try:
                    opa_bundle: OpaBundleDbo = BundleService.generate_bundle(
                        session=session, platform=platform
                    )

                    log: str = f"Bundle generated for platform: {platform}"
                    context: dict = {
                        "platform": platform,
                        "etag": opa_bundle.e_tag,
                        "policy_hash": opa_bundle.policy_hash,
                    }
                    session.commit()

                except Exception as e:
                    log: str = f"Error generating bundle for platform: {platform}"
                    logger.error(log)

                finally:
                    event_logger.log_event(
                        asset="bundle", action="generate", log=log, context=context
                    )

    @staticmethod
    def _get_current_datetime() -> datetime:
        return datetime.now()

    @staticmethod
    def bundle_requires_refresh(session, platform: str) -> bool:
        # regenerate the bundle in the following cases:
        # - metadata object does not exist
        # - metadata timestamp is older than newest record from resource/principal tables
        # - policy docs hash does not match metadata object
        # - bundle file does not
        logger.info(f"Checking if bundle requires refresh for {platform}")

        opa_bundle: OpaBundleDbo = BundleService.get_current_bundle_metadata(
            session, platform
        )
        if not opa_bundle:  # no bundle exists, must be created
            logger.info("Bundle metadata does not exist, it must be refreshed")
            return True

        if not os.path.isfile(
            os.path.join(opa_bundle.bundle_directory, opa_bundle.bundle_filename)
        ):
            logger.info("Bundle file does not exist, must be refreshed")
            return True

        bundle_generator: BundleGenerator = BundleGenerator(
            session=session, platform=platform
        )

        policy_hash: str = BundleGenerator.get_policy_docs_hash(
            static_rego_file_path=bundle_generator.static_rego_file_path
        )

        latest_object_date: datetime = BundleService.get_latest_object_date(session)

        logger.info(f"Bundle timestamp: {opa_bundle.record_updated_date}")
        logger.info(
            f"Policy docs hash: {policy_hash}, Policy docs in bundle hash: {opa_bundle.policy_hash}"
        )

        recreate_bundle: bool = (
            latest_object_date > opa_bundle.record_updated_date
            or opa_bundle.policy_hash != policy_hash
        )
        logger.info(f"Recreate bundle?: {recreate_bundle}")
        return recreate_bundle

    @staticmethod
    def get_latest_object_date(session) -> datetime:
        datetime_min = datetime.min.replace(tzinfo=UTC)

        principals_updated_at: datetime = (
            PrincipalRepository.get_latest_principal_change_timestamp(session=session)
        )
        principal_attributes_updated_at: datetime = (
            PrincipalRepository.get_latest_principal_attribute_change_timestamp(
                session=session
            )
        )
        principal_groups_updated_at: datetime = (
            PrincipalGroupRepository.get_latest_change_timestamp(session=session)
        )
        resources_updated_at: datetime = (
            ResourceRepository.get_latest_resource_change_timestamp(session=session)
        )
        resource_attributes_updated_at: datetime = (
            ResourceRepository.get_latest_resource_attribute_change_timestamp(
                session=session
            )
        )

        logger.info(f"Latest principal change: {principals_updated_at}")
        logger.info(
            f"Latest principal attribute change: {principal_attributes_updated_at}"
        )
        logger.info(f"Latest principal group change: {principal_groups_updated_at}")
        logger.info(f"Latest resource change: {resources_updated_at}")
        logger.info(
            f"Latest resource attribute change: {resource_attributes_updated_at}"
        )
        return max(
            principals_updated_at or datetime_min,
            principal_attributes_updated_at or datetime_min,
            principal_groups_updated_at or datetime_min,
            resources_updated_at or datetime_min,
            resource_attributes_updated_at or datetime_min,
        )

    @staticmethod
    def generate_bundle(session, platform: str) -> OpaBundleDbo:
        logger.info(f"Generating bundle for {platform}")
        config: BundlerConfig = BundlerConfig().load()

        opa_bundle: OpaBundleDbo = OpaBundleDbo()
        opa_bundle.platform = platform
        opa_bundle.bundle_directory = config.bundle_directory

        # generate the bundle
        with BundleGenerator(session=session, platform=platform) as bundle:
            opa_bundle.e_tag = hashlib.md5(
                (
                    bundle.policy_hash + str(BundleService._get_current_datetime())
                ).encode("utf-8")
            ).hexdigest()

            opa_bundle.policy_hash = bundle.policy_hash
            opa_bundle.bundle_filename = f"{opa_bundle.e_tag}_bundle.tar.gz"
            session.add(opa_bundle)

            # copy the bundle out somewhere as it is created in tmpdir
            os.makedirs(str(opa_bundle.bundle_directory), exist_ok=True)
            target_bundle_path = os.path.join(
                str(opa_bundle.bundle_directory), str(opa_bundle.bundle_filename)
            )
            shutil.copyfile(
                os.path.join(bundle.directory, bundle.filename),
                target_bundle_path,
            )
            logger.info(
                f"Bundle created at: {target_bundle_path} policy hash: {bundle.policy_hash}, ETag: {opa_bundle.e_tag}"
            )
        return opa_bundle

    @staticmethod
    def clean_up_bundle_storage(session) -> None:
        logger.info(f"Cleaning up bundle storage")
        config: BundlerConfig = BundlerConfig().load()
        cutoff_date = datetime.now() - timedelta(days=config.bundle_retention_days)

        old_bundles = (
            session.query(OpaBundleDbo)
            .filter(OpaBundleDbo.record_updated_date < cutoff_date)
            .all()
        )

        for bundle in old_bundles:
            bundle_path = os.path.join(bundle.bundle_directory, bundle.bundle_filename)
            if os.path.exists(bundle_path):
                os.remove(bundle_path)
            session.delete(bundle)
            logger.info(f"Deleted bundle {bundle.bundle_filename}")

    @staticmethod
    def get_current_bundle_metadata(session, platform: str) -> OpaBundleDbo:
        opa_bundle: OpaBundleDbo = (
            session.query(OpaBundleDbo)
            .filter(OpaBundleDbo.platform == platform)
            .order_by(OpaBundleDbo.record_updated_date.desc())
            .first()
        )
        if opa_bundle:
            logger.info(
                f"Current bundle is: policy hash: {opa_bundle.policy_hash}, ETag: {opa_bundle.e_tag}"
            )
        else:
            logger.info("Current bundle does not exist")
        return opa_bundle
