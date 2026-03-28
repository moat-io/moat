import os
import shutil
import hashlib
from database import Database
from datetime import datetime, timedelta, UTC
from sqlalchemy import func

from events import EventLogger
from models import OpaBundleDbo
from opa.bundle_generator import BundleGenerator
from repositories import (
    PrincipalRepository,
    PrincipalGroupRepository,
    ResourceRepository,
)
from app_logger import Logger, get_logger
from services.bundle.src.bundler_config import BundlerConfig

logger: Logger = get_logger("services.bundle")


class BundleService:
    @staticmethod
    def _to_non_negative_int(value: int) -> int:
        return max(0, value)

    @staticmethod
    def refresh_bundle(
        database: Database, event_logger: EventLogger, platform: str | None = None
    ) -> None:
        with database.Session() as session:
            platforms: list[str] = (
                [platform] if platform else BundleGenerator.get_supported_platforms()
            )

            if not platforms:
                logger.warning("No supported platforms found for bundle refresh")

            for target_platform in platforms:
                log: str = f"Bundle generated for platform: {target_platform}"
                if BundleService.bundle_requires_refresh(
                    session=session, platform=target_platform
                ):
                    try:
                        opa_bundle: OpaBundleDbo = BundleService.generate_bundle(
                            session=session, platform=target_platform
                        )

                        context: dict = {
                            "platform": target_platform,
                            "etag": opa_bundle.e_tag,
                            "policy_hash": opa_bundle.policy_hash,
                            "state": "success",
                        }
                        session.commit()

                        event_logger.log_event(
                            asset="bundle", action="generate", log=log, context=context
                        )

                    except Exception as e:
                        logger.error(
                            f"Error generating bundle for platform: {target_platform} :: {e}"
                        )
                        event_logger.log_event(
                            asset="bundle",
                            action="generate",
                            log=log,
                            context={
                                "platform": target_platform,
                                "state": "failure",
                                "error": str(e),
                            },
                        )
                        session.rollback()
                else:
                    logger.info(f"No bundle refresh required for {target_platform}")

            BundleService.clean_up_bundle_storage(session, event_logger)

    @staticmethod
    def _get_current_datetime() -> datetime:
        return datetime.now()

    @staticmethod
    def bundle_requires_refresh(session, platform: str) -> bool:
        # regenerate the bundle in the following cases:
        # - metadata object does not exist
        # - metadata timestamp is older than newest record from resource/principal tables
        # - policy docs hash does not match metadata object
        # - bundle file does not exist
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
            static_rego_file_path=bundle_generator.static_rego_root_path,
            platform=platform,
        )

        latest_object_date: datetime = BundleService.get_latest_object_date(session)
        logger.info(f"Latest date of record in metadata tables: {latest_object_date}")

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
    def clean_up_bundle_storage(session, event_logger: EventLogger) -> None:
        logger.info("Cleaning up bundle storage")
        config: BundlerConfig = BundlerConfig().load()
        retention_days = BundleService._to_non_negative_int(
            value=config.bundle_retention_days,
        )
        min_bundle_count = BundleService._to_non_negative_int(
            value=config.bundle_minimum_count,
        )
        cutoff_date = datetime.now(UTC) - timedelta(days=retention_days)

        platform_bundle_counts = dict(
            session.query(
                OpaBundleDbo.platform,
                func.count(OpaBundleDbo.opa_bundle_id),
            )
            .group_by(OpaBundleDbo.platform)
            .all()
        )
        max_deletions_by_platform = {
            platform: max(0, bundle_count - min_bundle_count)
            for platform, bundle_count in platform_bundle_counts.items()
        }
        deleted_bundles_by_platform = dict.fromkeys(platform_bundle_counts, 0)

        old_bundles = (
            session.query(OpaBundleDbo)
            .filter(OpaBundleDbo.record_updated_date < cutoff_date)
            .order_by(
                OpaBundleDbo.platform.asc(),
                OpaBundleDbo.record_updated_date.asc(),
                OpaBundleDbo.opa_bundle_id.asc(),
            )
            .all()
        )

        for bundle in old_bundles:
            deleted_count = deleted_bundles_by_platform.get(bundle.platform, 0)
            max_deletions = max_deletions_by_platform.get(bundle.platform, 0)

            if deleted_count >= max_deletions:
                continue

            bundle_path = os.path.join(bundle.bundle_directory, bundle.bundle_filename)
            if os.path.exists(bundle_path):
                os.remove(bundle_path)
            session.delete(bundle)
            deleted_bundles_by_platform[bundle.platform] = deleted_count + 1
            event_logger.log_event(
                asset="bundle",
                action="delete",
                log=f"Deleted bundle {bundle.bundle_filename}",
                context={
                    "platform": bundle.platform,
                    "bundle_filename": bundle.bundle_filename,
                    "bundle_date": bundle.record_updated_date,
                    "cutoff_date": cutoff_date,
                },
            )

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
