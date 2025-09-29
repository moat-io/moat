import os
from pathlib import Path
from unittest import mock
from datetime import datetime, timedelta, UTC
from database import Database
from models import OpaBundleDbo
from services.bundle import BundleService
from services.bundle.src.bundler_config import BundlerConfig


def test_bundle_requires_refresh(database: Database, tmp_path: Path):
    with database.Session() as session:
        # with no bundle defined, must be created
        assert BundleService.bundle_requires_refresh(session=session, platform="trino")

        # create bundle record with future timestamp
        opa_bundle_dbo: OpaBundleDbo = OpaBundleDbo()
        opa_bundle_dbo.platform = "trino"
        opa_bundle_dbo.bundle_filename = "bundle.tar.gz"
        opa_bundle_dbo.bundle_directory = str(tmp_path)
        opa_bundle_dbo.policy_hash = "4a37e1dc809799e5b360f09fb95439ee"
        opa_bundle_dbo.record_updated_date = datetime.now(UTC) + timedelta(days=1)
        session.add(opa_bundle_dbo)
        session.flush()

        # no file existing - should need regen still
        assert BundleService.bundle_requires_refresh(session=session, platform="trino")

        # create file
        with open(os.path.join(tmp_path, "bundle.tar.gz"), "w") as f:
            f.write("qwerty")

        # file exists, policy docs should match, and bundle date is newer than object dates
        assert not BundleService.bundle_requires_refresh(
            session=session, platform="trino"
        )

        # break the policy hash
        opa_bundle_dbo.policy_hash = "bad hash"
        session.flush()
        assert BundleService.bundle_requires_refresh(session=session, platform="trino")

        # revert
        opa_bundle_dbo.policy_hash = "4a37e1dc809799e5b360f09fb95439ee"
        session.flush()
        assert not BundleService.bundle_requires_refresh(
            session=session, platform="trino"
        )

        # make the bundle older than the data
        opa_bundle_dbo.record_updated_date = datetime.now(UTC) + timedelta(days=-1)
        session.flush()
        assert BundleService.bundle_requires_refresh(session=session, platform="trino")


def test_generate_bundle(database: Database, tmp_path):
    # first bundle - should be nothing in DB
    with mock.patch.object(BundlerConfig, "bundle_directory", str(tmp_path)):
        with mock.patch.object(
            BundleService, "_get_current_datetime", return_value=datetime(2025, 1, 1)
        ):
            with database.Session() as session:
                opa_bundle_1: OpaBundleDbo = BundleService.generate_bundle(
                    session=session, platform="trino"
                )

                # check the bundle file exists - the content is tested elsewhere
                assert os.path.exists(
                    os.path.join(
                        opa_bundle_1.bundle_directory, opa_bundle_1.bundle_filename
                    )
                )
                first_bundle_filename: str = opa_bundle_1.bundle_filename

                # check the contents of the object
                assert opa_bundle_1.platform == "trino"
                assert opa_bundle_1.e_tag == "03408ae13f7ba441750554b221915682"
                assert opa_bundle_1.policy_hash == "4a37e1dc809799e5b360f09fb95439ee"
                session.commit()

        # second bundle - should invalidate the old one
        with mock.patch.object(
            BundleService, "_get_current_datetime", return_value=datetime(2025, 1, 2)
        ):
            with database.Session() as session:
                opa_bundle_2: OpaBundleDbo = BundleService.generate_bundle(
                    session=session, platform="trino"
                )

                # check the bundle file exists - the content is tested elsewhere
                assert os.path.exists(
                    os.path.join(
                        opa_bundle_2.bundle_directory, opa_bundle_2.bundle_filename
                    )
                )
                session.commit()

        # make the first bundle older than the retention period
        with database.Session() as session:
            opa_bundles = (
                session.query(OpaBundleDbo)
                .order_by(OpaBundleDbo.record_updated_date.desc())
                .all()
            )
            opa_bundles[1].record_updated_date = datetime.now(UTC) - timedelta(days=10)
            session.commit()

        # clean up the bundle storage
        with database.Session() as session:
            BundleService.clean_up_bundle_storage(session=session)
            session.commit()

        with database.Session() as session:
            opa_bundles = (
                session.query(OpaBundleDbo)
                .order_by(OpaBundleDbo.record_updated_date.desc())
                .all()
            )
            assert len(opa_bundles) == 1
            # the old file should have been deleted
            assert len(os.listdir(tmp_path)) == 1
            assert first_bundle_filename not in os.listdir(tmp_path)
