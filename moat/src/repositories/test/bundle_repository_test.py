from database import Database
from models import OpaBundleDbo
from ..src.bundle_repository import BundleRepository


def test_get_bundle_by_id(database: Database) -> None:
    with database.Session.begin() as session:
        # Create a test bundle
        bundle = OpaBundleDbo(
            platform="test-platform",
            e_tag="test-etag-123",
            bundle_filename="test-bundle.tar.gz",
            bundle_directory="/path/to/bundles",
            policy_hash="abc123def456",
        )
        session.add(bundle)
        session.flush()
        bundle_id = bundle.opa_bundle_id
        session.commit()

        # Retrieve the bundle
    with database.Session.begin() as session:
        result = BundleRepository.get_bundle_by_id(session=session, bundle_id=bundle_id)

        assert result is not None
        assert result.opa_bundle_id == bundle_id
        assert result.platform == "test-platform"
        assert result.e_tag == "test-etag-123"
        assert result.bundle_filename == "test-bundle.tar.gz"
        assert result.bundle_directory == "/path/to/bundles"
        assert result.policy_hash == "abc123def456"


def test_get_bundle_by_id_not_found(database: Database) -> None:
    with database.Session.begin() as session:
        result = BundleRepository.get_bundle_by_id(session=session, bundle_id=99999)
        assert result is None
