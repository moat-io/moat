from database import Database
from models import OpaBundleDbo

from ..src.opa_bundle_repository import OpaBundleRepository


def test_get_all_with_search_and_pagination_no_search(database: Database) -> None:
    """Test getting all bundles with pagination but no search term."""
    repo: OpaBundleRepository = OpaBundleRepository()

    # First, add some test data
    with database.Session.begin() as session:
        session.add(
            OpaBundleDbo(
                platform="trino",
                e_tag="abc123",
                bundle_filename="bundle_trino_v1.tar.gz",
                bundle_directory="/var/bundles/trino",
                policy_hash="hash123",
            )
        )
        session.add(
            OpaBundleDbo(
                platform="snowflake",
                e_tag="def456",
                bundle_filename="bundle_snowflake_v1.tar.gz",
                bundle_directory="/var/bundles/snowflake",
                policy_hash="hash456",
            )
        )
        session.add(
            OpaBundleDbo(
                platform="databricks",
                e_tag="ghi789",
                bundle_filename="bundle_databricks_v1.tar.gz",
                bundle_directory="/var/bundles/databricks",
                policy_hash="hash789",
            )
        )
        session.commit()

    # Test pagination - get first page
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=2,
            sort_ascending=True,
            search_term="",
        )

        assert bundle_count == 3
        assert len(bundles) == 2
        assert all([isinstance(b, OpaBundleDbo) for b in bundles])
        # Check alphabetical order (databricks, snowflake)
        assert bundles[0].platform == "databricks"
        assert bundles[1].platform == "snowflake"

    # Test pagination - get second page
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=1,
            page_size=2,
            sort_ascending=True,
            search_term="",
        )

        assert bundle_count == 3
        assert len(bundles) == 1
        assert bundles[0].platform == "trino"


def test_get_all_with_search_and_pagination_with_search(database: Database) -> None:
    """Test searching bundles by platform, filename, etag, or hash."""
    repo: OpaBundleRepository = OpaBundleRepository()

    # Add test data
    with database.Session.begin() as session:
        session.add(
            OpaBundleDbo(
                platform="trino",
                e_tag="abc123",
                bundle_filename="bundle_trino_v1.tar.gz",
                bundle_directory="/var/bundles/trino",
                policy_hash="hash123",
            )
        )
        session.add(
            OpaBundleDbo(
                platform="snowflake",
                e_tag="def456",
                bundle_filename="bundle_snowflake_v1.tar.gz",
                bundle_directory="/var/bundles/snowflake",
                policy_hash="hash456",
            )
        )
        session.add(
            OpaBundleDbo(
                platform="databricks",
                e_tag="ghi789",
                bundle_filename="bundle_databricks_v1.tar.gz",
                bundle_directory="/var/bundles/databricks",
                policy_hash="hash789",
            )
        )
        session.commit()

    # Search by platform
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="trino",
        )

        assert bundle_count == 1
        assert len(bundles) == 1
        assert bundles[0].platform == "trino"
        assert bundles[0].bundle_filename == "bundle_trino_v1.tar.gz"

    # Search by e_tag
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="def456",
        )

        assert bundle_count == 1
        assert len(bundles) == 1
        assert bundles[0].platform == "snowflake"

    # Search by partial filename
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="databricks",
        )

        assert bundle_count == 1
        assert len(bundles) == 1
        assert bundles[0].platform == "databricks"

    # Search by hash
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="hash456",
        )

        assert bundle_count == 1
        assert len(bundles) == 1
        assert bundles[0].platform == "snowflake"

    # Search with no results
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="nonexistent",
        )

        assert bundle_count == 0
        assert len(bundles) == 0


def test_get_all_with_sorting(database: Database) -> None:
    """Test sorting bundles by different columns."""
    repo: OpaBundleRepository = OpaBundleRepository()

    # Add test data
    with database.Session.begin() as session:
        session.add(
            OpaBundleDbo(
                platform="zebra",
                e_tag="zzz999",
                bundle_filename="bundle_a.tar.gz",
                bundle_directory="/var/bundles/zebra",
                policy_hash="hash_z",
            )
        )
        session.add(
            OpaBundleDbo(
                platform="alpha",
                e_tag="aaa111",
                bundle_filename="bundle_z.tar.gz",
                bundle_directory="/var/bundles/alpha",
                policy_hash="hash_a",
            )
        )
        session.commit()

    # Sort by platform ascending
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="",
        )

        assert len(bundles) == 2
        assert bundles[0].platform == "alpha"
        assert bundles[1].platform == "zebra"

    # Sort by platform descending
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="platform",
            page_number=0,
            page_size=10,
            sort_ascending=False,
            search_term="",
        )

        assert len(bundles) == 2
        assert bundles[0].platform == "zebra"
        assert bundles[1].platform == "alpha"

    # Sort by bundle_filename ascending
    with database.Session.begin() as session:
        bundle_count, bundles = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="bundle_filename",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="",
        )

        assert len(bundles) == 2
        assert bundles[0].bundle_filename == "bundle_a.tar.gz"
        assert bundles[1].bundle_filename == "bundle_z.tar.gz"


def test_get_by_id(database: Database) -> None:
    """Test getting a bundle by its ID."""
    repo: OpaBundleRepository = OpaBundleRepository()

    # Add test data
    with database.Session.begin() as session:
        bundle = OpaBundleDbo(
            platform="test_platform",
            e_tag="test_etag",
            bundle_filename="test_bundle.tar.gz",
            bundle_directory="/var/bundles/test",
            policy_hash="test_hash",
        )
        session.add(bundle)
        session.commit()
        bundle_id = bundle.opa_bundle_id

    # Retrieve by ID
    with database.Session.begin() as session:
        retrieved_bundle = repo.get_by_id(session=session, opa_bundle_id=bundle_id)

        assert retrieved_bundle is not None
        assert isinstance(retrieved_bundle, OpaBundleDbo)
        assert retrieved_bundle.opa_bundle_id == bundle_id
        assert retrieved_bundle.platform == "test_platform"
        assert retrieved_bundle.e_tag == "test_etag"
        assert retrieved_bundle.bundle_filename == "test_bundle.tar.gz"
        assert retrieved_bundle.bundle_directory == "/var/bundles/test"
        assert retrieved_bundle.policy_hash == "test_hash"

    # Test with non-existent ID
    with database.Session.begin() as session:
        non_existent = repo.get_by_id(session=session, opa_bundle_id=999999)
        assert non_existent is None
