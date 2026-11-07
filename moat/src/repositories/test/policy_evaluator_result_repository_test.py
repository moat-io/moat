from database import Database
from models import PolicyEvaluatorResultDbo

from ..src.policy_evaluator_result_repository import PolicyEvaluatorResultRepository

GRANTS: list[tuple[str, str, bool]] = [
    ("test::principal::alice", "trino.sales.orders", True),
    ("test::principal::bob", "trino.sales.customers", True),
    ("test::principal::carol", "trino.hr.salaries", False),
]


def _seed(database: Database) -> None:
    with database.Session.begin() as session:
        PolicyEvaluatorResultRepository.truncate_tables(
            session=session, models=[PolicyEvaluatorResultDbo]
        )
        for principal_fq_name, resource_fq_name, active in GRANTS:
            session.add(
                PolicyEvaluatorResultDbo(
                    principal_fq_name=principal_fq_name,
                    resource_fq_name=resource_fq_name,
                    platform="trino",
                    action="select",
                    active=active,
                )
            )


def test_get_grants_paginated_returns_active_grants_sorted(
    database_empty: Database,
) -> None:
    _seed(database_empty)

    with database_empty.Session.begin() as session:
        count, grants = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session, page_number=0, page_size=20
        )

        # the inactive grant is no longer part of the ACL
        assert count == 2
        assert [grant.principal_fq_name for grant in grants] == [
            "test::principal::alice",
            "test::principal::bob",
        ]


def test_get_grants_paginated_includes_withdrawn_grants_when_asked(
    database_empty: Database,
) -> None:
    _seed(database_empty)

    with database_empty.Session.begin() as session:
        count, grants = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session, page_number=0, page_size=20, active=None
        )
        assert count == 3

        count, grants = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session, page_number=0, page_size=20, active=False
        )
        assert count == 1
        assert grants[0].principal_fq_name == "test::principal::carol"


def test_get_grants_paginated_searches_every_searchable_column(
    database_empty: Database,
) -> None:
    _seed(database_empty)

    with database_empty.Session.begin() as session:
        for search_term, expected_count in (
            ("alice", 1),
            ("customers", 1),
            ("trino", 2),
            ("select", 2),
            ("nothing-matches-this", 0),
        ):
            count, _ = PolicyEvaluatorResultRepository.get_grants_paginated(
                session=session,
                page_number=0,
                page_size=20,
                search_term=search_term,
            )
            assert count == expected_count, search_term


def test_get_grants_paginated_paginates_and_sorts_descending(
    database_empty: Database,
) -> None:
    _seed(database_empty)

    with database_empty.Session.begin() as session:
        count, first_page = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session,
            page_number=0,
            page_size=1,
            sort_col_name="principal_fq_name",
            sort_ascending=False,
        )
        assert count == 2
        assert [grant.principal_fq_name for grant in first_page] == [
            "test::principal::bob"
        ]

        _, second_page = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session,
            page_number=1,
            page_size=1,
            sort_col_name="principal_fq_name",
            sort_ascending=False,
        )
        assert [grant.principal_fq_name for grant in second_page] == [
            "test::principal::alice"
        ]


def test_get_grants_paginated_ignores_an_unknown_sort_column(
    database_empty: Database,
) -> None:
    """The sort key arrives off a query string, so it must not be able to raise."""
    _seed(database_empty)

    with database_empty.Session.begin() as session:
        count, grants = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session,
            page_number=0,
            page_size=20,
            sort_col_name="drop table; --",
        )
        assert count == 2
        assert [grant.principal_fq_name for grant in grants] == [
            "test::principal::alice",
            "test::principal::bob",
        ]
