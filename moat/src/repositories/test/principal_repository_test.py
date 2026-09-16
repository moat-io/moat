from database import Database
from models import (
    AttributeDto,
    PrincipalAttributeStagingDbo,
    PrincipalDbo,
    PrincipalStagingDbo,
    PrincipalAttributeDbo,
    PrincipalAttributeHistoryDbo,
)

from ..src.principal_repository import PrincipalRepository


def test_truncate_staging(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        session.add(PrincipalStagingDbo())
        session.add(PrincipalAttributeStagingDbo())
        session.commit()

    with database.Session.begin() as session:
        assert session.query(PrincipalStagingDbo).count() == 1
        assert session.query(PrincipalAttributeStagingDbo).count() == 1

        repo.truncate_principal_staging_table(session=session)
        repo.truncate_principal_attribute_staging_table(session=session)
        session.commit()

    with database.Session.begin() as session:
        assert session.query(PrincipalStagingDbo).count() == 0
        assert session.query(PrincipalAttributeStagingDbo).count() == 0


def test_get_all(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        principal_count, principals = repo.get_all(
            session=session,
        )

        assert principal_count == 5
        assert all([isinstance(p, PrincipalDbo) for p in principals])
        assert len(principals) == 5

        # entitlements
        assert all([len(p.entitlements) >= 1 for p in principals])

        # attributes
        assert all([len(p.attributes) >= 1 for p in principals])


def test_get_by_username(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        principal: PrincipalDbo = repo.get_by_username(
            session=session, user_name="alice"
        )

        assert principal.user_name == "alice"
        assert principal.first_name == "Alice"
        assert principal.last_name == "Cooper"

        assert principal.entitlements == [
            "sales_data_analysis",
            "marketing_strategy",
            "it_team_supervision",
            "code_repository_access",
        ]

        assert {
            attr.attribute_key: attr.attribute_value for attr in principal.attributes
        } == {
            "Commercial": "Marketing,IT,Sales",
            "Employee": "True",
            "Privacy": "Marketing",
            "Redact": "PII",
            "Restricted": "Marketing,IT",
        }


def test_get_principal_attribute_history(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Get Alice's principal
        principal: PrincipalDbo = repo.get_by_username(
            session=session, user_name="alice"
        )

        # Get her attribute history
        history_records = repo.get_principal_attribute_history(
            session=session, principal_id=principal.principal_id
        )

        # Verify we got history records
        assert len(history_records) > 0
        assert all(
            [isinstance(h, PrincipalAttributeHistoryDbo) for h in history_records]
        )

        # Verify records are sorted by timestamp (oldest first)
        timestamps = [h.history_record_created_date for h in history_records]
        assert timestamps == sorted(timestamps)

        # Verify all records are for alice's attributes
        for record in history_records:
            assert record.fq_name.startswith(principal.fq_name)

        # Test with non-existent principal_id
        empty_history = repo.get_principal_attribute_history(
            session=session, principal_id=999999
        )
        assert empty_history == []


def test_get_all_with_search_and_pagination_by_username(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search by username
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="alice",
        )

        assert count == 1
        assert len(principals) == 1
        assert principals[0].user_name == "alice"


def test_get_all_with_search_and_pagination_by_attribute_key(
    database: Database,
) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search by attribute key
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="Employee",
        )

        # Should find all principals with "Employee" attribute key
        assert count > 0
        assert all([isinstance(p, PrincipalDbo) for p in principals])

        # Verify at least one principal has the attribute
        found_attribute = False
        for principal in principals:
            if any(attr.attribute_key == "Employee" for attr in principal.attributes):
                found_attribute = True
                break
        assert found_attribute


def test_get_all_with_search_and_pagination_by_attribute_value(
    database: Database,
) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search by attribute value - "Marketing" appears in Commercial attribute
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="Marketing",
        )

        # Should find principals with "Marketing" in attribute values
        assert count > 0
        assert all([isinstance(p, PrincipalDbo) for p in principals])

        # Verify at least one principal has Marketing in their attributes
        found_marketing = False
        for principal in principals:
            if any(
                "Marketing" in attr.attribute_value for attr in principal.attributes
            ):
                found_marketing = True
                break
        assert found_marketing


def test_get_all_with_search_and_pagination_no_results(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search for non-existent term
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="nonexistentterm12345",
        )

        assert count == 0
        assert len(principals) == 0


def test_get_all_with_search_and_pagination_empty_search(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Empty search should return all principals
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="",
        )

        assert count == 5  # Based on test_get_all, there are 5 principals
        assert len(principals) == 5
        assert all([isinstance(p, PrincipalDbo) for p in principals])


def test_get_all_with_search_and_pagination_with_pagination(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Test pagination with page size 2
        count, principals_page1 = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=2,
            sort_ascending=True,
            search_term="",
        )

        assert count == 5
        assert len(principals_page1) == 2

        # Get second page
        count, principals_page2 = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=1,
            page_size=2,
            sort_ascending=True,
            search_term="",
        )

        assert count == 5
        assert len(principals_page2) == 2

        # Ensure no overlap between pages
        page1_ids = {p.principal_id for p in principals_page1}
        page2_ids = {p.principal_id for p in principals_page2}
        assert len(page1_ids.intersection(page2_ids)) == 0


def test_get_all_with_search_and_pagination_sorting(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Test ascending sort
        count, principals_asc = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="",
        )

        usernames_asc = [p.user_name for p in principals_asc]
        assert usernames_asc == sorted(usernames_asc)

        # Test descending sort
        count, principals_desc = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=False,
            search_term="",
        )

        usernames_desc = [p.user_name for p in principals_desc]
        assert usernames_desc == sorted(usernames_desc, reverse=True)


def test_get_all_with_search_and_pagination_multiple_terms_space_separated(
    database: Database,
) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search with space-separated terms - both must match (AND relationship)
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="alice Employee",
        )

        # Should find principals that match both "alice" AND "Employee"
        assert count == 1
        assert all([isinstance(p, PrincipalDbo) for p in principals])

        # Verify alice is in the results and has Employee attribute
        alice = principals[0]
        assert any(attr.attribute_key == "Employee" for attr in alice.attributes)


def test_get_all_with_search_and_pagination_multiple_terms_comma_separated(
    database: Database,
) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search with comma-separated terms - both must match (AND relationship)
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="alice,Marketing",
        )

        # Should find principals that match both "alice" AND "Marketing"
        assert count == 1
        assert all([isinstance(p, PrincipalDbo) for p in principals])

        # Verify alice is in the results and has Marketing in attributes
        alice = principals[0]
        assert any("Marketing" in attr.attribute_value for attr in alice.attributes)


def test_get_all_with_search_and_pagination_multiple_terms_no_match(
    database: Database,
) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search with terms where one exists but combined they don't match anything
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="alice nonexistentterm12345",
        )

        # Should find no results since BOTH terms must match
        assert count == 0
        assert len(principals) == 0


def test_get_all_with_search_and_pagination_multiple_terms_with_extra_spaces(
    database: Database,
) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        # Search with extra spaces and commas - should handle gracefully
        count, principals = repo.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=0,
            page_size=10,
            sort_ascending=True,
            search_term="  alice  ,  ,  Employee  ",
        )

        # Should find principals that match both "alice" AND "Employee"
        assert count >= 1
        alice = next((p for p in principals if p.user_name == "alice"), None)
        assert alice is not None


def _filter_principals(
    repo: PrincipalRepository, session, attributes: list[AttributeDto]
) -> set[str]:
    _, principals = repo.get_all_with_search_and_pagination(
        session=session,
        sort_col_name="user_name",
        page_number=0,
        page_size=10,
        sort_ascending=True,
        attributes=attributes,
    )
    return {principal.user_name for principal in principals}


# the seeded principals, for the expectations below:
#
#   alice   Commercial = 'Marketing,IT,Sales'   Privacy = 'Marketing'
#   anne    Commercial = 'IT,Marketing,HR'      Privacy = 'IT'
#   bob     Commercial = 'Sales,HR,IT'          Privacy = 'Sales'
#   frank   Commercial = 'HR,Sales,Marketing'   Privacy = 'HR'


def test_get_attribute_values_splits_multi_valued_attributes(
    database: Database,
) -> None:
    """
    'Commercial' is seeded as a joined list (e.g. 'Sales,HR,IT'). The filter
    dropdown must offer each component on its own, never the joined string.
    """
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        values: list[str] = repo.get_attribute_values(
            session=session, attribute_key="Commercial"
        )

        assert values == sorted(values)
        assert all("," not in value for value in values)
        assert {"Sales", "HR", "IT"}.issubset(set(values))


def test_attribute_filter_matches_a_single_component(database: Database) -> None:
    """
    'Sales' matches a value that is exactly 'Sales', and a joined value holding
    'Sales' as a component wherever it sits in the list:

        bob     Commercial = 'Sales,HR,IT'          - first
        frank   Commercial = 'HR,Sales,Marketing'   - middle
        alice   Commercial = 'Marketing,IT,Sales'   - last
        bob     Privacy    = 'Sales'                - the whole value
    """
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        assert _filter_principals(
            repo=repo,
            session=session,
            attributes=[
                AttributeDto(attribute_key="Commercial", attribute_value="Sales")
            ],
        ) == {"alice", "bob", "frank"}

        assert _filter_principals(
            repo=repo,
            session=session,
            attributes=[AttributeDto(attribute_key="Privacy", attribute_value="Sales")],
        ) == {"bob"}


def test_attribute_filter_is_not_a_substring_match(database: Database) -> None:
    """'Sale' is not a value of 'Sales,HR,IT'."""
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        assert (
            _filter_principals(
                repo=repo,
                session=session,
                attributes=[
                    AttributeDto(attribute_key="Commercial", attribute_value="Sale")
                ],
            )
            == set()
        )


def test_attribute_filter_escapes_like_wildcards(database: Database) -> None:
    """A value is matched literally - '%' and '_' are not wildcards."""
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        for attribute_value in ["Sale_", "Sale%", "%"]:
            assert (
                _filter_principals(
                    repo=repo,
                    session=session,
                    attributes=[
                        AttributeDto(
                            attribute_key="Commercial", attribute_value=attribute_value
                        )
                    ],
                )
                == set()
            ), attribute_value


def test_attribute_filter_requires_every_condition(database: Database) -> None:
    """
    Every filter row has to match, rows on the same attribute included, so each
    one added narrows the result rather than widening it.
    """
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        assert _filter_principals(
            repo=repo,
            session=session,
            attributes=[AttributeDto(attribute_key="Commercial", attribute_value="HR")],
        ) == {"anne", "bob", "frank"}

        # a second value of the same attribute narrows it: alice is already out,
        # and bob's Commercial has no Marketing
        assert _filter_principals(
            repo=repo,
            session=session,
            attributes=[
                AttributeDto(attribute_key="Commercial", attribute_value="HR"),
                AttributeDto(attribute_key="Commercial", attribute_value="Marketing"),
            ],
        ) == {"anne", "frank"}

        # and a row on a different attribute narrows it again
        assert _filter_principals(
            repo=repo,
            session=session,
            attributes=[
                AttributeDto(attribute_key="Commercial", attribute_value="HR"),
                AttributeDto(attribute_key="Commercial", attribute_value="Marketing"),
                AttributeDto(attribute_key="Privacy", attribute_value="IT"),
            ],
        ) == {"anne"}


def test_attribute_filter_is_satisfied_across_attribute_rows(
    database: Database,
) -> None:
    """
    The conditions may be met by different attribute rows of the same principal -
    a record does not have to carry them all on one row.
    """
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        assert _filter_principals(
            repo=repo,
            session=session,
            attributes=[
                AttributeDto(attribute_key="Commercial", attribute_value="Sales"),
                AttributeDto(attribute_key="Privacy", attribute_value="Sales"),
                AttributeDto(attribute_key="Employee", attribute_value="True"),
            ],
        ) == {"bob"}


def test_attribute_filter_returns_nothing_when_no_record_meets_every_condition(
    database: Database,
) -> None:
    """Each principal has a single Privacy value, so two of them meet nobody."""
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        assert (
            _filter_principals(
                repo=repo,
                session=session,
                attributes=[
                    AttributeDto(attribute_key="Privacy", attribute_value="HR"),
                    AttributeDto(attribute_key="Privacy", attribute_value="IT"),
                ],
            )
            == set()
        )
