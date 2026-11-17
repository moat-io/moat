from database import Database
from models import PolicyEvaluatorResultDbo
from repositories import PolicyEvaluatorResultRepository


def test_get_all_with_search_and_pagination(database_empty: Database) -> None:
    with database_empty.Session.begin() as session:
        rows = [
            PolicyEvaluatorResultDbo(
                principal_fq_name="acl-test-principal-a",
                resource_fq_name="acl-test-resource-z",
                platform="acl-test-platform",
                action="FilterTables",
                active=True,
            ),
            PolicyEvaluatorResultDbo(
                principal_fq_name="acl-test-principal-b",
                resource_fq_name="acl-test-resource-a",
                platform="acl-test-platform",
                action="FilterTables",
                active=False,
            ),
            PolicyEvaluatorResultDbo(
                principal_fq_name="another-principal",
                resource_fq_name="acl-test-resource-b",
                platform="acl-test-platform",
                action="FilterTables",
                active=True,
            ),
        ]
        session.add_all(rows)
        session.flush()
        row_ids: list[int] = [row.id for row in rows]

        result_count, page = (
            PolicyEvaluatorResultRepository.get_all_with_search_and_pagination(
                session=session,
                sort_col_name="resource_fq_name",
                sort_ascending=True,
                page_number=0,
                page_size=1,
                search_term="acl-test",
            )
        )
        assert result_count == 3
        assert len(page) == 1
        assert page[0].resource_fq_name == "acl-test-resource-a"
        assert page[0].active is False

        _, descending_page = (
            PolicyEvaluatorResultRepository.get_all_with_search_and_pagination(
                session=session,
                sort_col_name="resource_fq_name",
                sort_ascending=False,
                page_number=0,
                page_size=2,
                search_term="acl-test",
            )
        )
        assert [row.resource_fq_name for row in descending_page] == [
            "acl-test-resource-z",
            "acl-test-resource-b",
        ]

        result_count, matching_rows = (
            PolicyEvaluatorResultRepository.get_all_with_search_and_pagination(
                session=session,
                sort_col_name="resource_fq_name",
                page_number=0,
                page_size=10,
                search_term="another acl-test-resource-b",
            )
        )
        assert result_count == 1
        assert matching_rows[0].principal_fq_name == "another-principal"

        result_count, no_matching_rows = (
            PolicyEvaluatorResultRepository.get_all_with_search_and_pagination(
                session=session,
                sort_col_name="resource_fq_name",
                page_number=0,
                page_size=10,
                search_term="another acl-test-resource-a",
            )
        )
        assert result_count == 0
        assert no_matching_rows == []

        session.query(PolicyEvaluatorResultDbo).filter(
            PolicyEvaluatorResultDbo.id.in_(row_ids)
        ).delete(synchronize_session=False)
