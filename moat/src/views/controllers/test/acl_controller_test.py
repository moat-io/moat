from unittest.mock import patch

from views.controllers import AclController


@patch(
    "views.controllers.src.acl_controller.PolicyEvaluatorResultRepository."
    "get_all_with_search_and_pagination"
)
def test_get_acls_paginated(delegated_query) -> None:
    delegated_query.return_value = (1, ["acl-row"])

    result = AclController.get_acls_paginated(
        session="session",
        sort_col_name="platform",
        page_number=2,
        page_size=10,
        sort_ascending=False,
        search_term="alice",
    )

    assert result == (1, ["acl-row"])
    delegated_query.assert_called_once_with(
        session="session",
        sort_col_name="platform",
        page_number=2,
        page_size=10,
        sort_ascending=False,
        search_term="alice",
    )
