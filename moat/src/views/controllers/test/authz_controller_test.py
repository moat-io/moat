# from unittest import mock
#
# import pytest
# from auth import OpaAuthzProvider
# from database import Database
# from models import AttributeDto
#
# from ..src.authz_controller import AuthzController
#
#
# @mock.patch.object(OpaAuthzProvider, "authorize", return_value=True)
# def test_authorize_success(database: Database) -> None:
#
#     # positive case, no exception
#     with database.Session.begin() as session:
#         AuthzController.authorize(
#             session=session,
#             user_name="alice",
#             action="READ",
#             object_type="POLICY",
#             object_state="draft",
#             object_attributes=[],
#         )
#
#
# @mock.patch.object(OpaAuthzProvider, "authorize", return_value=False)
# def test_authorize_failure(database: Database) -> None:
#     # positive case, no exception
#     with database.Session.begin() as session:
#         with pytest.raises(Exception) as excinfo:
#             AuthzController.authorize(
#                 session=session,
#                 user_name="alice",
#                 action="READ",
#                 object_type="POLICY",
#                 object_state="draft",
#                 object_attributes=[],
#             )
#
#     assert (
#         str(excinfo.value)
#         == "403 Forbidden: You don't have the permission to access the requested resource. "
#         "It is either read-protected or not readable by the server."
#     )
