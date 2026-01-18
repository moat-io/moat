# import pytest
# from database import Database
# from models import AttributeDto
#
# from ..src.principals_controller import PrincipalsController
#
#
# def test_get_principal_attributes_by_username__alice(database: Database) -> None:
#     with database.Session.begin() as session:
#         assert PrincipalsController.get_principal_attributes_by_username(
#             session=session, user_name="alice"
#         ) == [
#             AttributeDto(attribute_key="ad_group", attribute_value="SALES_ANALYSTS_GL"),
#             AttributeDto(
#                 attribute_key="ad_group", attribute_value="MARKETING_DIRECTORS_GL"
#             ),
#             AttributeDto(attribute_key="ad_group", attribute_value="IT_SUPERVISORS_GL"),
#             AttributeDto(attribute_key="ad_group", attribute_value="DEVELOPERS"),
#             AttributeDto(attribute_key="Sales", attribute_value="Commercial"),
#             AttributeDto(attribute_key="Marketing", attribute_value="Commercial"),
#             AttributeDto(attribute_key="Marketing", attribute_value="Privacy"),
#             AttributeDto(attribute_key="IT", attribute_value="Commercial"),
#             AttributeDto(attribute_key="IT", attribute_value="Restricted"),
#         ]
#
#
# def test_get_principal_attributes_by_username__boris(database: Database) -> None:
#     with database.Session.begin() as session:
#         with pytest.raises(ValueError):
#             PrincipalsController.get_principal_attributes_by_username(
#                 session=session, user_name="boris"
#             )
