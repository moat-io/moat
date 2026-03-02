# from app_logger import Logger, get_logger
# from auth import OpaAuthzProvider, OpaPermittaAuthzActionEnum
# from flask import abort, g
# from models import AttributeDto
# from sqlalchemy.orm import Session
#
# from .principals_controller import PrincipalsController
#
# logger: Logger = get_logger("controller.authz")
#
#
# class AuthzController:
#     @staticmethod
#     def authorize(
#         session: Session,
#         user_name: str,
#         action: OpaPermittaAuthzActionEnum,
#         object_type: str = "POLICY",
#         object_state: str = "Draft",
#         object_attributes: list[AttributeDto] | None = None,
#     ) -> None:
#         if not AuthzController().check(
#             session=session,
#             user_name=user_name,
#             action=action,
#             object_type=object_type,
#             object_state=object_state,
#             object_attributes=object_attributes,
#         ):
#             abort(403)
#
#     @staticmethod
#     def check(
#         session: Session,
#         user_name: str,
#         action: OpaPermittaAuthzActionEnum,
#         object_type: str = "POLICY",
#         object_state: str = "Draft",
#         object_attributes: list[AttributeDto] | None = None,
#     ) -> bool:
#         principal_attributes: list[AttributeDto] = (
#             PrincipalsController.get_principal_attributes_by_username(
#                 session=session, user_name=user_name
#             )
#         )
#
#         # send to OPA
#         authz: OpaAuthzProvider = OpaAuthzProvider(
#             user_name=user_name,
#             user_attributes=principal_attributes,
#         )
#         return authz.authorize(
#             action=action,
#             object_type=object_type,
#             object_state=object_state,
#             object_attributes=object_attributes or [],
#         )
