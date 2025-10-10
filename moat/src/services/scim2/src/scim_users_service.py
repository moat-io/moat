from typing import Tuple
import copy
from app_logger import Logger, get_logger
from models import PrincipalDbo, PrincipalAttributeDbo
from .scim_config import ScimConfig
from .scim_service_base import ScimServiceBase

logger: Logger = get_logger("scim2.service.users")
scim_config = ScimConfig.load()


class ScimUsersService(ScimServiceBase):

    @staticmethod
    def get_users(
        session,
        offset: int,
        count: int,
    ) -> Tuple[int, list[PrincipalDbo]]:
        """
        Returns all groups which were defined by SCIM
        We cant return others because they might be deleted by SCIM and we won't have the payload anyway
        """
        total_count: int = session.query(PrincipalDbo).count()
        groups: list[PrincipalDbo] = (
            session.query(PrincipalDbo)
            .filter(PrincipalDbo.source_type == ScimServiceBase.SOURCE_TYPE)
            .offset(offset)
            .limit(count)
            .all()
        )
        return total_count, groups

    @staticmethod
    def get_user_by_id(session, source_uid: str) -> PrincipalDbo:
        return (
            session.query(PrincipalDbo)
            .filter(PrincipalDbo.source_uid == source_uid)
            .first()
        )

    @staticmethod
    def user_exists(session, source_uid: str) -> bool:
        return (
            ScimUsersService.get_user_by_id(session=session, source_uid=source_uid)
            is not None
        )

    @staticmethod
    def create_user(session, scim_payload: dict) -> dict:
        principal = ScimUsersService.update_user(
            session=session,
            scim_payload=scim_payload,
            principal=PrincipalDbo(),
        )

        session.add(principal)
        for attribute in principal.attributes:
            session.add(attribute)

        return scim_payload

    @staticmethod
    def update_user(
        session, principal: PrincipalDbo, scim_payload: dict
    ) -> PrincipalDbo:
        principal.fq_name = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_fq_name_jsonpath
        )

        principal.first_name = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_first_name_jsonpath
        )

        principal.last_name = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_last_name_jsonpath
        )

        principal.user_name = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_user_name_jsonpath
        )

        principal.email = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_email_jsonpath
        )

        principal.source_uid = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_source_uid_jsonpath
        )

        principal.active = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.principal_active_jsonpath
        )

        principal.source_type = ScimUsersService.SOURCE_TYPE
        principal.scim_payload = scim_payload

        # attributes
        if scim_config.principal_attributes_jsonpath:
            ScimUsersService._merge_attributes_on_principal(
                session=session, principal=principal, scim_payload=scim_payload
            )

        # entitlements
        ScimUsersService._merge_entitlements_on_principal(
            principal=principal, scim_payload=scim_payload
        )

        return principal

    @staticmethod
    def _merge_attributes_on_principal(
        session, principal: PrincipalDbo, scim_payload: dict
    ) -> None:
        attributes: dict = (
            ScimServiceBase._get_jsonpath_attribute(
                # TODO: HACK: Fix this
                copy.deepcopy(scim_payload),
                scim_config.principal_attributes_jsonpath,
            )
            or {}
        )

        # attributes should be a dict, with scalar or simple list values
        for attribute_key, attribute_value in attributes.items():
            if isinstance(attribute_value, list):  # flatten
                attributes[attribute_key] = ",".join([a for a in attribute_value])

        # diff against existing attrs on the principal
        scim_attributes = [(k, v) for k, v in attributes.items()]
        principal_attributes = [
            (a.attribute_key, a.attribute_value) for a in principal.attributes
        ]

        # delete the removed attributes
        [
            session.delete(pa)
            for pa in principal.attributes
            if (pa.attribute_key, pa.attribute_value) not in scim_attributes
        ]

        added_attributes = [a for a in scim_attributes if a not in principal_attributes]

        for attribute_key, attribute_value in added_attributes:
            attribute: PrincipalAttributeDbo = PrincipalAttributeDbo()
            attribute.fq_name = principal.fq_name
            attribute.attribute_key = attribute_key
            attribute.attribute_value = attribute_value
            principal.attributes.append(attribute)

    @staticmethod
    def _merge_entitlements_on_principal(
        principal: PrincipalDbo, scim_payload: dict
    ) -> None:
        entitlements: list[str] | str = (
            ScimServiceBase._get_jsonpath_attribute(
                scim_payload,
                scim_config.principal_entitlements_jsonpath,
            )
            or []
        )

        if not isinstance(entitlements, list):
            entitlements = [entitlements]

        # nothing really required here as they are on the same object
        principal.entitlements = list(set(entitlements))
