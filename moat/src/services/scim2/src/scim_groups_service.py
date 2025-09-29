from typing import Tuple
import re

from app_logger import Logger, get_logger
from models import PrincipalGroupDbo
from .scim_config import ScimConfig
from .scim_service_base import ScimServiceBase
from .. import ScimUsersService

logger: Logger = get_logger("scim2.service.groups")
scim_config = ScimConfig.load()


class ScimGroupsService(ScimServiceBase):

    @staticmethod
    def get_groups(
        session,
        offset: int,
        count: int,
    ) -> Tuple[int, list[PrincipalGroupDbo]]:
        """
        Returns all groups which were defined by SCIM
        We cant return others because they might be deleted by SCIM and we won't have the payload anyway
        """
        total_count: int = session.query(PrincipalGroupDbo).count()
        groups: list[PrincipalGroupDbo] = (
            session.query(PrincipalGroupDbo)
            .filter(PrincipalGroupDbo.source_type == ScimServiceBase.SOURCE_TYPE)
            .offset(offset)
            .limit(count)
            .all()
        )
        return total_count, groups

    @staticmethod
    def get_group_by_id(session, source_uid: str) -> PrincipalGroupDbo:
        return (
            session.query(PrincipalGroupDbo)
            .filter(PrincipalGroupDbo.source_uid == source_uid)
            .first()
        )

    @staticmethod
    def group_exists(session, source_uid: str) -> bool:
        return (
            ScimGroupsService.get_group_by_id(session=session, source_uid=source_uid)
            is not None
        )

    @staticmethod
    def create_group(session, scim_payload: dict) -> dict:
        principal_group: PrincipalGroupDbo = ScimGroupsService.update_group(
            scim_payload=scim_payload,
            principal_group=PrincipalGroupDbo(),
        )
        session.add(principal_group)

        return scim_payload

    @staticmethod
    def update_group(
        principal_group: PrincipalGroupDbo, scim_payload: dict
    ) -> PrincipalGroupDbo:
        principal_group.fq_name = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.group_fq_name_jsonpath
        )

        principal_group.source_uid = ScimServiceBase._get_jsonpath_attribute(
            scim_payload, scim_config.group_source_uid_jsonpath
        )

        members_raw: list[str] = (
            ScimServiceBase._get_jsonpath_attribute(
                scim_payload, scim_config.group_member_username_jsonpath
            )
            or []
        )
        principal_group.members = []

        for member_raw in members_raw:
            try:
                match = re.search(scim_config.group_member_username_regex, member_raw)
                principal_group.members.append(match.group(1))
            except AttributeError:
                logger.warning(
                    f"Failed to extract username from {member_raw} with regex: {scim_config.group_member_username_regex}"
                )

        principal_group.source_type = ScimUsersService.SOURCE_TYPE
        principal_group.scim_payload = scim_payload
        return principal_group
