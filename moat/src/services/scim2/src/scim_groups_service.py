from typing import Tuple
import re

from app_logger import Logger, get_logger
from models import PrincipalGroupDbo, PrincipalGroupMemberDbo
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
        principal_group: PrincipalGroupDbo,
        scim_payload: dict,
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

        # Extract new member fq_names from SCIM payload
        payload_member_fq_names = set()
        for member_raw in members_raw:
            try:
                match = re.search(scim_config.group_member_username_regex, member_raw)
                payload_member_fq_names.add(match.group(1))
            except AttributeError:
                logger.warning(
                    f"Failed to extract username from {member_raw} with regex: {scim_config.group_member_username_regex}"
                )

        # Get existing members and their fq_names
        existing_members = {m.member_fq_name: m for m in principal_group.members}
        existing_member_fq_names = set(existing_members.keys())

        # Delete members that are no longer in the new list
        members_to_delete = existing_member_fq_names - payload_member_fq_names
        for member_fq_name in members_to_delete:
            member_to_remove = existing_members[member_fq_name]
            principal_group.members.remove(member_to_remove)

        # Add new members that don't exist yet
        members_to_add = payload_member_fq_names - existing_member_fq_names
        for member_fq_name in members_to_add:
            new_member = PrincipalGroupMemberDbo(
                member_fq_name=member_fq_name,
                principal_group_id=principal_group.principal_group_id,
            )
            principal_group.members.append(new_member)

        principal_group.source_type = ScimUsersService.SOURCE_TYPE
        principal_group.scim_payload = scim_payload
        return principal_group
