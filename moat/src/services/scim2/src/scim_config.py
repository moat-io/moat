from app_config import AppConfigModelBase


class ScimConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "scim"
    principal_fq_name_jsonpath: str = "$.userName"
    principal_first_name_jsonpath: str = "$.name.givenName"
    principal_last_name_jsonpath: str = "$.name.familyName"
    principal_user_name_jsonpath: str = "$.userName"
    principal_email_jsonpath: str = "$.emails[?(@.primary)].value"
    principal_source_uid_jsonpath: str = "$.id"
    principal_active_jsonpath: str = "$.active"

    principal_attributes_jsonpath: str = None
    principal_entitlements_jsonpath: str = "$.entitlements[*].value"

    group_fq_name_jsonpath: str = "$.displayName"
    group_source_uid_jsonpath: str = "$.id"
    group_member_username_jsonpath: str = "$.members[?(@.type == User)].value"
    group_member_username_regex: str = "^(.*)@.*"

    user_schema_filepath: str = "moat/config/scim_user_schema.json"
    group_schema_filepath: str = "moat/config/scim_group_schema.json"
