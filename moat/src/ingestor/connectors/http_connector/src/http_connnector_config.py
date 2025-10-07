from app_config import AppConfigModelBase


class HttpConnectorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "http_connector"
    auth_method: str = "api-key"
    api_key: str = None

    url: str = None
    username: str = None
    password: str = None
    ssl_verify: bool = True
    certificate_path: str = None

    principal_fq_name_jsonpath: str = "$.userName"
    principal_first_name_jsonpath: str = "$.name.givenName"
    principal_last_name_jsonpath: str = "$.name.familyName"
    principal_user_name_jsonpath: str = "$.userName"
    principal_email_jsonpath: str = "$.attributes.email"
    principal_source_uid_jsonpath: str = "$.id"
    principal_active_jsonpath: str = "$.active"

    def principals_jsonpath_mapping(self):
        return {
            "fq_name": self.principal_fq_name_jsonpath,
            "first_name": self.principal_first_name_jsonpath,
            "last_name": self.principal_last_name_jsonpath,
            "user_name": self.principal_user_name_jsonpath,
            "email": self.principal_email_jsonpath,
        }
