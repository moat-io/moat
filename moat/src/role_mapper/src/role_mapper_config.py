from app_config import AppConfigModelBase


class RoleMapperConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "role_mapper"

    max_users_per_role: int = 500
    max_resources_per_role: int = 500

    def validate(self) -> None:
        if self.max_users_per_role < 1:
            raise ValueError("role_mapper.max_users_per_role must be at least 1")
        if self.max_resources_per_role < 1:
            raise ValueError("role_mapper.max_resources_per_role must be at least 1")
