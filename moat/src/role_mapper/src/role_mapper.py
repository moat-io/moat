from app_logger import Logger, get_logger
from database import Database
from repositories import RoleRepository

from .role_mapper_config import RoleMapperConfig

logger: Logger = get_logger("role_mapper")


class RoleMapper:
    def __init__(self) -> None:
        self.config: RoleMapperConfig = RoleMapperConfig().load()
        self.config.validate()

    @staticmethod
    def _get_database() -> Database:
        database: Database = Database()
        database.connect()
        return database

    def map_roles(self) -> None:
        logger.info(
            "Mapping active ACLs to roles with limits "
            f"users={self.config.max_users_per_role}, "
            f"resources={self.config.max_resources_per_role}"
        )
        database: Database = self._get_database()
        with database.Session.begin() as session:
            counts = RoleRepository.reconcile(
                session=session,
                max_users_per_role=self.config.max_users_per_role,
                max_resources_per_role=self.config.max_resources_per_role,
            )
        logger.info(
            "Role mapping complete: "
            f"inserted_or_updated={counts.inserted_or_updated}, "
            f"deleted={counts.deleted}"
        )
