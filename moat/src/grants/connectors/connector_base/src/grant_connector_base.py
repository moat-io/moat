from database import Database
from app_config import AppConfigModelBase
from repositories import PrincipalRepository, ResourceRepository


class GrantConnectorBase:

    def sync(self, database: Database, platform: str = None):
        """

        1. List all principals
        2. List objects in target system
        3. Determine principals and objects which have changed - Future
        4. For changed principals, check all objects
        5. For changed objects, check all principals
        """

        with database.Session.begin() as session:
            source_principal_names = self._list_source_principals(session=session)
            self.sync_principals(active_usernames=source_principal_names)

        self.sync_grants(active_usernames=source_principal_names)

    def _list_source_principals(self, session) -> set[str]:
        count, principals = PrincipalRepository.get_all(session=session)
        return set([p.fq_name for p in principals])

    # def _list_source_resources(self, session, platform: str) -> set[str]:
    #     count, tables = ResourceRepository.get_all_by_platform(
    #         session=session, platform=platform
    #     )
    #     return set([t.fq_name for t in tables if t.object_type == "table"])

    def sync_principals(self, active_usernames: set[str]):
        pass

    def sync_grants(self, active_usernames: set[str]) -> None:
        pass
