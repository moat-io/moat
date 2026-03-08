from models import PrincipalDbo, ResourceDbo
from opa import OpaClient


class ResourceBase:
    def __init__(self, resource_name: str):
        self.resource_name = resource_name


class TrinoResource(ResourceBase):
    @property
    def catalog(self) -> str:
        return self._catalog

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def table(self) -> str:
        return self._table

    def __init__(self, resource_name: str):
        super().__init__(resource_name=resource_name)
        self._catalog = resource_name.split(".")[0]
        self._schema = resource_name.split(".")[1]
        self._table = resource_name.split(".")[2]


class DocumentResource(ResourceBase):
    @property
    def document_name(self) -> str:
        return self._resource_name


class GrantController:
    def __init__(self):
        pass

    def process(self):
        """
        Ensure the bundle is up to date
        * this will run after the bundle creation, so we can assume that the bundle is up to date
        * we use a local OPA, and enforce the bundle by using the one which we have in the DB

        Process:
        List the principals
        List the resources

        Split the resource by platform type
        Send the trino resources to the trino resource type, and get back an OPA request
        Send the OPA request to OPA and get a response
        Send the OPA response to the trino resource type and get back an answer

        Batch the resources into OPA for each user (store grants?)

        Should have a class which knows how to split a resource and how to send it to the trino
        OPA policy. This assumes that the policy uses the format of a trino resource
        - probably a good idea so we can have one policy type for all database platforms


        Class structure:
        * Grant controller
        * Trino Grant Thing

        Incremental mode:
        * Maintain a high-water mark for each platform
        * Refresh all grants for each affected user, and all users for each resource
        """
        opa_client: OpaClient = OpaClient()

        principals: list[PrincipalDbo] = self._list_principals()
        resources: list[ResourceBase] = self._list_resources(platform="trino")

        access_map: dict[str, list[str]] = {}

        # for principal in principals:
        #     for resource in resources:
        #         if opa_client.

    def _list_principals(self) -> list[PrincipalDbo]:
        pass

    def _list_resources(self, platform: str) -> list[ResourceBase]:
        pass
