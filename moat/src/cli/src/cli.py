import click
from ingestor import IngestionController
from models import ObjectTypeEnum


@click.group()
def cli():
    pass


@cli.command()
@click.option("--connector-name", help="Connector type to ingest with")
@click.option(
    "--object-type",
    type=click.Choice([e.value for e in ObjectTypeEnum]),
    help="Type of object to ingest",
)
@click.option(
    "--platform",
    help="Name of the source platform, in case of multiple sources for the same object type",
)
@click.option(
    "--deactivate-omitted",
    is_flag=True,
    default=False,
    help="If set, the ingestion will deactivate objects that are omitted from the ingestion"
    "This should eb used when the source is a complete snapshot of the data",
)
def ingest(
    connector_name: str, object_type: str, platform: str, deactivate_omitted: bool
):
    ingestion_controller = IngestionController()
    ingestion_controller.ingest(
        connector_name=connector_name,
        platform=platform,
        object_type=ObjectTypeEnum(object_type),
        deactivate_omitted=deactivate_omitted,
    )


# @cli.command()
# def ingest_all():
#     """
#     This is a temporary hack to ingest all data from LDAP and DBAPI sources.
#     """
#     import os
#
#     # create_db()
#
#     ingestion_controller = IngestionController()
#
#     # os.environ["CONFIG_FILE_PATH"] = "moat/config/config.principal_ingestion.yaml"
#     # ingestion_controller.ingest(
#     #     connector_name="ldap",
#     #     platform="ad",
#     #     object_type=ObjectTypeEnum.PRINCIPAL,
#     # )
#     #
#     # os.environ["CONFIG_FILE_PATH"] = "moat/config/config.principal_ingestion.yaml"
#     # ingestion_controller.ingest(
#     #     connector_name="ldap",
#     #     platform="ad",
#     #     object_type=ObjectTypeEnum.PRINCIPAL_ATTRIBUTE,
#     # )
#
#     os.environ["CONFIG_FILE_PATH"] = "moat/config/config.resource_ingestion.yaml"
#     ingestion_controller.ingest(
#         connector_name="dbapi",
#         platform="trino",
#         object_type=ObjectTypeEnum.RESOURCE,
#     )
#
#     os.environ["CONFIG_FILE_PATH"] = (
#         "moat/config/config.resource_attribute_ingestion.yaml"
#     )
#     ingestion_controller.ingest(
#         connector_name="dbapi",
#         platform="trino",
#         object_type=ObjectTypeEnum.RESOURCE_ATTRIBUTE,
#     )


if __name__ == "__main__":
    cli()
