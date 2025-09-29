import click
from ingestor import IngestionController
from models import ObjectTypeEnum
from worker import Worker


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


@cli.command()
def start_worker():
    worker: Worker = Worker()
    worker.start()


if __name__ == "__main__":
    cli()
