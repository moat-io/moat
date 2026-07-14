from app_logger import Logger, get_logger
from database import Database
from models import PolicyEvaluatorResultDbo, ResourceDbo, ObjectTypeEnum
from opa.models.src.trino_opa_request_model import OpaTableResource
from opa.opa_client.src.opa_client import OpaClient
from repositories.src.policy_evaluator_result_repository import (
    PolicyEvaluatorResultRepository,
)
from repositories.src.principal_repository import PrincipalRepository
from repositories.src.resource_repository import ResourceRepository

from .policy_evaluator_config import PolicyEvaluatorConfig

logger: Logger = get_logger("policy_evaluator")


class PolicyEvaluator:
    def __init__(self):
        self.config: PolicyEvaluatorConfig = PolicyEvaluatorConfig().load()
        self.opa_client: OpaClient = OpaClient()

    def evaluate(self, session) -> None:
        """
        Evaluates policies for all principals and resources, storing results in the database.

        Process:
        1. Create a new database transaction
        2. Truncate the policy evaluator results table
        3. Query the database for all principals
        4. Query the database for all platforms
        5. For each platform:
           - Query the database for all resources in configurable batch sizes
           - Query the local OPA agent in batch mode for the batch of resources for each principal
           - Insert the results in the database
        6. Commit the database transaction

        TODO:
        - Implement batch size configuration for resource queries
        - Handle OPA query errors gracefully
        - Support single platform usage (e.g. trino might not be required)
        - Consider parallelizing resource evaluation for each principal
        """
        try:
            logger.info("Starting policy evaluation")

            # Truncate the results table
            logger.info("Truncating policy evaluator results table")
            PolicyEvaluatorResultRepository.truncate_table(session)

            # Get all active principals
            logger.info("Fetching all active principals")
            principal_count, principals = PrincipalRepository.get_all_active(session)
            logger.info(f"Found {principal_count} active principals")

            if principal_count == 0:
                logger.warning("No active principals found, skipping evaluation")
                session.commit()
                return

            # Get all platforms (distinct platforms from resources)
            logger.info("Fetching all platforms")
            platform_dtos = ResourceRepository.list_platforms(session)
            platforms = [p.name for p in platform_dtos]
            logger.info(f"Found {len(platforms)} platforms: {platforms}")

            total_results = 0

            # For each platform, process resources in batches
            for platform in platforms:
                logger.info(f"Processing platform: {platform}")

                # Get all resources for this platform
                resource_count, resources = ResourceRepository.get_all_by_platform(
                    session, platform
                )
                logger.info(f"Found {resource_count} resources for platform {platform}")

                # Process resources in batches
                for i in range(0, resource_count, self.config.batch_size):
                    batch = resources[i : i + self.config.batch_size]
                    batch_num = (i // self.config.batch_size) + 1
                    total_batches = (
                        resource_count + self.config.batch_size - 1
                    ) // self.config.batch_size

                    logger.info(
                        f"Processing batch {batch_num}/{total_batches} "
                        f"({len(batch)} resources) for platform {platform}"
                    )

                    # Convert resources to the format expected by OPA
                    resource_dicts = self._convert_resources_to_opa_format(batch)

                    # For each principal, evaluate all resources in this batch
                    for principal in principals:
                        username = principal.user_name

                        logger.debug(
                            f"Evaluating {len(resource_dicts)} resources for principal {username}"
                        )

                        # Call OPA in batch mode
                        results = self.opa_client.filter_resources_batch(
                            username=username, resources=resource_dicts
                        )

                        # Create result objects for allowed resources
                        result_objects = []
                        for resource, is_allowed in zip(batch, results):
                            if is_allowed:
                                result_obj = PolicyEvaluatorResultDbo(
                                    principal=username,
                                    resource_type=resource.object_type,
                                    resource=resource.fq_name,
                                )
                                result_objects.append(result_obj)

                        # Bulk insert results
                        if result_objects:
                            PolicyEvaluatorResultRepository.bulk_insert(
                                session, result_objects
                            )
                            total_results += len(result_objects)
                            logger.debug(
                                f"Inserted {len(result_objects)} allowed resources "
                                f"for principal {username}"
                            )

            session.commit()
            logger.info(
                f"Policy evaluation completed successfully. "
                f"Total allowed resource grants: {total_results}"
            )

        except Exception as e:
            logger.exception(f"Error during policy evaluation: {e}")
            raise

    def _convert_resources_to_opa_format(
        self, resources: list[ResourceDbo]
    ) -> list[OpaTableResource]:
        """
        Convert resource DBOs to the format expected by OPA.
        Currently supports only trino table resources.
        """
        table_resources = []
        for resource in resources:
            parts = resource.fq_name.split(".")

            table_resources.append(
                OpaTableResource(catalog=parts[0], schema=parts[1], table=parts[2])
            )

        return table_resources
