import hashlib
from app_logger import Logger, get_logger
from views.models import PolicyDslVm
from opa.bundle_generator import BundleGenerator

logger: Logger = get_logger("controllers.policy")


class PoliciesController:
    @staticmethod
    def get_all_policies() -> list[PolicyDslVm]:
        bundle_generator: BundleGenerator = BundleGenerator()
        file_paths: list[str] = bundle_generator.get_rego_policy_file_path_list()

        policies: list[PolicyDslVm] = [
            PolicyDslVm(file_path=file_path, policy_dsl=open(file_path).read())
            for file_path in file_paths
        ]

        return policies

    @staticmethod
    def get_by_id(policy_id: str) -> PolicyDslVm | None:
        bundle_generator: BundleGenerator = BundleGenerator()
        file_paths: list[str] = bundle_generator.get_rego_policy_file_path_list()

        for file_path in file_paths:
            if hashlib.sha1(file_path.encode()).hexdigest() == policy_id:
                return PolicyDslVm(
                    file_path=file_path, policy_dsl=open(file_path).read()
                )

        return None
