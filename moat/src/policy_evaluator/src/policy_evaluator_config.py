from app_config import AppConfigModelBase


class PolicyEvaluatorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "policy_evaluator"

    batch_size: int = 500
    platform: str = "trino"
    opa_batch_path: str = "/v1/data/moat/trino/batch"
