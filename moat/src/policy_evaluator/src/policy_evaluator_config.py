from app_config import AppConfigModelBase


class PolicyEvaluatorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "policy_evaluator"
    batch_size: int = 100
