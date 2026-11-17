from app_config import AppConfigModelBase


class PolicyEvaluatorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "policy_evaluator"

    batch_size: int = 500
    platform: str = "trino"
    opa_batch_path: str = "/v1/data/moat/trino/batch"
    # comma separated user_names evaluated individually instead of by cohort
    special_users: str = ""
    # number of concurrent OPA batch requests
    max_workers: int = 1
    # run from the worker whenever a new bundle is generated for the platform
    evaluate_on_bundle_change: bool = False
