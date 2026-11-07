from ..src.policy_evaluator_config import PolicyEvaluatorConfig


def test_defaults() -> None:
    config = PolicyEvaluatorConfig()
    assert config.batch_size == 500
    assert config.platform == "trino"
    assert config.opa_batch_path == "/v1/data/moat/trino/batch"


def test_load_falls_back_to_defaults() -> None:
    # the unittest config file does not define any policy_evaluator.* keys, so
    # load() must leave the code defaults in place
    config = PolicyEvaluatorConfig().load()
    assert config.batch_size == 500
    assert config.platform == "trino"
    assert config.opa_batch_path == "/v1/data/moat/trino/batch"
