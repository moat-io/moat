from ..src.policies_controller import PoliciesController
from views.models import PolicyDslVm


def test_get_all_policies():
    controller = PoliciesController()
    policies = controller.get_all_policies()

    assert isinstance(policies, list)
    assert len(policies) > 0

    for policy in policies:
        assert isinstance(policy, PolicyDslVm)
        assert policy.file_path.endswith(".rego")
        assert len(policy.policy_dsl) > 0

        # Verify that policy_dsl content matches the file content
        with open(policy.file_path, "r") as f:
            assert policy.policy_dsl == f.read()


def test_get_by_id():
    controller = PoliciesController()
    policy = controller.get_by_id("does not exist")
    assert policy is None

    policy = controller.get_by_id("1106eada510eabcd95f7f9709830416a5ba3769a")
    assert policy.file_path == "moat/test/test_data/rego/trino/common.rego"
