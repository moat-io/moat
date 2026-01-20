from ..src.policy_dsl_vm import PolicyDslVm


def test_policy_dsl_vm() -> None:
    assert (
        PolicyDslVm(file_path="opa/trino/decision_logs.rego", policy_dsl="").policy_name
        == "Decision Logs"
    )
    assert (
        PolicyDslVm(file_path="opa/trino/decision_logs.rego", policy_dsl="").id
        == "198dd79e13c0ac6e311f4b8ce1fac965781c1be8"
    )

    assert (
        PolicyDslVm(
            file_path="opa/trino/column_masking.rego", policy_dsl=""
        ).policy_name
        == "Column Masking"
    )
    assert (
        PolicyDslVm(file_path="opa/trino/column_masking.rego", policy_dsl="").id
        == "19005cb160034a3f0a898343fae2ba14dfd6e63d"
    )
