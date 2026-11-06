---
name: run-opa-tests
description: Run the Moat OPA/Rego policy tests. Use when asked to run or verify OPA tests, or to check a Rego policy change.
---

# Run the OPA policy tests

Run the Trino Rego test suite from the repository root:

```bash
opa test opa/trino -v
```

This is also available through the project test script:

```bash
./test.sh opa
```

The suite includes all policy and test files in `opa/trino`, including
`batch_mode_test.rego`. Print the complete test output, including each test's
result and the final pass/fail summary. Do not truncate or summarize the output.
If `opa` is not installed or is not on `PATH`, report that clearly; do not
install tools unless the user asks.

When asked only to run tests, do not edit Rego policies or tests. If the user
asks to fix a failure, inspect the failing test and relevant policy first,
make a focused change, then rerun the full suite.
