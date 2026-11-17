---
name: run-python-tests
description: Run the Moat Python tests with pytest. Use when asked to run or verify Python tests in this repository.
---

# Run the Moat Python tests

Always run pytest from the repository root (`moat-io/moat`). Pass a test path
under `moat/src` at minimum, and set `PYTHONPATH=moat/src` so the test
configuration imports the project's helper package (`moat/src/test`) instead
of Python's standard-library `test` package. Do not `cd moat` before invoking
pytest.

## Commands

Run the full Python suite:

```bash
PYTHONPATH=moat/src pytest moat/src
```

Run a targeted test file while preserving the repository-root working
directory:

```bash
PYTHONPATH=moat/src pytest moat/src/repositories/test/resource_repository_test.py
```

Use the corresponding path under `moat/src` for a test directory, test file,
or individual test node. For example:

```bash
PYTHONPATH=moat/src pytest moat/src/views/test/acls_view_test.py
PYTHONPATH=moat/src pytest moat/src/views/test/acls_view_test.py::test_acls_index
```

If `pytest` is not available on `PATH`, use the repository virtual environment
from the same root directory:

```bash
PYTHONPATH=moat/src ./venv/bin/pytest moat/src
```

Do not install dependencies unless pytest reports a missing dependency. Report
test failures and environment/setup errors distinctly; do not treat a failed
test run as a passing verification.
