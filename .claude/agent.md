
# Repo Guidelines

* all new python code must have tests in the associated `tests` directory within the same python package
* all code must be covered by unit tests using pytest
* all sqlalchemy code is in repository classes in `moat/src/repositories`
* all flask code is in `moat/src/views`
* all pydantic are in `moat/src/models`
* each python file should contain a single class, with the class name matching the file name where the class name is PascalCase and the file name is snake_case
* every newly created class must be imported in the `__init__.py` files at each level of its package hierarchy, all the way up to the content root, so that it is exported and importable from the top-level package
* instantiated objects should have explicit type hints, e.g. `policy_evaluator: PolicyEvaluator = PolicyEvaluator()`
* when calling a function that returns a tuple, any item of the tuple that is not used should be replaced with an underscore, e.g. `_, resources = ResourceRepository.get_all_by_platform(session, "trino")`
* any code which needs to interact with open policy agent should should use the `OpaClient` class in `moat/src/opa_client.py

## Policy evaluator feature
The policy evaluator is a task which uses the local open policy agent instance to determine which tables and columns
are allowed to be queried by each user. The results are stored in the database. The policy evaluator is run as a
background task.

The database table which stores the information is `policy_evaluator_results`. Its schema is:

| Column Name   | Data Type | Description                         |
|---------------|-----------|-------------------------------------|
| id            | INTEGER   | Unique identifier for the result    |
| principal     | TEXT      | username of the principal           |
| resource_type | TEXT      | Database, schema, table, column etc |
| resource      | TEXT      | Name of the resource                |

### Process:
* Create a new database transaction
* Truncate the policy evaluator results table
* Query the database for all principals
* Query the database for all platforms
* For each platform:
  * Query the database for all resources in configurable batch sizes
  * Query the local OPA agent in batch mode for the batch of resources for each principal
  * Insert the results in the database
* Commit the database transaction


### Implementation:
* all policy evaluator code is in `moat/src/policy_evaluator/src`
* all policy evaluator tests are in `moat/src/policy_evaluator/tests`
* The entrypoint for the policy evaluator is `moat/src/policy_evaluator/src/policy_evaluator.py` and contains a class called `PolicyEvaluator`
* The policy evaluator will be run as a background task, called by the `Worker` class in `moat/src/worker.py`
* The `Worker` class will be responsible for scheduling the policy evaluator to run periodically
* The `OPAClient` class in `moat/src/opa_client.py` is used to query the local OPA agent. It will need to be modified
  * to support batching of queries
  * it will use the trino implementation of OPA policies

