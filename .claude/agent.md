
# Repo Guidelines

* all new python code must have tests in the associated `tests` directory within the same python package
* all sqlalchemy code is in repository classes in `moat/src/repositories`
* all flask code is in `moat/src/views`
* all pydantic are in `moat/src/models`
* repository integration tests must use the `database_empty` or `database` fixture so they run against the configured PostgreSQL or MySQL database; do not substitute SQLite for repository tests
* use `app_logger` heavily to log significant actions, execution progress, decisions, parameters and results, successful outcomes, and failures; include useful context and exception details while avoiding secrets and sensitive data