# Policy Evaluator

The policy evaluator materializes OPA's `FilterTables` decisions as a
queryable snapshot of which principals can access which tables. The results are
stored in the Moat PostgreSQL database in `policy_evaluator_results`; consumers
can query this table instead of requesting the current set of grants from OPA.
The snapshot is refreshed each time the evaluator runs.

## How it works

For the configured `policy_evaluator.platform`, the evaluator:

1. Loads active principals and resources belonging to that platform.
2. Selects table resources whose fully qualified name has the form
   `catalog.schema.table`.
3. For each principal, sends the tables to OPA in batches of
   `policy_evaluator.batch_size` (default `500`). It evaluates the `FilterTables`
   operation at `policy_evaluator.opa_batch_path` (default
   `/v1/data/moat/trino/batch`); OPA returns the indices of tables allowed for
   that principal.
4. Reconciles the allowed principal/table pairs with the database rows for the
   configured platform and action:
   - new grants are inserted;
   - grants that become allowed again are reactivated;
   - grants no longer allowed are soft-deleted by setting `active = false`.

Rows are not physically deleted when a grant is revoked. Updates to existing
rows refresh `record_updated_date`; `record_created_date` remains the time the
grant was first recorded.

## Where results are written

Results are written to the `policy_evaluator_results` table in the configured
Moat database. Each row represents one principal/resource grant:

| Column | Description |
|---|---|
| `principal_fq_name` | Fully qualified principal name. |
| `resource_fq_name` | Fully qualified table resource name. |
| `platform` | Platform evaluated, configured by `policy_evaluator.platform`. |
| `action` | OPA operation evaluated (`FilterTables`). |
| `active` | Whether the grant is currently allowed; revoked grants are retained with `false`. |
| `record_created_date` | When the row was first created. |
| `record_updated_date` | When the row was last changed. |

For example, query current grants with:

```sql
SELECT principal_fq_name, resource_fq_name
FROM policy_evaluator_results
WHERE platform = 'trino'
  AND action = 'FilterTables'
  AND active = true;
```

## Mapping ACLs to generic roles

`map-acl-roles` assigns each active ACL row to a generic role and stores the
assignment in `roles`. The table has one row
per ACL entry: `policy_evaluator_result_id` references the ACL row's existing
primary key (`policy_evaluator_results.id`), and `role_id` identifies its
role. No role spans platform instances or actions.

Roles are formed by grouping each platform/action's sorted distinct principals
and resources into configured-size buckets. Every mapping in a bucket pair has
at most the configured number of distinct users and resources. Role IDs are
derived from the platform, action, and bucket positions; they are opaque and
may change when ACL changes shift bucket boundaries or when limits change.
Inactive and incomplete ACL rows are not assigned.

The mapper performs ranking, assignment, upsert, and cleanup in SQL; ACL rows
are not loaded into Python. Each run recalculates assignments from active ACLs
in the database, but only inserts, changes, or removes mapping rows whose
assignments differ. This favors correctness when grants are deactivated or
changed while keeping the large data operation inside PostgreSQL. It does not
connect to any of the platforms represented by the ACLs.

Run it after the database migration:

```bash
python -m cli.src.cli map-acl-roles
```

The default limits are 500 distinct users and 500 distinct resources per role.
Set `role_mapper.max_users_per_role` and
`role_mapper.max_resources_per_role` to positive integers to match a target
platform's limits. Roles are generic database records; platform-specific
adapters can read the ACL entries for each role and apply them to their target.

## Invoking from the CLI

The Click CLI command is `evaluate-policy`. Run it in an environment configured
to connect to both PostgreSQL and OPA. For the repository's Docker Compose
development environment, start PostgreSQL and OPA, then run:

```bash
podman-compose up -d postgres opa
./test.sh policy-evaluator
```

The helper runs `python -m cli.src.cli evaluate-policy` in a one-off Moat
container, with `CONFIG_FILE_PATH=config/config.docker.yaml`. It does not start
the services, migrate the database, or populate principals and resources.
Ensure the database is migrated and contains the relevant principals and
platform resources before invoking the command. In another configured
environment, invoke the CLI directly:

```bash
python -m cli.src.cli evaluate-policy
```

or in a container:
```bash
./entrypoint.sh policy-evaluator
```

The evaluator is suitable for scheduled execution, such as a Kubernetes
`CronJob`, to keep the stored snapshot current.

## Configuration

See [Configuration Properties](config/properties.md) for
`policy_evaluator.batch_size`, `policy_evaluator.platform`,
`policy_evaluator.opa_batch_path`, `role_mapper.max_users_per_role`, and
`role_mapper.max_resources_per_role`.
