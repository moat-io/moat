# Policy Evaluator

The policy evaluator materializes OPA's `FilterTables` decisions as a
queryable snapshot of which principals can access which tables. The results are
stored in the Moat PostgreSQL database in `policy_evaluator_results`; consumers
can query this table instead of requesting the current set of grants from OPA.
The snapshot is refreshed each time the evaluator runs.

## How it works

For the configured `policy_evaluator.platform`, the evaluator:

1. Loads the principals from the platform's current OPA bundle (the latest row
   in `opa_bundles`), so grouping uses exactly the data OPA evaluates against.
   Principal `fq_name`s are looked up in the database by `user_name`; bundle
   principals missing from the database are skipped. Resources belonging to the
   platform are loaded from the database.
2. Selects table resources whose fully qualified name has the form
   `catalog.schema.table`.
3. Groups principals into cohorts with identical attributes, entitlements and
   groups in the bundle. Users
   listed in `policy_evaluator.special_users` each get a cohort of their own.
4. For one representative of each cohort, sends the tables to OPA in batches of
   `policy_evaluator.batch_size` (default `500`), with up to
   `policy_evaluator.max_workers` (default `1`) requests in flight. It evaluates
   the `FilterTables` operation at `policy_evaluator.opa_batch_path` (default
   `/v1/data/moat/trino/batch`); OPA returns the indices of allowed tables, and
   the result is applied to every member of the cohort.
5. Reconciles the allowed principal/table pairs with the database rows for the
   configured platform and action:
   - new grants are inserted;
   - grants that become allowed again are reactivated;
   - grants no longer allowed are soft-deleted by setting `active = false`.

   Inserted and changed rows record the bundle's `e_tag` and `policy_hash`.

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
| `bundle_e_tag` | ETag of the bundle that produced the row's last change. |
| `bundle_policy_hash` | Policy hash of that bundle. |
| `active` | Whether the grant is currently allowed; revoked grants are retained with `false`. |
| `record_created_date` | When the row was first created. |
| `record_updated_date` | When the row was last changed. |

Indexes back the ACL API's queries: `(record_created_date, id)` for the default
sort and `newer_than`, `resource_fq_name` for the resource prefix filter, and on
PostgreSQL trigram (`pg_trgm`) indexes on `principal_fq_name` and
`resource_fq_name` for search. Search is unindexed on MySQL.

For example, query current grants with:

```sql
SELECT principal_fq_name, resource_fq_name
FROM policy_evaluator_results
WHERE platform = 'trino'
  AND action = 'FilterTables'
  AND active = true;
```

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

## Running on bundle change

Set `policy_evaluator.evaluate_on_bundle_change: "true"` to have the worker run
the evaluator itself. After each bundle refresh the worker compares the ETag of
the platform's current bundle with the one before the refresh, and evaluates
when it changed (including the first bundle). A new bundle is only generated
when principal, resource or policy data changed. The bundle's `policy_hash`
is not used because it covers the policy files only, not the principal and
resource data. The evaluation runs in
the worker loop, so the next bundle refresh waits until it finishes.

## Configuration

See [Configuration Properties](config/properties.md) for
`policy_evaluator.batch_size`, `policy_evaluator.platform`,
`policy_evaluator.opa_batch_path`, `policy_evaluator.special_users`, and
`policy_evaluator.max_workers`.
