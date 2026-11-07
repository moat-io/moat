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

## Reading the results

The stored grants are also served by the ACL API and shown in the
[Access Control List view](ui.md#access-control-list-view), so neither the UI nor a
client needs to query the table directly.

```bash
curl -s "http://127.0.0.1:8001/api/acl/v1/grants?search=trino&page_size=2"
```

| Parameter | Default | Purpose |
| --- | --- | --- |
| `page` | `1` | Page to return, from 1. |
| `page_size` | `20` | Rows per page, at most 100. |
| `search` | - | Free text match on principal, resource, platform or action. |
| `sort_key` | `principal_fq_name` | Column to sort on; an unknown column falls back to the default. |
| `sort_ascending` | `true` | Sort direction. |
| `active` | `true` | `false` returns the grants that have been revoked. |

The response is `{"page", "page_size", "total", "results"}`, where each result carries
the columns of the table above. The endpoint is not tagged `mcp`, so it is not exposed
as an [MCP tool](mcp.md).

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
`policy_evaluator.batch_size`, `policy_evaluator.platform`, and
`policy_evaluator.opa_batch_path`.
