# Resource Metadata Ingestion
Moat currently supports loading resource metadata from `trino` via SQL/DBAPI.
Resources are ingested separately to their attributes to allow flexibility of loading from different sources.
For example, resources could be retrieved from an `information_schema` and the resource attributes from a data catalog.

Both sources require the user to supply a query which returns data with specific column names as defined below for each type.
The output of these queries are loaded into staging tables, then merged into the main tables. This will only succeed if
the records are unique.

## Ingesting Resources
The resource ingestion query must return a full set of resources with the following columns:

| Column name | Type    | Description                            | Example                                                  |
|-------------|---------|----------------------------------------|----------------------------------------------------------|
| fq_name     | varchar | The fully qualified name of the object | `database.schema.table` or `database.schema.table.column` |
| object_type | varchar | The type of the object                 | `table` or `column`                                      |

It is preferred to limit this dataset to only include the objects for which OPA should provide access control.
* Avoid adding all columns to tables which do not have column-level access control
* Avoid adding tables which are not exposed to users

### Example SQL
```sql
select fq_name, object_type from (
    select table_catalog || '.' || table_schema || '.' || table_name as fq_name, 'table' as object_type
    from datalake.information_schema.tables
    where table_schema <> 'information_schema'
    union all
    select table_catalog || '.' || table_schema || '.' || table_name || '.' || column_name as fq_name, 'column' as object_type
    from datalake.information_schema.columns
    where table_schema <> 'information_schema'
)
```

## Ingesting Resource Attributes
The resource attribute ingestion query must return a full set of resources with the following columns:

| Column name     | Type    | Description                            | Example                                                   |
|-----------------|---------|----------------------------------------|-----------------------------------------------------------|
| fq_name         | varchar | The fully qualified name of the object | `database.schema.table` or `database.schema.table.column` |
| attribute_key   | varchar | The key of the attribute               | `Subject`                                                 |
| attribute_value | varchar | The value of the attribute             | `HR`, `Sales`                                             |

### Example SQL
```sql
SELECT 
    table_catalog || '.' || table_schema || '.' || table_name AS fq_name,
    attribute_key,
    attribute_value
FROM datalake.information_schema.tables tbls
JOIN data_catalog.public.attributes cat on tbls.fq_name = cat.table_name
```