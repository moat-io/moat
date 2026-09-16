# Release Notes

## Unreleased

### Features

- **Advanced filtering**: the principals and resources views gain a filter panel with
  facets built from live data - source, platform, object type, active status and
  attribute key/value. Values within a facet are OR'd; different facets are AND'ed.
  Applied filters show as removable chips, and attribute pills in the table are
  clickable to filter by them. See [User Interface](ui.md).
- **CSV download honours the filters**: `/principals/download` and
  `/resources/download` now accept the same query parameters as the table and run the
  same query, so the export always matches what is on screen. Pagination is not
  applied - the export is the whole result set.
- **Filter state in the URL**: filters, search, sort and page are pushed to the address
  bar, so a filtered view can be bookmarked, shared and refreshed.
- **MCP server documentation**: how to expose an endpoint as a tool, how to start the
  server and how to register it with a client. See [MCP Server](mcp.md).
- **UI**: row detail now opens in a side drawer rather than a modal; tables gain sticky
  headers, a rows-per-page selector, a density toggle, loading indicators, skeleton
  rows and proper empty states; `/` focuses search and `Esc` clears it.

### Fixes

- **User entitlements API returned 500** (`GET /api/entitlements/v1/users`). It passed
  `source_type` to a repository method that did not accept it. As this is the only
  endpoint tagged `mcp`, the MCP server's only tool was non-functional.
- **Multi-term search was broken**: terms were AND'ed against a single joined attribute
  row, so a search such as `finance admin` could never match. Each term is now matched
  with a correlated `EXISTS`.
- **Resources sort control raised `KeyError`**: it sorted on `databases.database_name`,
  a column that does not exist. It now sorts on `fq_name` and toggles direction.
- **Bundles sort control did nothing**: its `hx-target` named `#opa-bundles-table`,
  which is not present in the page.
- **Attribute filters containing a colon were corrupted** by splitting on every colon
  rather than the first.
- Removed a dead `scope=schemas` branch in the resources view that called a
  commented-out controller method.
- Fixed an unparseable class on `<body>` (`dark:bg-gray-900bg-gray-50`).

### Dependencies

- `fastmcp` and `httpx` are now declared in `requirements.in`; they were imported by
  `moat/src/mcp_serve.py` but never declared.
- Front end upgraded to **htmx 2**, **Tailwind CSS 4** and **Flowbite 4**.
  - Tailwind 4 is configured in CSS; `moat/ui/tailwind.config.js` has been removed and
    its contents moved into `moat/ui/css/input.css`.
  - Flowbite's JS is now imported from npm instead of a vendored copy in
    `moat/ui/js/flowbite.js`, keeping its JS and CSS versions in step.
  - `apexcharts` and the unused `moat/ui/js/charts.js` have been removed.

### Breaking Changes

- The resources CSV export gains an `active` column, so the header is now
  `fq_name,platform,object_type,active,attributes`. Any downstream parser that pins
  column positions needs updating.
- `GET /principals/<id>/history-modal` is now `GET /principals/<id>/detail` and renders
  the drawer contents (identity and attributes as well as history).

## 0.21.0

### Major Features

- **MySQL Support**: Added MySQL as a backend database option alongside PostgreSQL. Users can now choose between PostgreSQL and MySQL based on their infrastructure requirements.

### Breaking Changes

- **Database Schema Recreation Required**: Due to significant changes in the database schema to support multiple database backends, **all existing PostgreSQL databases must be recreated**. Please backup your data before upgrading and follow the migration guide in the deployment documentation.

### Configuration Changes

- New configuration files added:
  - `moat/config/config.docker.mysql.yaml` - MySQL Docker configuration
  - `moat/config/unittest/config.mysql.yaml` - MySQL unit test configuration
  - PostgreSQL unit test config moved to `moat/config/unittest/config.postgres.yaml`

#### bundle_generator.static_rego_file_path
This configuration option now has the `platform` value appended in code, so this should be removed:
- **Existing:** `bundle_generator.static_rego_file_path: "/opt/moat/opa/trino"`
- **New:** `bundle_generator.static_rego_file_path: "/opt/moat/opa"`

### Database Changes

- Consolidated all migrations into a single migration: `cd68c92b9b9c`
- Added database-agnostic array and datetime handling
- Enhanced merge functionality to support both PostgreSQL and MySQL
- Improved history table implementation for cross-database compatibility

### Testing

- Added MySQL unit test fixture
- Updated all repository and service tests to work with both database backends

### Documentation

- Updated deployment documentation with MySQL setup instructions
- Updated developer setup guide with MySQL configuration
