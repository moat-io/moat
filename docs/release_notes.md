# Release Notes

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
* **Existing:** `bundle_generator.static_rego_file_path: "/opt/moat/opa/trino"`
* **New:** `bundle_generator.static_rego_file_path: "/opt/moat/opa"`

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
