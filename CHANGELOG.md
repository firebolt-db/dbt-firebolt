# Changelog

## v.0.21.3

### Breaking Changes

- `host` has been renamed to `api_endpoint` please update your `profiles.yml` accordingly

### Features

- dbt-firebolt now connects to Firebolt using the native Firebolt SDK rather than through a JDBC driver, removing the necessity for a Java runtime.

### Fixes

## v.0.21.2

### Readme

- Updated and edited various sections of readme, adding new tables and examples for clarity. [#6](https://github.com/firebolt-db/dbt-firebolt/pull/6)

### Breaking Changes

- `engine_name` has been renamed to `engine` please update your `profiles.yml` accordingly [#4](https://github.com/firebolt-db/dbt-firebolt/pull/4)
### Features

- added ability to specify an account for users who have more than one account associated with their credentials [#4](https://github.com/firebolt-db/dbt-firebolt/pull/4)
### Fixes

- fixed bug where database connection URL used backslashes on Windows due to `os.path.join` [#5](https://github.com/firebolt-db/dbt-firebolt/pull/5)


## v.0.21.1

### Fixes

- removed log statements and an extra `firebolt__get_create_index_sql` macro error via [#2](https://github.com/firebolt-db/dbt-firebolt/pull/2)

- added ability to specify an account for users who have more than one account associated with their credentials

## v.0.21.0

### Features

- Initial release for PyPI!

### Fixes

- Many
### Under the Hood

- A lot of work!

### Contributors

- [@ima-hima](https://github.com/ima-hima) and [@swanderz](https://github.com/swanderz)
