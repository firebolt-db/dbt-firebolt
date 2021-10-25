# dbt-firebolt
[dbt](https://www.getdbt.com) adapter for [Firebolt](https://www.firebolt.io/)

dbt-firebolt supports dbt 0.21 and newer

## Installation

First, download the [JDBC driver](https://firebolt-publishing-public.s3.amazonaws.com/repo/jdbc/firebolt-jdbc-1.15-jar-with-dependencies.jar) and place it wherever you'd prefer.
If you've never installed a Java Runtime Environment you will need to download and install one from either [OpenJDK](https://openjdk.java.net/install/) or [Oracle](https://www.oracle.com/java/technologies/downloads/).

Now install dbt-firebolt. For the current version:
```
pip install git+https://github.com/firebolt-analytics/dbt-firebolt.git
```

## Setup

### Engines
For dbt to function with Firebolt you must have an engine connected to your database and available. In addition, these needs must be met:

1. The engine must be a general-purpose read-write engine.
1. You must have permissions to access the engine.
1. The engine must be running.
1. *If* you're not using the default engine for the database, the name of the engine must be specified.

### YAML configuration file
You'll need to add your project to the `profiles.yml` file. These fields are necessary:

- `type`
- `user`
- `password`
- `database`
- `schema`
- `jar_path`

These fields are optional:

- `engine_name` (See note above.)
- `host` (The host defaults to `api.app.firebolt.io`. If you want to use a dev account you must include the `host` field and set it to `api.dev.firebolt.io`.)

Note that, although the value of `type` is always `firebolt`, it must be included either in `profiles.yml` or in the dbt_project.yml file for your application.

#### Example file:
```
my_project:
  target: fb_app
  fb_app:
      type: firebolt
      user: <my_username>
      password: <my_password>
      database: <my_db_name>
      schema: <my_schema_name>
      jar_path: <path_to_my_local_jdbc_jar>
      threads: 1
    # The following two fields are optional. Please see the notes above.
      engine_name: <my_engine_name>
      host: api.app.firebolt.io
```

## dbt feature support
| feature          | supported          |
|------------------|--------------------|
| tables/views     | :white_check_mark: |
| ephemeral        | :white_check_mark: |
| tests            | :white_check_mark: |
| docs             | :white_check_mark: |
| incremental      | :x:                |
| snapshot         | :x:                |
| source_freshness | :white_check_mark: |


## Model Configuration in Firebolt

### Dimension and Fact Tables

Both [fact and dimension tables](https://docs.firebolt.io/concepts/working-with-tables#fact-and-dimension-tables) are supported.
When `materialized='table'`, `table type` and `primary index` configurations can be set. `table type` can be either `fact` or `dimension`. `primary_index` is required for fact tables, and can be a string or a list of strings.

These configs can be set by either:
1. a config block at the top of that model's SQL file (like below), or
  ```sql
  {{
    config(
      materialized = 'table',
      table_type = 'dimension',
      primary_index = ['customer_id', 'first_name']
      )
  }}
  ```
2. in the `dbt_project.yml` or model schema YAML file.

Read more in [dbt docs on configuring models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#configuring-models)

### Join and Aggregating Indexes

In addition to [primary indexes](https://docs.firebolt.io/concepts/get-instant-query-response-time#how-to-choose-your-primary-index), Firebolt also supports:
- [Aggregating](https://docs.firebolt.io/concepts/get-instant-query-response-time#get-sub-second-query-response-time-using-aggregating-indexes)
- [Join](https://docs.firebolt.io/concepts/get-instant-query-response-time#accelerate-joins-using-join-indexes)

dbt-firebolt follows the same convention for indexes as was introduced to dbt-postgres ([more info on indexes usage in dbt-postgres](https://docs.getdbt.com/reference/resource-configs/postgres-configs#indexes)).

#### Naming

In dbt-firebolt, indexes are named as follows, with the number being a unix timestamp at the time of execution
- template: `table-name__key-column__index-type_unix-timestamp`
- join index: `my_orders__order_id__join_1633504263`
- aggregating index: `my_orders__order_id__aggregating_1633504263`

#### Usage
The `index` argument takes a list of dictionaries, where each dictionary is an index you'd like to define. there are two `types` of indexes that can be defined here: `aggregating` and `join`. The required fields for each index are as follows:
- `aggregating`: `key_column` (string) and `aggregation` (string of list of strings)
- `join`: `join_column` (string) and `dimension_column` (string of list of strings)

#### Fact table with aggregating index
```sql
-- orders.sql
{{
  config(
    materialized = 'table',
    table_type = 'fact',
    primary_index = 'id',
    indexes = [
      {
        'type': 'aggregating',
        'key_column': 'order_id',
        'aggregation': ['COUNT(DISTINCT status)', 'AVG(customer_id)']
      }
    ]
    )
}}
```
#### Dimension table with join index
```sql
-- orders.sql
{{
  config(
    materialized = 'table',
    table_type = 'dimension',
    indexes = [
      {
        'type': 'join',
        'join_column': 'order_id',
        'dimension_column': ['customer_id', 'status']
      }
    ]
    )
}}
```
#### Fact table with two aggregating indexes and one join index
```sql
-- orders.sql
{{
  config(
    materialized = 'table',
    table_type = 'dimension',
    indexes = [
      {
        'type': 'aggregating',
        'key_column': 'order_id',
        'aggregation': ['COUNT(DISTINCT status)', 'AVG(customer_id)']
      },
      {
        'type': 'aggregating',
        'key_column': 'customer_id',
        'aggregation': 'COUNT(DISTINCT status)'
      },
      {
        'type': 'join',
        'join_column': 'order_id',
        'dimension_column': ['customer_id', 'status']
      }
    ]
    )
}}
```


## Recommended dbt project configurations

### `quote_columns`

To prevent a warning, you should add a configuration as below to your `dbt_project.yml`. For more info, see the [relevant dbt docs page](https://docs.getdbt.com/reference/resource-configs/quote_columns)

```yml
seeds:
  +quote_columns: false # or `true` if you have csv column headers with spaces
```

### dbt projects with concurrent users

Currently, with `dbt-firebolt`, all models will be run in the same schema, so the `schema` provided isn't used, but is still required. If you are a team of analytics engineers using a the same database and engine, a recommended practice is to add the following macro to your project. It will prefix the model name/alias with the schema value to provide namespacing so that multiple developers are not interacting with the same set of models.

For example, consider two analytics engineers on the same project: Shahar and Eric.

If in their `.dbt/profiles.yml`, Sharar provides `schema=sh`, and Eric, `schema=er`, when they each run the `customers` model, the models will land in the database as `sh_customers` and `er_customers`, respectively.


```sql
-- macros/generate_alias_name.sql
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        {{ node.schema }}_{{ node.name }}

    {%- else -%}

        {{ node.schema }}_{{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}
```

### External Tables

Documentation on dbt's use of external tables can be found in the dbt [documentation](https://docs.getdbt.com/reference/resource-properties/external).

Documentation on using external tables including properly configuring IAM can be found in the Firebolt [documentation](https://docs.firebolt.io/sql-reference/commands/ddl-commands#create-external-table).

#### Installation

To install and use dbt-external-tables with firebolt, you must:
1. Add the package to your `packages.yml`,
    ```yml
    packages:
    - package: dbt-labs/dbt_external_tables
        version: <VERSION>
    ```
2. add this to your `dbt_project.yml`, and
    ```yml
    dispatch:
      - macro_namespace: dbt_external_tables
        search_order: ['dbt', 'dbt_external_tables']
    ```
3. call `dbt deps`
#### Usage

To use external tables, you must define a table as `EXTERNAL` in your project.yml file. Every external table must contain fields for url, type, and object pattern. Note that the Firebolt external table specification differs slightly from the dbt specification in the dbt documentation in that it does not require all the fields shown in dbt's documentation.

In addition to specifying the columns, an external table may specify partitions. Partitions are not columns and a partition name cannot have the same name as a column. An example yaml file follows. In order to avoid yml parsing errors it is likely necessary to quote at least the `url`, `object_pattern`, and `regex` values.

```yml
sources:
  - name: firebolt_external
    schema: "{{ target.schema }}"
    loader: S3

    tables:
      - name: <table_name>
        external:
          url: 's3://<bucket_name>/'
          object_pattern: '<regex>'
          type: '<type>'
          credentials:
            internal_role_arn: arn:aws:iam::id:<role>/<bucket_name>
            external_role_id: <external-id>
          object_pattern: '<regex>'
          compression: '<compression_type>'
          partitions:
            - name: <partition_name>
              data_type: <partition_type>
              regex: '<partition_definition_regex>'
          columns:
            - name: <column_name>
              data_type: <type>
```
## Changelog

See our [changelog](CHANGELOG.md) or our release history for more info
