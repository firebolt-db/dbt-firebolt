# dbt-firebolt

The [dbt](https://www.getdbt.com) adapter for [Firebolt](https://www.firebolt.io/). Supports dbt 0.21+.

## Installation

1. Download the [Firebolt JDBC driver](https://firebolt-publishing-public.s3.amazonaws.com/repo/jdbc/firebolt-jdbc-1.18-jar-with-dependencies.jar) and place it wherever you'd prefer.
2. If you've never installed a Java Runtime Environment you will need to download and install one from either [OpenJDK](https://openjdk.java.net/install/) or [Oracle](https://www.oracle.com/java/technologies/downloads/).
3. Install the dbt-firebolt package from pypi:
   ```
   pip install dbt-firebolt
   ```

## Setup

### Connecting to Firebolt

To connect to Firebolt from dbt, you'll need to add a new [profile](https://docs.getdbt.com/dbt-cli/configure-your-profile) to your `profiles.yml` file. A Firebolt profile uses the following fields:


| Field                    | Description |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`                   | Must be set to `firebolt`. This must be included either in `profiles.yml` or in the `dbt_project.yml` file. |
| `user`                   | Your Firebolt username. |
| `password`               | Your Firebolt password. |
| `database`               | The name of your Firebolt database. |
| `schema`                 | A string to prefix the names of generated tables with. For an example of how this works, see [this section below](https://github.com/firebolt-db/dbt-firebolt#dbt-projects-with-concurrent-users). |
| `jar_path`               | The path to your JDBC driver on your local drive. |
| `threads`                | Must be set to `1`. Multi-threading is not currently supported. |
| `engine_name` (Optional) | The name (not the URL) of the Firebolt engine to use. If omitted, it will use your default engine. |
| `host` (Optional)        | The host name of the connection. For all customers it is `api.app.firebolt.io`, which will be used if omitted. |
| `account` (Optional)     | The account name (not the account ID). If omitted, it will use your default account. |

Example `profiles.yml`:

```
<my_project>:
  target: fb_app
  fb_app:
      type: firebolt
      user: <my_username>
      password: <my_password>
      database: <my_db_name>
      schema: <my_schema_name>
      jar_path: <path_to_my_local_jdbc_jar>
      threads: 1
    # Optional
      engine: <my_engine>
      host: api.app.firebolt.io
      account: <my_account_name>
```

### Troubleshooting Connections

If you encounter issues connecting to Firebolt from dbt, make sure the following criteria are met:
- The engine must be a general-purpose read-write engine, not an analytics engine.
- You must have adequate permissions to access the engine.
- The engine must be running.
- If you're not using the default engine for the database, you must specify an engine name.
- If there is more than one account associated with your credentials, you must specify an account.

## Feature Support

The table below shows which dbt and Firebolt features are supported by the adapter. dbt-firebolt is under active development and will be gradually unlocking more features over time.

| Feature          | Supported          |
|------------------|--------------------|
| Table materializations     | :white_check_mark: |
| Ephemeral materializations       | :white_check_mark: |
| View materializations | :x: |
| Incremental materializations | :x: |
| Seeds | :white_check_mark: |
| Tests            | :white_check_mark: |
| Documentation             | :white_check_mark: |
| Snapshots         | :x:                |
| Custom schemas | :x: (see [workaround](https://github.com/firebolt-db/dbt-firebolt#dbt-projects-with-concurrent-users)) |
| Custom databases | :x: |
| Source freshness | :white_check_mark: |
| External tables | :white_check_mark: |
| Primary indexes | :white_check_mark: |
| Aggregating indexes | :white_check_mark: |
| Join indexes | :white_check_mark: |
| Partitioned tables | :x: |

## Model Configuration in Firebolt

### Dimension and Fact Tables

Firebolt [fact and dimension tables](https://docs.firebolt.io/concepts/working-with-tables#fact-and-dimension-tables) can both be read in dbt, but they need to be specified in a config block. You can include these config settings in any of these locations:
1. At the top of a dbt model SQL file (see below). 
  ```
{{
    config(
      materialized = 'table',
      table_type = 'dimension',
      primary_index = ['customer_id', 'first_name']
      )
}}
  ```
2. As a model config in the `dbt_project.yml` file.
```yml
models:
  <my_project>
    materialized: 'table'
    table_type =  ‘fact’ 
    primary_index = ['customer_id', 'first_name']
```

|     Field     |                                                     Description                                                      |
|---------------|----------------------------------------------------------------------------------------------------------------------|
| `materialized`  | Use `table`                                                                                                            |
| `table_type`    | Can be either `fact` or `dimension`                                                                                  |
| `primary_index` | Required for fact tables and can be either a string or a list of strings for the column names of your primary index. |

Read more in [dbt docs on configuring models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#configuring-models)

### Join and Aggregating Indexes

In addition to [primary indexes](https://docs.firebolt.io/concepts/get-instant-query-response-time#how-to-choose-your-primary-index), Firebolt also supports:
- [Aggregating](https://docs.firebolt.io/concepts/get-instant-query-response-time#get-sub-second-query-response-time-using-aggregating-indexes)
- [Join](https://docs.firebolt.io/concepts/get-instant-query-response-time#accelerate-joins-using-join-indexes)

dbt-firebolt follows the same convention for indexes as was introduced to dbt-postgres ([more info on indexes usage in dbt-postgres](https://docs.getdbt.com/reference/resource-configs/postgres-configs#indexes)).

#### Naming

In dbt-firebolt, indexes are named as follows, with the number being a unix timestamp at the time of execution
- **Syntax**: `<table-name>__<key-column>__<index-type>_<unix-timestamp>`
- **Join index example**: `my_orders__order_id__join_1633504263`
- **Aggregating index example**: `my_orders__order_id__aggregating_1633504263`

#### Usage
To use Firebolt aggregating or join indexes in dbt, you need to add a list of dictionaries to your dbt config block. The following configurations must be included: 

#### Aggregating index

|     key     |                                                                                           value                                                                                           |
|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`        | `aggregating`                                                                                                                                                                             |
| `key_column`  | The table column specified as key_column when the aggregating index was created. See [Firebolt docs](https://docs.firebolt.io/sql-reference/commands/ddl-commands#create-join-index) for more information |
| `aggregation` | One or more aggregation functions performed as part of the index                                                                                                                          |

#### Example of fact table with aggregating index
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
#### Join index

|       key        |                                       value                                        |
|------------------|------------------------------------------------------------------------------------|
| `type`             | `join`                                                                               |
| `join_column`      | The join_column specified when the join index was created.                         |
| `dimension_column` | One or more dimension columns that were specified when the join index was created. |



#### Example of dimension table with join index
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
#### Example of fact table with two aggregating indexes and one join index
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

Currently with `dbt-firebolt`, all models will be run in the same schema, so the `schema` provided isn't used, but is still required. If your team consists of multiple analytics engineers using the same database and engine, we recommend adding the following macro to your project. This will prefix the model name/alias with the schema value to provide namespacing so that multiple developers are not interacting with the same set of models.

```sql
-- macros/generate_alias_name.sql
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        {{ node.schema }}__{{ node.name }}

    {%- else -%}

        {{ node.schema }}__{{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}
```

For an example of how this works, let’s say Shahar and Eric are both working on the same project.


In their `.dbt/profiles.yml`, Sharar provides `schema=sh`, and Eric, `schema=er`. When they each run the `customers` model, the models will land in the database as `sh_customers` and `er_customers`, respectively.



### External Tables

More information on dbt's use of external tables can be found in the dbt [documentation](https://docs.getdbt.com/reference/resource-properties/external).

More information on using external tables including properly configuring IAM can be found in the Firebolt [documentation](https://docs.firebolt.io/sql-reference/commands/ddl-commands#create-external-table).

#### Installation

To install and use dbt-external-tables with firebolt, you must:
1. Add this package to your packages.yml:
    ```yml
    packages:
    - package: dbt-labs/dbt_external_tables
        version: <VERSION>
    ```
2. add these fields to your `dbt_project.yml`: 
    ```yml
    dispatch:
      - macro_namespace: dbt_external_tables
        search_order: ['dbt', 'dbt_external_tables']
    ```
3. Pull in the packages.yml dependencies by calling `dbt deps`
#### Usage

To use external tables, you must define a table as `EXTERNAL` in your `dbt_project.yml` file. Every external table must contain fields for url, type, and object pattern. Note that the Firebolt external table specifications require fewer fields than what is specified in the dbt documentation.

In addition to specifying the columns, an external table may specify partitions. Partitions are not columns and they cannot have the same name as columns. To avoid yaml parsing errors the url, object_pattern, and regex values should be encased in quotation marks. .

#### Example dbt_project.yml file for an external table

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
