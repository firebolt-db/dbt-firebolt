<img width="1113" alt="Screen Shot 2021-12-10 at 1 09 09 PM" src="https://user-images.githubusercontent.com/7674553/145641621-a7dabe78-da92-4f0a-bbd2-54ccf7f34b57.png">


# dbt-firebolt

The [dbt](https://www.getdbt.com) adapter for [Firebolt](https://www.firebolt.io/). dbt-firebolt offers the following benefits to Firebolt customers:
* SQL-based data transformation
* Life cycle management for native Firebolt objects (fact tables, dimension tables, primary indexes, aggregating indexes, join indexes, etc.)
* Declarative, version-controlled data modeling
* Auto-generated data lineage and documentation

dbt-firebolt supports dbt 1.0+.


## Installation

Install the dbt-firebolt package from PyPI:
   ```
   pip install dbt-firebolt
   ```


## Setup

#### Connecting to Firebolt

To connect to Firebolt from dbt, you'll need to add a new Firebolt profile to your `profiles.yml` file. Please see the [dbt documentation on Firebolt profiles](https://docs.getdbt.com/reference/warehouse-profiles/firebolt-profile#connecting-to-firebolt) to set it up.

#### Setup Recommendations

For the best experience we recommend that you make the following changes to your dbt project:
* [Set an explicit value for `quote_columns`](https://docs.getdbt.com/reference/resource-configs/firebolt-configs#setting-quote_columns)
* [Add the `generate_alias_name` macro to your project](https://docs.getdbt.com/reference/warehouse-profiles/firebolt-profile#supporting-concurrent-development)


## Feature Support

The table below shows which dbt and Firebolt features are supported by the adapter. dbt-firebolt is under active development and will be gradually unlocking more features over time.

| Feature                      | Supported          |
|------------------------------|--------------------|
| Table materializations       | :white_check_mark: |
| Ephemeral materializations   | :white_check_mark: |
| View materializations        | :white_check_mark: |
| Incremental materializations - append | :white_check_mark: |
| Incremental materializations - insert_overwrite | :white_check_mark: |
| Incremental materializations - delete+insert | :white_check_mark: |
| Incremental materializations - merge | :x: |
| Snapshots                    | :white_check_mark: |
| Seeds                        | :white_check_mark: |
| Tests                        | :white_check_mark: |
| Documentation                | :white_check_mark: |
| Custom schemas               | :x: (see [workaround](https://docs.getdbt.com/reference/warehouse-profiles/firebolt-profile#supporting-concurrent-development)) |
| Custom databases             | :x: |
| Source freshness             | :white_check_mark: |
| External tables              | :white_check_mark: |
| Primary indexes              | :white_check_mark: |
| Aggregating indexes          | :white_check_mark: |
| Join indexes                 | :x: (syntax supported, but not effective) |


## Constraints support

More on constraints in [Platform constraint support](https://docs.getdbt.com/docs/collaborate/govern/model-contracts#platform-constraint-support)


| Constraint type | Support | Platform enforcement |
|-----------------|---------|----------------------|
| not_null        | :white_check_mark: Supported | :white_check_mark: Enforced |
| primary_key     | :x: Not Supported | :x: Not enforced |
| foreign_key     | :x: Not Supported | :x: Not enforced |
| unique          | :white_check_mark: Supported | :x: Not enforced |
| check           | :x: Not supported | :x: Not enforced |


## Using dbt-firebolt

For information on configuring dbt models and external tables for Firebolt, see the [dbt documentation for Firebolt configurations](https://docs.getdbt.com/reference/resource-configs/firebolt-configs).

## Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/dbt-firebolt/tree/main/CONTRIBUTING.MD)

## Changelog

See our [changelog](CHANGELOG.md) or our release history for more information.
