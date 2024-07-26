set -e
dbt debug
dbt clean
dbt deps
dbt compile
dbt run-operation stage_external_sources
cp ../.github/workflows/jaffle_shop/sources_external_tables_id_secret.yml models/staging/sources_external_tables.yml
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
dbt seed
dbt seed --full-refresh
dbt run
dbt source freshness
dbt test
dbt docs generate
