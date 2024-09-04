set -xe
dbt debug
dbt clean
dbt deps
dbt compile
dbt run-operation stage_external_sources
cp ../dbt-firebolt/.github/workflows/jaffle_shop/sources_external_tables_id_secret.yml models/staging/sources_external_tables.yml
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
if [[ -n "$AWS_ACCESS_ROLE_ARN" ]]; then
    # Can't test this on FB 1.0
    cp ../dbt-firebolt/.github/workflows/jaffle_shop/sources_external_tables_iam.yml models/staging/sources_external_tables.yml
    dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
    # Test COPY INTO
    cp ../dbt-firebolt/.github/workflows/jaffle_shop/sources_external_tables_copy.yml models/staging/sources_external_tables.yml
    dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
fi
dbt seed
dbt seed --full-refresh
dbt run
dbt source freshness
dbt test
# Trigger incremental check
dbt run
dbt docs generate
