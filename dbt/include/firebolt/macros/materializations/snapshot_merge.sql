{% macro firebolt__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    UPDATE {{ target.render() }} AS DBT_INTERNAL_DEST
    SET dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    FROM {{ source }} AS DBT_INTERNAL_SOURCE
    WHERE DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
    AND DBT_INTERNAL_DEST.dbt_valid_to IS NULL
    AND DBT_INTERNAL_SOURCE.dbt_change_type IN ('update', 'delete');

    INSERT INTO {{ target.render() }} ({{ insert_cols_csv }})
    SELECT {{ insert_cols_csv }}
    FROM {{ source }} AS DBT_INTERNAL_SOURCE
    WHERE DBT_INTERNAL_SOURCE.dbt_scd_id NOT IN (
        SELECT dbt_scd_id FROM {{ target.render() }}
    )
    AND DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';

{% endmacro %}