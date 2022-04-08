{% macro get_incremental_sql(strategy, source, target, unique_key, dest_columns) %}
  {%- if strategy == 'append' -%}
    {# Only insert new records into existing table, relying on user to manage
       merges. #}
    {{ get_insert_into_sql(source, target, unique_key, dest_columns) }}
  {%- elif strategy is not none -%}
    {% do exceptions.raise_compiler_error('Incremental strategy {{ strategy }} is not supported.') %}
  {%- else -%}
    {% do exceptions.raise_compiler_error('No incremental strategy was provided.') %}
  {%- endif -%}
{% endmacro %}


{% macro get_insert_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    INSERT INTO {{ target }} ({{ dest_cols_csv }})
        SELECT {{ dest_cols_csv }}
        FROM {{ source }}
{%- endmacro %}
