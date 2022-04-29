{% macro get_incremental_sql(strategy, source, target, unique_key, dest_columns) %}
{# Retrieve appropriate sql for whichever incremental strategy is given.
   unique_key is here only as a placeholder for future use. #}
  {%- if strategy == 'append' -%}
    {# Only insert new records into existing table, relying on user to manage
       merges. #}
    {{ get_append_only_sql(source, target, dest_columns) }}
  {%- elif strategy is not none -%}
    {% do exceptions.raise_compiler_error('Incremental strategy ' ~ strategy ~ ' is not supported.') %}
  {%- endif -%}
{% endmacro %}


{% macro get_append_only_sql(source, target, dest_columns) -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) %}

  INSERT INTO {{ target }} ({{ dest_cols_csv }})
      SELECT {{ dest_cols_csv }}
      FROM {{ source }}
{%- endmacro %}
