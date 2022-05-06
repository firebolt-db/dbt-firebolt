{% macro get_incremental_sql(strategy, source, target, unique_key, dest_columns) %}
{# Retrieve appropriate sql for whichever incremental strategy is given.
   unique_key is here only as a placeholder for future use. #}
  {%- if strategy == 'append' -%}
    {# Only insert new records into existing table, relying on user to manage
       merges. #}
    {{ get_append_only_sql(source, target, dest_columns) }}
  {%- if strategy == 'insert-overwrite' -%}
    {# Only insert new records into existing table, relying on user to manage
       merges. #}
    {{ get_insert_overwrite_sql(source, target, dest_columns) }}
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


{% macro get_insert_overwrite_sql(source, target, dest_columns) %}
  {# TODO: do I need to validate? See dbt-spark #}
  {%- set partition_columns = config.get('partition_by') | map(attribute='quoted') | join(', ') -%}
  {% call statement('partition_vals', fetch_result=True) %}

      SELECT partition_columns FROM {{ source }}
  {% endcall %}
  {% set partition_vals_tbl = load_result('partition_vals').table %}
  {% adapter.filter_table(partition_vals_tbl, 'type', 'view') %}
  {{ return(load_result('timestamp').table) }}
  -- SELECT columns FROM source
  -- do stupid agate table to get partition_vals
  --
  {{ drop_partitions_sql(target, partition_vals) }}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

  {{ get_append_only_sql(source, target, dest_columns) }}
{% endmacro %}


{% macro drop_partitions_sql(table, partition_vals) %}
  {# Write SQL code to drop partition each partions in partions.
     Args:
       partition_vals: a list of tuples
  #}
  {% for val in partition_vals %}
    {%- set partition = val | join(', ') %}
    ALTER TABLE {{table}} DROP PARTITION {{ partition }};
  {% endfor %}
{% endmacro %}
