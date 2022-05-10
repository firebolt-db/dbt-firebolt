{% macro get_incremental_sql(strategy, source, target, unique_key, dest_columns) %}
{# Retrieve appropriate sql for whichever incremental strategy is given.
   unique_key is here only as a placeholder for future use. #}
  {%- if strategy == 'append' -%}
    {# Only insert new records into existing table, relying on user to manage
       merges. #}
    {{ get_append_only_sql(source, target, dest_columns) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {# Only insert new records into existing table, relying on user to manage
       merges. #}
    {{ get_insert_overwrite_sql(source, target, dest_columns) }}
  {%- elif strategy is not none -%}
    {% do exceptions.raise_compiler_error('Incremental strategy %s is not supported '
                                          'on model %s.' % (strategy, target)) %}
  {% else %}
    {{ exceptions.raise_compiler_error('No incremental strategy was specified '
                                       'for model %s.' % (target)) }}
  {%- endif -%}
{% endmacro %}


{% macro get_append_only_sql(source, target, dest_columns) -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) %}

  INSERT INTO {{ target }} ({{ dest_cols_csv }})
      SELECT {{ dest_cols_csv }}
      FROM {{ source }}
{%- endmacro %}


{% macro get_insert_overwrite_sql(source, target, dest_columns) %}
  {# Compile SQL to drop correct partitions in target and insert from source. #}
  {%- set partition_columns = config.get('partition_by') -%}
  {% if not partition_columns -%}
    {{ exceptions.raise_compiler_error('No partition was specified for model %s source: %s.' %
                                       (target, source)) }}
  {% endif %}
  {# Get values of partition columns for each row that will be inserted from 
     source. For each of those rows drop the partition in the target. #}
  {% call statement('get_partition_cols', fetch_result=True) %}

    SELECT 
    {% if partition_columns is iterable and partition_columns is not string -%}
        {{ partition_columns | join(', ') }}
    {%- else -%}
        {{ partition_columns }}
    {%- endif %}
    FROM {{ source }}
  {% endcall %}
  {%- set partition_vals = load_result('get_partition_cols').table.rows -%}
  {%- if partition_vals -%}
    {{ log('\n\n** partition values: ' ~ partition_vals, True) }}
    {{ drop_partitions_sql(target, partition_vals) }}
  {%- endif -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {{ get_append_only_sql(source, target, dest_columns) }}
{% endmacro %}


{% macro drop_partitions_sql(table, partition_vals) %}
  {# 
  Write SQL code to drop partition each partions in partions.
  Args:
    partition_vals: a list of tuples
  #}
  {%- for val in partition_vals -%}
    {%- set partition = val | join(', ') -%}
    ALTER TABLE {{table}} DROP PARTITION '{{ partition }}';
  {% endfor %}
{% endmacro %}
