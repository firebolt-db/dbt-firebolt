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
    {% do exceptions.raise_compiler_error('Model %s has incremental strategy %s '
                                          'specified, but that strategy is not '
                                          'supported.' % (target, strategy)) %}
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
  {%- set partition_vals = config.get('partitions') -%}
  {# Get values of partition columns for each row that will be inserted from
     source. For each of those rows we'll drop the partition in the target. Use
     the partition values provided in the confg block. _If_ no partition values
     are provided, query the DB to find all partition values in the source table
     and drop them. To match format of SQL query results, we convert each sublist
     to a string. #}
  {% if partition_vals %}
    {{ drop_partitions_sql(target, partition_vals, True) }}
  {% else %} {# No partition values were set in config. #}
    {% call statement('get_partition_cols', fetch_result=True) %}

      SELECT
      {% if partition_columns is iterable and partition_columns is not string -%}
        DISTINCT({{ partition_columns | join(', ') }})
      {%- else -%}
        DISTINCT {{ partition_columns }}
      {%- endif %}
      FROM {{ source }}
    {%- endcall -%}
    {%- set partition_vals = load_result('get_partition_cols').table.rows -%}
    {{ drop_partitions_sql(target, partition_vals, False) }}
  {%- endif -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {{ get_append_only_sql(source, target, dest_columns) }}
{% endmacro %}


{% macro drop_partitions_sql(relation, partitions_vals, set_in_config) %}
  {#
  Write SQL code to drop each partition in `partitions_vals`.
  Args:
    relation: a relation whose name will be used for `DROP PARTITION` queries.
    partitions_vals: a list of strings, each of which begins with a '['' and
    ends with a ']', as such: '[val1, val2]', and each of which represents
    a partition to be dropped.
    set_in_config: a boolean used to determine how to treat `partitions_vals`,
    whether as a list of strings or as a list of Agate rows.
  #}
  {%- for partition in partitions_vals -%}
    {%- set partition -%}
      {%- if partition is iterable and partition is not string -%}
        {%- if set_in_config -%}
          {# partition is a list of strings. #}
          {{ partition | string() | strip() | slice(1, -1) }}

        {%- else -%}
          {# partition is a list of Agate rows. #}
          {{ partition | join(', ') }}
        {%- endif -%}
      {%- else -%}
        {{ partition }}
      {%- endif -%}
    {%- endset %}

  ALTER TABLE {{relation}} DROP PARTITION {{ partition.strip() }};
  {%- endfor -%}
{% endmacro %}
