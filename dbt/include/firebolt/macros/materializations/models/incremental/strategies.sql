{% macro get_incremental_sql(strategy, source, target, unique_key, dest_columns, incremental_predicates) %}
  {#
  Retrieve appropriate SQL for whichever incremental strategy is given.
  Args:
    strategy: string, which incremental strategy is in to be used.
    source: string, table from which queried results will be taken.
    target: string, table into which results will be inserted.
    unique_key: string, only as a placeholder for future use
    dest_columns: List[string] of the names of the columns which data will be
      inserted into.
  #}
  {%- if strategy == 'append' -%}
    {#- Only insert new records into existing table, relying on user to manage
       merges. -#}
    {{ get_insert_only_sql(source, target, dest_columns) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#- Insert new data. If any data is duplicate, drop partition containing
       previous data and insert new. -#}
    {{ get_insert_overwrite_sql(source, target, dest_columns) }}
  {%- elif strategy == 'delete+insert' -%}
    {% do return(get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates)) %}
  {%- elif strategy is not none -%}
    {% do exceptions.raise_compiler_error('Model %s has incremental strategy %s '
                                          'specified, but that strategy is not '
                                          'supported.' % (target, strategy)) %}
  {% else %}
    {{ exceptions.raise_compiler_error('No incremental strategy was specified '
                                       'for model %s.' % (target)) }}
  {%- endif -%}
{% endmacro %}


{% macro get_insert_only_sql(source, target, dest_columns) -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) %}

  INSERT INTO {{ target }} ({{ dest_cols_csv }})
  SELECT {{ dest_cols_csv }}
  FROM {{ source }}
{%- endmacro %}


{% macro get_insert_overwrite_sql(source, target, dest_columns) %}
  {#
  Compile SQL to drop correct partitions in target and insert from source.
  Args:
    source: Relation from which queried results will be taken.
    target: Relation into which results will be inserted.
    dest_columns: list of the names of the columns which data will be
      inserted into.
  #}
  {%- set partition_cols = config.get('partition_by') -%}
  {%- set partition_vals = config.get('partitions') -%}
  {#- Get values of partition columns for each row that will be inserted from
     source. For each of those rows we'll drop the partition in the target. Use
     the partition values provided in the confg block. _If_ no partition values
     are provided, query the DB to find all partition values in the source table
     and drop them. To match format of SQL query results, we convert each sublist
     to a string. -#}
  {%- if partition_vals -%}
    {# Partition vals are set in config. #}
    {{ drop_partitions_sql(target, partition_vals, True) }}
  {%- else -%} {# No partition values were set in config. #}
    {%- call statement('get_partition_cols', fetch_result=True) %}

      SELECT
      {% if partition_cols is iterable and partition_cols is not string -%}
        DISTINCT {{ partition_cols | join(', ') }}
      {%- else -%}
        DISTINCT {{ partition_cols }}
      {%- endif %}
      FROM {{ source }}
    {%- endcall -%}
    {%- set partition_vals = load_result('get_partition_cols').table.rows -%}
    {{ drop_partitions_sql(target, partition_vals, partition_cols, False) }}
  {%- endif -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {{ get_insert_only_sql(source, target, dest_columns) }}
{% endmacro %}


{% macro drop_partitions_sql(relation,
                             partition_vals,
                             part_col_names,
                             vals_set_in_config) %}
  {#
  Write SQL code to drop each partition in `partition_vals`.
  Args:
    relation: a relation whose name will be used for `DROP PARTITION` queries.
    partition_vals: a list of strings, each of which begins with a '['' and
      ends with a ']', as such: '[val1, val2]', and each of which represents
      a partition to be dropped.
    part_col_names: a list of string, the names of the partition columns.
    vals_set_in_config: a boolean used to determine how to treat `partition_vals`,
      whether as a list of strings or as a list of Agate rows.
  #}
  {%- for vals in partition_vals -%}
    {%- set vals -%}
      {%- if vals is iterable and vals is not string -%}
        {%- if vals_set_in_config -%}
          {# `vals` is a list of strings. #}
          {{ vals | string() | trim() | slice(1, -1) }}
        {%- else -%}
          {# `vals` is a list of Agate rows. #}
          {%- if 1 == (vals | length) -%}
            {#- There's a weird behavior where, if dbt
                queries for only a single (text?) field `join()` removes the
                qoutes on the resulting string. So I have to do a little bit
                of extra work. -#}
            '{{ vals | join(', ') }}'
          {%- else -%}
            '{{ vals | join("', '") }}'
          {%- endif -%}
        {%- endif -%}
      {%- else -%}
        {{ vals }}
      {%- endif -%}
    {%- endset -%}
  {%- set vals = vals.strip() -%}
  {%- if vals.startswith("'[") -%}
    {#- If a single row is returned, but has  multiple values. -#}
    {%- set vals = vals[2:-2] -%}
  {%- endif -%}
  {#- At this point, vals is a simple string of values separated by commas. -#}
  {%- set col_types = adapter.get_columns_in_relation(relation) -%}
  {%- set vals = adapter.annotate_date_columns_for_partitions(vals,
                                                              part_col_names,
                                                              col_types) %}
  ALTER TABLE {{relation}} DROP PARTITION {{ vals.strip() }};
  {%- endfor -%}
{% endmacro %}
