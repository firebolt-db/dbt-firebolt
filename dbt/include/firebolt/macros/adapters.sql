{% macro firebolt__drop_schema(schema_relation) -%}
  {# until schemas are supported #}
  {# this macro will drop all tables and views #}
  {% set relations = (list_relations_without_caching(schema_relation)) %}

  {% set vews = adapter.filter_table(relations, 'type', 'view') %}
  {% set tbls = adapter.filter_table(relations, 'type', 'table') %}
  {% do drop_relations_loop(vews) %}
  {% do drop_relations_loop(tbls) %}
{% endmacro %}

{% macro drop_relations_loop(relations) %}
  {% for row in relations %}
    {%- set relation = api.Relation.create(database=target.database,
                                           schema=target.schema,
                                           identifier=row[1],
                                           type=row[3]
                                           ) -%}
    {% do drop_relation(relation) %}
  {%- endfor %}
{% endmacro %}


{% macro firebolt__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
        SELECT '{{target.schema}}' AS schema
    {% endcall %}
    {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro firebolt__create_schema(relation) -%}
{# stub. Not yet supported in Firebolt. #}
    {%- call statement('create_schema') -%}
        SELECT 'create_schema'
    {% endcall %}
{% endmacro %}


{% macro firebolt__current_timestamp() -%}
    NOW()
{%- endmacro %}


--TODO
---add column to table
--  ALTER TABLE {{ relation }}
--  ADD COLUMN modify column {{ adapter.quote(column_name) }}
---    {{ new_column_type }} {{ on_cluster_clause(label="on cluster") }}
{% macro firebolt__alter_column_type(relation, column_name, new_column_type) -%}
    {# stub #}
    {% call statement('alter_column_type') %}
        SELECT 'alter_column_type'
    {% endcall %}
{% endmacro %}


{% macro firebolt__check_schema_exists(information_schema, schema) -%}
    {# stub. Not yet supported in Firebolt. #}
    {% call statement('check_schema_exists', fetch_result=True, auto_begin=True) %}
        SELECT 'schema_exists'
    {% endcall %}
    {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro firebolt__get_create_index_sql(relation, index_dict) -%}
    {%- set index_config = adapter.parse_index(index_dict) -%}
    {%- set index_name = index_config.render(relation) -%}
    {%- set index_type = index_config.type | upper -%}

    {%- if index_type == "JOIN" -%}
        {{ make_create_index_sql(
          relation, index_name, "CREATE JOIN INDEX",
          index_config.join_column, index_config.dimension_column) }}
    {%- elif index_type == "AGGREGATING" -%}
        {{ make_create_index_sql(
          relation, index_name, "CREATE AND GENERATE AGGREGATING INDEX",
          index_config.key_column, index_config.aggregation) }}
    {%- endif -%}
{%- endmacro %}


{% macro make_create_index_sql(relation,
                               index_name,
                               create_statement,
                               spine_col,
                               other_col) -%}
  {{ create_statement }} "{{ index_name }}" ON {{ relation }} (
      {{ spine_col }},
      {% if other_col is iterable and other_col is not string -%}
          {{ other_col | join(', ') }}
      {%- else -%}
          {{ other_col }}
      {%- endif -%}
      );
{%- endmacro %}

{% macro drop_index(index_name, index_type) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        DROP {{ index_type | upper }} INDEX "{{ index_name }}"
    {%- endcall %}
{% endmacro %}

{% macro firebolt__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set index_name = index_config.render(relation) -%}
  {%- set index_type = index_config.type | upper -%}

  {%- if index_type == "JOIN" -%}
    {{ make_create_index_sql(
      relation, index_name, "CREATE JOIN INDEX",
      index_config.join_column, index_config.dimension_column) }}
  {%- elif index_type == "AGGREGATING" -%}
    {{ make_create_index_sql(
      relation, index_name, "CREATE AND GENERATE AGGREGATING INDEX", 
      index_config.key_column, index_config.aggregation) }}
  {%- endif -%}

{%- endmacro %}

{% macro make_create_index_sql(
  relation, index_name, create_statement, spine_col, other_col) -%}

  {{ create_statement }} "{{ index_name }}" ON {{ relation }} (
    {{ spine_col }},
    {% if other_col is iterable and other_col is not string -%}
      {{ other_col | join(', ') }}
    {%- else -%}
      {{ other_col }}
    {%- endif -%}
    );
{%- endmacro %}

{% macro drop_index(index_name, index_type) -%}

  {% call statement('drop_relation', auto_begin=False) -%}
    DROP {{ index_type | upper }} INDEX "{{ index_name }}"
  {%- endcall %}
{% endmacro %}


{% macro firebolt__drop_relation(relation) -%}
  {# drop non-primary indexes #}
  {% if relation.type == 'table' %}
    {% set idx_info_table = run_query('SHOW INDEXES;') %}
    {% set idxs_tbl = adapter.filter_table(idx_info_table, 'table_name', relation.identifier) %}
    {% set idxs_to_drop = adapter.filter_table(idxs_tbl, 'type', '^((?!primary).)*$') %}

    {% for row in idxs_to_drop %}
      {% do drop_index(row[0], row[2]) %}
    {% endfor %}
  {% endif %}

  {% call statement('drop_relation', auto_begin=False) -%}
    DROP {{ relation.type }} IF EXISTS {{ relation.identifier }}
  {%- endcall %}
{% endmacro %}


{% macro firebolt__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        SELECT column_name,
               data_type,
               character_maximum_length,
               numeric_precision_radix
          FROM information_schema.columns
         WHERE table_name = '{{ relation.identifier }}'
        ORDER BY column_name
    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro firebolt__list_relations_without_caching(schema_relation) %}
    {# TODO: schema_relation is ??
             What does "without caching" mean in this context? #}
    {% call statement('list_tables_without_caching', fetch_result=True) -%}
        SELECT '{{ schema_relation.database }}' AS "database",
               table_name AS "name",
               '{{ schema_relation.schema }}' AS "schema",
               'table' AS type
          FROM information_schema.tables
    {% endcall %}
    {% set table_info_table = load_result('list_tables_without_caching').table %}
    {% call statement('list_views_without_caching', fetch_result=True) -%}
        SHOW VIEWS
    {% endcall %}
    {% set view_info_table = load_result('list_views_without_caching').table %}
    {% set view_info_table_tweaked = adapter.reformat_view_results(
                                         view_info_table,
                                         schema_relation) %}
    {% if (table_info_table.rows | length) > 0
       or (view_info_table_tweaked.rows | length) > 0 %}
          {{ return(adapter.stack_tables([table_info_table, view_info_table_tweaked])) }}
    {% else %}
        {{ return(table_info_table) }}
    {% endif %}
{% endmacro %}


{% macro firebolt__create_table_as(temporary, relation, sql) -%}
    {# Create table using CTAS #}
    {%- set table_type = config.get('table_type', default='dimension') | upper -%}
    {%- set primary_index = config.get('primary_index') -%}

    CREATE {{ table_type }} TABLE IF NOT EXISTS {{ relation }}
    {%- if primary_index %}
        PRIMARY INDEX
        {% if primary_index is iterable and primary_index is not string -%}
            {{ primary_index | join(', ') }}
        {%- else -%}
            {{ primary_index }}
        {%- endif -%}
    {% endif %}
    AS (
        {{ sql }}
    )
{%- endmacro %}


{% macro firebolt__create_view_as(relation, sql) -%}
    CREATE VIEW IF NOT EXISTS {{ relation.identifier }} AS (
        {{ sql }}
    )
{% endmacro %}


{% macro firebolt__truncate_relation(relation) -%}
    {% call statement('truncate_relation', fetch_result=True) -%}
        SELECT COUNT(*) AS count
          FROM information_schema.tables
         WHERE "table_name" = '{{ relation }}';
    {% endcall %}
    {% set num_rows = load_result('truncate_relation').table %}
    {% if num_rows == '1' %}
        {% call statement('truncate_relation') %}
            TRUNCATE {{ relation.identifier }}
        {%- endcall %}
    {% endif %}
{% endmacro %}
