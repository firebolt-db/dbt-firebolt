{% macro firebolt__drop_schema(schema_relation) -%}
  {# Drop tables and views for schema_relation.
     Args:
         schema_relation (dict): Contains values for database and schema.
     Until schemas are supported this macro will drop all tables and views.
  #}
  {% set all_relations = (list_relations_without_caching(schema_relation)) %}

  {% set views = adapter.filter_table(all_relations, 'type', 'view') %}
  {% set tables = adapter.filter_table(all_relations, 'type', 'table') %}
  {% do drop_relations_loop(views) %}
  {% do drop_relations_loop(tables) %}
{% endmacro %}


{% macro drop_relations_loop(relations) %}
  {% for row in relations %}
    {%- set relation = api.Relation.create(database=target.database,
                                           schema=target.schema,
                                           identifier=row[1],
                                           type=row[3]) -%}
    {{ adapter.drop_relation(relation) }}
  {%- endfor %}
{% endmacro %}


{% macro firebolt__list_schemas(database) %}
  {# Return current schema. Name is a misnomer.
     TODO: Should this actually return all schemas? #}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}

      SELECT 'public' AS schema
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro firebolt__create_schema(relation) -%}
  {# stub. Not yet supported in Firebolt. #}
  {%- call statement('create_schema') %}

      SELECT 'create_schema'
  {% endcall %}
{% endmacro %}


{% macro firebolt__alter_column_type(relation, column_name, new_column_type) -%}
  {# Stub: alter statements not currently available in Firebolt. #}
  {% call statement('alter_column_type') %}

      SELECT 'alter_column_type'
  {% endcall %}
{% endmacro %}


{% macro firebolt__check_schema_exists(information_schema, schema) -%}
  {# Stub. Will be replaced by query later. #}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=True) %}

      SELECT 'schema_exists'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro make_create_index_sql(relation,
                               index_name,
                               create_statement,
                               spine_col,
                               other_col) -%}
  {# Create and return SQL for generating a join or aggregating index.
     Args:
       relation (dict):
       index_name (str): name of the index
       create_statement (str): either "CREATE JOIN INDEX" or
                               "CREATE AND GENERATE AGGREGATING INDEX"
       spine_col ([str]):
         if agg index, key columns
         if join index, join column
       other_col ([str]):
         if agg index, aggregating columns
         if join index, dimension column
       #}
  {{ create_statement }} "{{ index_name }}" ON {{ relation }} (
      {% if spine_col is iterable and spine_col is not string -%}
          {{ spine_col | join(', ') }},
      {% else -%}
          {{ spine_col }},
      {% endif -%}
      {% if other_col is iterable and other_col is not string -%}
          {{ other_col | join(', ') }}
      {%- else -%}
          {{ other_col }}
      {%- endif -%}
  );
{% endmacro %}


{% macro drop_index(index_name, index_type) -%}
  {# Drop aggregating or join index. #}
  {% call statement('drop_index', auto_begin=False) %}

      DROP {{ index_type | upper }} INDEX "{{ index_name }}"
  {% endcall %}
{% endmacro %}


{% macro firebolt__get_create_index_sql(relation, index_dict) -%}
  {# Return aggregating or join index SQL string. #}
  {# Parse index inputs and send parsed input to make_create_index_sql #}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set index_name = index_config.render_name(relation) -%}
  {%- set index_type = index_config.index_type | upper -%}
  {%- if index_type == "JOIN" -%}
      {{ make_create_index_sql(relation,
                               index_name,
                               "CREATE JOIN INDEX",
                               index_config.join_columns,
                               index_config.dimension_column) }}
  {%- elif index_type == "AGGREGATING" -%}
      {{ make_create_index_sql(relation,
                               index_name,
                               "CREATE AND GENERATE AGGREGATING INDEX",
                               index_config.key_columns,
                               index_config.aggregation) }}
  {%- endif -%}
{%- endmacro %}


{% macro sql_convert_columns_in_relation_firebolt(rows) -%}
  {% set columns = [] %}
  {% for row in rows %}
    {% do columns.append(api.Column(*row)) %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}


{% macro firebolt__get_columns_in_relation(relation) -%}
  {#-
  Return column information for table identified by relation as
  List[FireboltColumn].
    Args: relation: dbt Relation
  -#}
  {% set sql %}
    SELECT column_name, data_type from information_schema.columns
    WHERE table_name = '{{ relation.identifier }}'
  {% endset %}
  {%- set result = run_query(sql) -%}
  {% set columns = [] %}
  {% for row in result %}
    {% do columns.append(adapter.get_column_class().from_description(row['column_name'],
                         adapter.resolve_special_columns(row['data_type']))) %}
  {% endfor %}
  {% do return(columns) %}
{% endmacro %}


{% macro firebolt__list_relations_without_caching(relation) %}
  {# Return all views and tables as agate table.
     Args:
       relation (dict): Contains values for database and schema.

     dbt has a relations cache. Using this macro will list all
     the relations in the current schema using a direct DB query,
     rather than checking the cache. So the name is a misnomer. Should
     be list_relations_bypassing_cache or something.
  #}
  {% call statement('list_tables_without_caching', fetch_result=True) %}

      SELECT
          table_catalog AS "database",
          table_name AS "name",
          '{{ relation.schema }}' AS "schema",
          CASE
            WHEN table_type = 'VIEW' THEN 'view'
            ELSE 'table'
          END AS "type"
      FROM
          information_schema.tables
  {% endcall %}
  {% set info_table = load_result('list_tables_without_caching').table %}
  {{ return(info_table) }}
{% endmacro %}

{% macro firebolt__truncate_relation(relation) -%}
  {#
  Truncate relation. Actual macro is drop_relation in ./adapters/relation.sql.
  #}

  {# Firebolt doesn't currently support TRUNCATE, so DROP CASCADE.
     This should only be called from reset_csv_table, where it's followed by
     `create_csv_table`, so not recreating the table here. To retrieve old code,
     see commit f9984f6d61b8a1b877bc107b102eeb30eba54f35
     This will be replaced by `CREATE OR REPLACE`. #}
  {{ adapter.drop_relation(relation) }}
{% endmacro %}
