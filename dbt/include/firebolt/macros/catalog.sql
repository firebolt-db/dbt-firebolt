{# This is for building docs. Right now it's an incomplete description of
   the columns (for instance, `is_nullable` is missing) but more could be 
   added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {%- call statement('just_tables', fetch_result=True) %}
  -- {{ log('\n\n** Information schema:\n' ~ information_schemas, True) }}
  -- {{ log('\n\n** Schemas:\n' ~ schemas, True) }}

    SELECT tbls.table_schema AS table_database
        , '{{ schema }}' AS table_schema
        , tbls.table_type
        , tbls.table_name
        , cols.column_name
        , cols.data_type AS column_type
        , cols.ordinal_position AS column_index
        , 'TABLE' AS relation_type
    FROM information_schema.tables tbls
         JOIN information_schema.columns cols
            USING(table_name)
  {% endcall -%}
  {%- for row in load_result('just_tables').table -%}
    {{ log('\n\n** original table row values : ' ~  row.values(), True) }}
  {% endfor %}
  {#- `all_cols` is a list bc we'll append the views below. -#}
  {%- set all_cols = [load_result('just_tables').table] -%} 
  {%- set tables_for_log_output = load_result('just_tables').table.columns.dict()['table_name'].values() -%}
  {{ log('\n\n** all tables\n' ~ tables_for_log_output, True) }}
  {#- Now get all views. information_schema doesn't hold columns for views,
      so have to do this using get_columns_in_relation. -#}
  {%- set temp_rel = get_or_create_relation(database=database,
                                            schema=schema,
                                            identifier='tmp_rel',
                                            type='view') -%}
  {%- set all_relations = list_relations_without_caching(temp_rel) -%}
  {{ log('\n\n** all relations: \n' ~ all_relations, True) }}
  {%- set views = adapter.filter_table(all_relations, 'type', 'view') -%}
  {{ log('\n\n** all views: \n' ~ views, True) }}
  {{ log('\n\n** all view rows: \n' ~ views.rows, True) }}
  {%- set view_names = views.columns.dict()['name'].values() -%}
  {{ log('\n\n** view names: \n' ~ view_names, True) }}
  {%- for name in view_names -%}
    {{ log('\n** name: \n' ~ name, True) }}
    {%- set cur_view_cols = adapter.get_columns_in_relation(name) -%}
    {%- set tmp_view = adapter.create_relation_agate_table(
                           database_name=database,
                           schema=schema,
                           table_type='None',
                           table_name=name,
                           relation_type='VIEW',
                           columns=cur_view_cols) -%}
    {%- do all_cols.append(tmp_view) -%}
  {%- endfor -%}
  {{ log('\n\n** all columns', True) }}
  {%- for tbl in all_cols -%}
    {%- for row in tbl -%}
      {{ log('\n\n** row: ' ~  row.values(), True) }}
    {% endfor %}
  {%- endfor -%}
  {{ log('\n\n** Raw catalog:\n' ~ all_cols, True) }}
  {%- set final_table = adapter.stack_tables(all_cols) -%}
  {%- for row in final_table -%}
    {{ log('\n\n** row values : ' ~  row.values(), True) }}
  {% endfor %}
  {{ return(final_table) }}
{%- endmacro %}
