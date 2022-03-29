{# This is for building docs. Right now it's an incomplete description of
the columns (for instance, `is_nullable` is missing) but more could be added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {{ log('\n\n** Information schemas:\n' ~ information_schemas, True) }}
  {{ log('\n\n** Schemas:\n' ~ schemas, True) }}
  {%- call statement('catalog', fetch_result=True) %}
    SELECT tbls.table_schema AS table_database
         , NULL as table_schema
         , 'DIMENSION' AS table_type
         , tbls.table_name
         , cols.column_name
         , cols.data_type AS column_type
    FROM "information_schema"."tables" tbls
        JOIN "information_schema"."columns" cols
            USING(table_name)
    ORDER BY tbls.table_name, cols.column_name
  {%- endcall -%}
  {% call statement('show_tables', fetch_result=True) %}
      SHOW TABLES
  {% endcall %}
  {% set results = load_result('catalog').table %}
  {{ log('\n\n** table view\n' ~ load_result('show_tables'), True) }}
  {{ log('\n\n** raw catalog\n' ~ load_result('catalog'), True) }}
  {{ log('\n\n** Agate catalog\n' ~ results, True) }}
  {{ log('\n\n** rows:\n' ~ adapter.get_rows(results), True)}}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
