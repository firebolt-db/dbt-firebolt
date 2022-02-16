{# This is for building docs. Right now it's an incomplete description of
the columns (for instance, `is_nullable` is missing) but more could be added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {{ log('\n\n**information_schemas:\n' ~ information_schemas, True) }}
  {{ log('\n\n**schemas:\n' ~ schemas, True) }}
  {%- call statement('catalog', fetch_result=True) -%}
    SELECT tbls.table_schema AS table_database
         , 'NULL' as table_schema
         , tbls.table_name
         , cols.column_name
         , cols.data_type AS column_type
         , 'NULL' AS column_comment
    FROM "information_schema"."tables" tbls
        JOIN "information_schema"."columns" cols
            USING(table_name)
    ORDER BY tbls.table_name, cols.column_name

{#    WITH tbls AS (
        SELECT table_schema AS table_database
             , NULL AS table_schema
             , table_name
        FROM "information_schema"."tables"
    ),
    cols AS (
        SELECT table_name
             , column_name
             , data_type AS column_type
             , NULL AS column_comment
        FROM "information_schema"."columns"
    )
    SELECT tbls.table_database
         , tbls.table_schema
         , tbls.table_name
         , cols.column_name
         , cols.column_type
    FROM tbls
    JOIN cols ON tbls.table_name = cols.table_name
    ORDER BY tbls.table_name, cols.column_name
#}
  {%- endcall -%}
  {% call statement('show_tables', fetch_result=True) %}
      SHOW TABLES
  {% endcall %}
  {{ log('\n\n** table view\n' ~ load_result('show_tables'), True) }}
  {{ log('\n\n** raw catalog\n' ~ load_result('catalog'), True) }}
  {{ log('\n\n** Agate catalog\n' ~ load_result('catalog').table, True) }}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}


