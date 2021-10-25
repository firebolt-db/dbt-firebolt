{# This is for building docs. Right now it's an incomplete description of
the columns—for instance, `is_nullable` is missing—but more could be added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    WITH tbls AS (
        SELECT table_schema AS table_database
             , NULL AS table_schema
             , table_name AS table_name
        FROM "information_schema"."tables"
    ),
    cols AS (
        SELECT table_schema AS table_database
             , table_name
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
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
