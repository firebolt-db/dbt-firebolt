{# This is for building docs. Right now it's an incomplete description of
the columns (for instance, `is_nullable` is missing) but more could be added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {%- call statement('catalog', fetch_result=True) %}
     SELECT tbls.table_catalog AS table_database
          , tbls.table_schema as table_schema
          , table_type
          , tbls.table_name as table_name
          , cols.column_name as column_name
          , cols.data_type AS column_type
          , UPPER(tbls.table_type) as relation_type
          , cols.ordinal_position as column_index
     FROM  information_schema.tables tbls
          JOIN information_schema.columns cols
               USING(table_name)

     UNION

     SELECT views.table_catalog AS table_database
          , views.table_schema as table_schema
          , 'VIEW' AS table_type
          , views.table_name as table_name
          , cols.column_name AS column_name
          , cols.data_type AS column_type
          , 'VIEW' as relation_type
          , cols.ordinal_position as column_index
     FROM information_schema.views views
          JOIN information_schema.columns cols
               USING(table_name)
  {% endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
