{# This is for building docs. Right now it's an incomplete description of
the columns (for instance, `is_nullable` is missing) but more could be added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {%- call statement('catalog', fetch_result=True) %}

    SELECT tbls.table_schema AS table_database
         , NULL as table_schema
         , table_type
         , tbls.table_name
         , cols.column_name
         , cols.data_type AS column_type
         , 'table' AS relation_type
    FROM information_schema.tables tbls
         JOIN information_schema.columns cols
            USING(table_name)

    UNION

    SELECT views.table_schema AS table_database
         , NULL as table_schema
         , NULL AS table_type
         , views.table_name
         , NULL AS column_name
         , NULL AS column_type
         , 'view' AS relation_type
    FROM information_schema.views views
  {% endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
