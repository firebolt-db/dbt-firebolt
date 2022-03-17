{# This is for building docs. Right now it's an incomplete description of
the columns (for instance, `is_nullable` is missing) but more could be added later. #}

{% macro firebolt__get_catalog(information_schemas, schemas) -%}
  {%- call statement('catalog', fetch_result=True) %}
    SELECT tbls.table_schema AS table_database
         , '{{ target.schema }}' AS table_schema
         , tbls.table_type
         , tbls.table_name
         , cols.column_name
         , cols.data_type AS column_type
    FROM information_schema.tables tbls
         JOIN information_schema.columns cols
         USING(table_name)
    WHERE
        {% for schema in schemas -%}
          tbls.table_name LIKE '{{ schema }}_%' {%- if not loop.last %} or {% endif -%}
        {%- endfor %}
    ORDER BY tbls.table_name, cols.column_name
  {%- endcall -%}
  {{ log('\n\n** get_catalog:\n', True) }}
  {{ log(load_result('catalog'), True) }}
  {% set results = load_result('catalog').table %}
  {{ return(results) }}
{%- endmacro %}


