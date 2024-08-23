{# This is for building docs. Right now it's an incomplete description of
the columns (for instance, `is_nullable` is missing) but more could be added later. #}

{% macro firebolt__get_catalog(information_schema, schemas) -%}

    {% set query %}
        with tables as (
            {{ firebolt__get_catalog_tables_sql(information_schema) }}
            {{ firebolt__get_catalog_schemas_where_clause_sql(schemas) }}
        ),
        columns as (
            {{ firebolt__get_catalog_columns_sql(information_schema) }}
            {{ firebolt__get_catalog_schemas_where_clause_sql(schemas) }}
        )
        {{ firebolt__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}

{% macro firebolt__get_catalog_relations(information_schema, relations) -%}

    {% set query %}
        with tables as (
            {{ firebolt__get_catalog_tables_sql(information_schema) }}
            {{ firebolt__get_catalog_relations_where_clause_sql(relations) }}
        ),
        columns as (
            {{ firebolt__get_catalog_columns_sql(information_schema) }}
            {{ firebolt__get_catalog_relations_where_clause_sql(relations) }}
        )
        {{ firebolt__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}


{% macro firebolt__get_catalog_tables_sql(information_schema) -%}
    SELECT
      tbls.table_catalog AS table_database,
      tbls.table_schema as table_schema,
      table_type,
      tbls.table_name as table_name,
      CASE
        WHEN table_type = 'VIEW' THEN 'VIEW'
        ELSE 'TABLE'
      END AS relation_type
    FROM
      information_schema.tables tbls
{%- endmacro %}


{% macro firebolt__get_catalog_columns_sql(information_schema) -%}
    select
        table_catalog as "table_database",
        table_schema as "table_schema",
        table_name as "table_name",
        column_name as "column_name",
        ordinal_position as "column_index",
        data_type as "column_type"
    from information_schema.columns
{%- endmacro %}

{% macro firebolt__get_catalog_results_sql() -%}
    SELECT *
    FROM tables
    JOIN columns USING ("table_database", "table_schema", "table_name")
    ORDER BY "column_index"
{%- endmacro %}


{% macro firebolt__get_catalog_schemas_where_clause_sql(schemas) -%}
    WHERE ({%- for schema in schemas -%}
        UPPER("table_schema") = UPPER('{{ schema }}'){%- if not loop.last %} OR {% endif -%}
    {%- endfor -%})
{%- endmacro %}


{% macro firebolt__get_catalog_relations_where_clause_sql(relations) -%}
    WHERE (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    UPPER("table_schema") = UPPER('{{ relation.schema }}')
                    AND UPPER("table_name") = UPPER('{{ relation.identifier }}')
                )
            {% elif relation.schema %}
                (
                    UPPER("table_schema") = UPPER('{{ relation.schema }}')
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} OR {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}
