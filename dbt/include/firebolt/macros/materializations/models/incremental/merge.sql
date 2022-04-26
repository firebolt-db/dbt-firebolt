{% macro firebolt__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {% if unique_key is not none %}

    DELETE FROM {{ target }}
    WHERE ({{ unique_key }}) IN (
        SELECT ({{ unique_key }})
        FROM {{ source }}
    );
    {% endif %}

    INSERT INTO {{ target }} ({{ dest_cols_csv }})
        SELECT {{ dest_cols_csv }}
        FROM {{ source }}
{% endmacro %}


{#
{% macro firebolt__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none and include_sql_header }}

    MERGE INTO {{ target }} AS DBT_INTERNAL_DEST
        USING {{ source }} AS DBT_INTERNAL_SOURCE
        ON FALSE

    WHEN NOT matched by source
        {% if predicates %} AND {{ predicates | join(' and ') }} {% endif %}
        then delete

    WHEN NOT matched THEN INSERT
        ({{ dest_cols_csv }})
    VALUES
        ({{ dest_cols_csv }})
{% endmacro %}
#}
