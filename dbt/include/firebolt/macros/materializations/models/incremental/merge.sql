{% macro firebolt__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {#
    {% if unique_key is not none %}

        DELETE FROM {{ target }}
        WHERE ({{ unique_key }}) IN (
            SELECT ({{ unique_key }})
            FROM {{ source }}
        );
    {% endif %}
    #}
    INSERT INTO {{ target }} ({{ dest_cols_csv }})
        SELECT {{ dest_cols_csv }}
        FROM {{ source }}
{%- endmacro %}
