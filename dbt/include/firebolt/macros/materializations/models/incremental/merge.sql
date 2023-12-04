{% macro firebolt__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            where
                ({{ get_quoted_csv(unique_key) }}) in (
                    select {{ get_quoted_csv(unique_key) }}
                    from {{ source }}
                )
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %};
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    select {{ dest_cols_csv }}
    from {{ source }}

{%- endmacro %}
