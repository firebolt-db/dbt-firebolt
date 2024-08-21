{% macro firebolt__get_rename_view_sql(
        relation,
        new_name
    ) %}
    {{ exceptions.raise_compiler_error("Firebolt does not support view renames") }}
{% endmacro %}
