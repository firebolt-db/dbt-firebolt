{% macro firebolt__get_rename_table_sql(
        relation,
        new_name
    ) %}
    {{ exceptions.raise_compiler_error("Firebolt does not support table renames") }}
{% endmacro %}
