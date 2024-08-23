{%- macro firebolt__drop_table(relation) -%}
    DROP TABLE IF EXISTS {{ relation }} CASCADE
{%- endmacro -%}
