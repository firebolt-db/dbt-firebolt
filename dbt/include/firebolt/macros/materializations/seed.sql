{% macro firebolt__get_binding_char() %}
  {# Override the wildcard character in prepared SQL statements. #}
  {{ return('?') }}
{% endmacro %}


{% macro firebolt__create_csv_table(model, agate_table) %}
    {%- set column_override = model['config'].get('column_types', {}) -%}
    {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
    {% set sql %}

      CREATE DIMENSION TABLE IF NOT EXISTS {{ this.render() }} (
          {%- for col_name in agate_table.column_names -%}
              {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
              {%- set type = column_override.get(col_name, inferred_type) -%}
              {%- set column_name = (col_name | string) -%}
              {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }}
              {%- if not loop.last -%}, {%- endif -%}
          {%- endfor -%}
      )
    {% endset %}
    {% call statement('_') %}
        {{ sql }}
    {%- endcall %}
    {{ return(sql) }}
{% endmacro %}


{# TODO: Make sure this is going to run correctly, or delete it. #}
{% macro firebolt__reset_csv_table(model,
                                   full_refresh,
                                   old_relation,
                                   agate_table) %}
    {% set sql = "" %}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
    {% else %}
        {{ adapter.truncate_relation(old_relation) }}
    {% endif %}
    {% set sql = create_csv_table(model, agate_table) %}
    {{ return(sql) }}
{% endmacro %}
