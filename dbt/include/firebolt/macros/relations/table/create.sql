{% macro firebolt__create_table_as(
        temporary,
        relation,
        select_sql,
        language = 'sql'
    ) -%}
      {# Create table using CTAS
         Args:
          temporary (bool): Unused, included so macro signature matches
            that of dbt's default macro
          relation (dbt relation/dict)
          select_sql (string): The SQL query that will be used to generate
            the internal query of the CTAS
          language (string): sql or python models. Firebolt only supports sql.
      #}
    {%- if language == 'python' -%}
        {{ exceptions.raise_compiler_error(
            "Firebolt does not currently support " "Python models."
        ) }}

        {%- elif language not in ['python', 'sql'] -%}
        {{ exceptions.raise_compiler_error(
            "Unexpected language parameter supplied: %s " "Must be either 'sql' or 'python'." % (language)
        ) }}
    {%- endif -%}

    {%- set table_type = config.get(
        'table_type',
        default = 'dimension'
    ) | upper -%}
    {%- set primary_index = config.get('primary_index') -%}
    {%- set incremental_strategy = config.get('incremental_strategy') -%}
    {%- set partitions = config.get('partition_by') %}
    CREATE {{ table_type }} TABLE IF NOT EXISTS {{ relation }}

    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced -%}
        {{ get_assert_columns_equivalent(select_sql) }}
        {{ get_table_columns_and_constraints() }};
    INSERT INTO
        {{ relation }}
        (
            {{ adapter.dispatch(
                'get_column_names',
                'dbt'
            )() }}
        ) {%- set select_sql = get_select_subquery(select_sql) %}
    {% endif %}

    {%- if primary_index %}
        primary INDEX {% if primary_index is iterable and primary_index is not string %}
            {{ primary_index | join(', ') }}
        {%- else -%}
            {{ primary_index }}
        {%- endif -%}
    {%- endif -%}

    {% if partitions %}
        PARTITION BY {% if partitions is iterable and partitions is not string %}
            {{ partitions | join(', ') }}
        {%- else -%}
            {{ partitions }}
        {%- endif -%}
    {%- endif %}

    {%- if not contract_config.enforced %}
        AS (
    {% endif -%}

    {{ select_sql }}

    {% if not contract_config.enforced -%})
    {%- endif -%}
{% endmacro %}
