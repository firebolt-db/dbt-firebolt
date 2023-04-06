{% macro firebolt__get_show_grant_sql(relation) %}
    {# Usually called from apply_grants. Should not be called directly. #}
    {{ adapter.raise_grant_error() }}
{% endmacro %}


{%- macro firebolt__get_grant_sql(relation, privilege, grantee) -%}
    {# Usually called from apply_grants. Should not be called directly. #}
    {{ adapter.raise_grant_error() }}
{%- endmacro -%}


{%- macro firebolt__get_revoke_sql(relation, privilege, grantee) -%}
    {# Usually called from apply_grants. Should not be called directly. #}
    {{ adapter.raise_grant_error() }}
{%- endmacro -%}


{% macro firebolt__copy_grants() %}
    {{ return(True) }}
{% endmacro %}


{% macro firebolt__apply_grants(relation, grant_config, should_revoke) %}
    {% if grant_config %}
        {{ adapter.raise_grant_error() }}
    {% endif %}
{% endmacro %}


{%- macro firebolt__get_dcl_statement_list(relation, grant_config, get_dcl_macro) -%}
    {# Firebolt does not support DCL statements yet #}
    {% if grant_config %}
        {{ adapter.raise_grant_error() }}
    {% endif %}
    {{ return([]) }}
{%- endmacro %}
