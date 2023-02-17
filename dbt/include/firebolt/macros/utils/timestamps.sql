{% macro firebolt__current_timestamp() %}
    NOW()
{% endmacro %}


{# TODO: Do we still need this?
   See https://packboard.atlassian.net/browse/FIR-12156 #}
{% macro firebolt__snapshot_string_as_time(timestamp) -%}
  {% call statement('timestamp', fetch_result=True) %}

      SELECT CAST('{{ timestamp }}' AS DATE)
  {% endcall %}
  {{ return(load_result('timestamp').table) }}
{% endmacro %}
