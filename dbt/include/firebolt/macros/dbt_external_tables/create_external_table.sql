{% macro firebolt__create_external_table(source_node) %}
    {%- set external = source_node.external -%}
    {%- if 'partitions' in external -%}
        {%- set columns = adapter.make_field_partition_pairs(source_node.columns.values(),
                                                             external.partitions) -%}
    {%- else -%}
        {%- set columns = adapter.make_field_partition_pairs(source_node.columns.values(),
                                                             []) -%}
    {%- endif -%}
    -- {%- set partitions = external.partitions -%}
    {%- set credentials = external.credentials -%}
    {# Leaving out "IF NOT EXISTS" because this should only be called by
       if no DROP IF is necessary. #}
    CREATE EXTERNAL TABLE {{source(source_node.source_name, source_node.name)}} (
        {%- for column in columns -%}
          {{ column }}
          {{- ',' if not loop.last }}
        {% endfor -%}
    )
    {% if external.url %} URL = '{{external.url}}' {%- endif %}
    {%- if credentials and credentials.internal_role_arn %}
        CREDENTIALS = (AWS_ROLE_ARN = '{{credentials.internal_role_arn}}'
        {%- if credentials.external_role_id %}
                       AWS_ROLE_EXTERNAL_ID = '{{credentials.external_role_id}}'
        {%- endif -%}
        )
    {% elif credentials and credentials.aws_key_id %}
        CREDENTIALS = (AWS_KEY_ID = '{{credentials.aws_key_id}}'
                       AWS_SECRET_KEY = '{{credentials.aws_secret_key}}')
    {%- endif %}
    {%- if external.object_pattern -%} OBJECT_PATTERN = '{{external.object_pattern}}' {%- endif %}
    {% if external.object_patterns -%}
        OBJECT_PATTERN =
            {%- for obj in external.object_patterns -%}
                {{ obj }}
                {{- ',' if not loop.last }}
            {%- endfor %}
    {%- endif %}
    {%- if external.compression -%} COMPRESSION = {{external.compression}} {%- endif %}
    TYPE = {{ external.type }}
{% endmacro %}
