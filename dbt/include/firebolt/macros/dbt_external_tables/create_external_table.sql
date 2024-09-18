{% macro firebolt__create_external_table(source_node) %}
    {% if source_node.external.strategy == 'copy' %}
        {{ firebolt__create_with_copy_from(source_node) }}
    {% else %}
        {{ firebolt__create_with_external_table(source_node) }}
    {% endif %}
{% endmacro %}

{% macro firebolt__create_with_external_table(source_node) %}
    {%- set external = source_node.external -%}
    {%- if 'partitions' in external -%}
        {%- set columns = adapter.make_field_partition_pairs(source_node.columns.values(),
                                                             external.partitions) -%}
    {%- else -%}
        {%- set columns = adapter.make_field_partition_pairs(source_node.columns.values(),
                                                             []) -%}
    {%- endif -%}
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

{% macro firebolt__create_with_copy_from(source_node) %}
    {# COPY FROM is only available in Firebolt 2.0. #}
    {%- set external = source_node.external -%}
    {%- set credentials = external.credentials -%}
    {%- set where_clause = external.where -%}
    {%- set options = external.options -%}
    {%- set csv_options = options.csv_options -%}
    {%- set error_file_credentials = options.error_file_credentials -%}

    {# There are no partitions, but this formats the columns correctly. #}
    {%- if 'partitions' in external -%}
        {%- set columns = adapter.make_field_partition_pairs(source_node.columns.values(),
                                                             external.partitions) -%}
    {%- else -%}
        {%- set columns = adapter.make_field_partition_pairs(source_node.columns.values(),
                                                             []) -%}
    {%- endif -%}
    COPY INTO {{source(source_node.source_name, source_node.name)}}
    {%- if columns and columns | length > 0 %}
    (
        {%- for column in columns -%}
          {{ column.name }}
          {%- if column.default %} DEFAULT {{ column.default }}{% endif %}
          {%- if column.source_column_name %} {{ '$' ~ loop.index0 }}{% endif %}
          {{- ',' if not loop.last }}
        {%- endfor -%}
    )
    {%- endif %}
    FROM '{{external.url}}'
    {%- if options or credentials %}
        WITH
        {%- if options.object_pattern %}
            PATTERN = '{{options.object_pattern}}'
        {%- endif %}
        {%- if options.type %}
            TYPE = {{ options.type }}
        {%- endif %}
        {%- if options.auto_create %}
            AUTO_CREATE = {{ options.auto_create | upper }}
        {%- endif %}
        {%- if options.allow_column_mismatch %}
            ALLOW_COLUMN_MISMATCH = {{ options.allow_column_mismatch | upper }}
        {%- endif %}
        {%- if options.error_file %}
            ERROR_FILE = '{{ options.error_file }}'
        {%- endif %}
        {%- if error_file_credentials %}
            ERROR_FILE_CREDENTIALS = (AWS_KEY_ID = '{{ error_file_credentials.aws_key_id }}' AWS_SECRET_KEY = '{{ error_file_credentials.aws_secret_key }}')
        {%- endif %}
        {%- if options.max_errors_per_file %}
            MAX_ERRORS_PER_FILE = {{ options.max_errors_per_file }}
        {%- endif %}
        {%- if csv_options %}
            {%- if csv_options.header %}
                HEADER = {{ csv_options.header | upper }}
            {%- endif %}
            {%- if csv_options.delimiter %}
                DELIMITER = '{{ csv_options.delimiter }}'
            {%- endif %}
            {%- if csv_options.newline %}
                NEWLINE = '{{ csv_options.newline }}'
            {%- endif %}
            {%- if csv_options.quote %}
                QUOTE = {{ csv_options.quote }}
            {%- endif %}
            {%- if csv_options.escape %}
                ESCAPE = '{{ csv_options.escape }}'
            {%- endif %}
            {%- if csv_options.null_string %}
                NULL_STRING = '{{ csv_options.null_string }}'
            {%- endif %}
            {%- if csv_options.empty_field_as_null %}
                EMPTY_FIELD_AS_NULL = {{ csv_options.empty_field_as_null | upper }}
            {%- endif %}
            {%- if csv_options.skip_blank_lines %}
                SKIP_BLANK_LINES = {{ csv_options.skip_blank_lines | upper }}
            {%- endif %}
            {%- if csv_options.date_format %}
                DATE_FORMAT = '{{ csv_options.date_format }}'
            {%- endif %}
            {%- if csv_options.timestamp_format %}
                TIMESTAMP_FORMAT = '{{ csv_options.timestamp_format }}'
            {%- endif %}
        {%- endif %}
        {%- if credentials %}
            CREDENTIALS = (AWS_KEY_ID = '{{credentials.aws_key_id}}' AWS_SECRET_KEY = '{{credentials.aws_secret_key}}')
        {%- endif %}
    {%- endif %}
    {%- if where_clause %}
        WHERE {{ where_clause }}
    {%- endif %}
{% endmacro %}
