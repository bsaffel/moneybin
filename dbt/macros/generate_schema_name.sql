{#
    Override dbt's default schema name generation to use custom schema names directly
    without concatenation, preventing duplication like "brandon_brandon_prep".

    When a custom schema is specified, use it exactly as provided.
    When no custom schema is specified, use the target schema from profiles.yml.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
