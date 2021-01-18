{% macro get_status_value(value) %}

        {% if value == "true"%}
        {{ return("SKIPPED") }}
        {% else %}
        {{return("LOADED")}}
        {%- endif -%}
{% endmacro %}