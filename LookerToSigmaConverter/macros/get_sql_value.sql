{% macro get_sql_value(sql) %}

    {% set v = dbt_utils.get_query_results_as_dict(sql) %}

    {% set dict = [] %}

    {% for COLUMN_ in v.COLUMN_ %}

    {% set COL_NAME = v.COLUMN_[loop.index0] %}

    {% set dict1 = dict.append(COL_NAME) %}

    {% endfor %}

    {% if dict|length == 0 %}

    {{ return(None) }}

    {% else %}    

    {% set value = dict|first %}

    {{ return(value) }}

    {% endif %}

{% endmacro %}