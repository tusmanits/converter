{% macro get_persisted_flag(model_name, persisted_type, persisted_sql) %}
    
    {{return (get_persisted_value_for_persist_for(model_name, persisted_type,  persisted_sql))}}

{% endmacro %}


{% macro get_persisted_value_for_persist_for(model_name, persisted_type, persisted_sql) %}
    
    {%- set log_table = get_persisted_relation() -%}

    {% set split_list = persisted_sql.split(' ') %}
    
    {% set interval = split_list[0] %}
    {% set date_part = split_list[1] %}

    {%- set date_part1 -%}
    '{{ date_part }}'
    {%- endset -%}

    {%- set sql -%}
    SELECT LAST_LOADED_PERSISTED_VALUE AS COLUMN_ from {{ log_table }}
    WHERE LOWER(PERSISTED_MODEL) = LOWER('{{ model_name }}') AND LOWER(PERSISTED_TYPE) = lower('{{persisted_type}}')
    ORDER BY PERSISTED_TIMESTAMP DESC
    LIMIT 1
    {%- endset -%}

    {%- set last_persisted_value_raw = get_sql_value(sql) -%}

    {%- set last_persisted_value -%}
    {% if last_persisted_value_raw != None %}'{{ last_persisted_value_raw }}'{% else %}null::varchar(512){% endif %}
    {%- endset -%}

    {%- set added_date_ =  dbt_utils.dateadd(date_part1, interval, last_persisted_value)-%}

    {%- set added_date_sql -%}
    SELECT {{added_date_}} as COLUMN_
    {%- endset -%}

    {%- set added_date_raw = get_sql_value(added_date_sql) -%}

    {%- set added_date -%}
    {% if added_date_raw != None %}'{{ added_date_raw }}'{% else %}null::varchar(512){% endif %}
    {%- endset -%}

    {{log(added_date, info = True)}}

    {%- set date_part_diff -%}
    'seconds'
    {%- endset -%}

    {%- set utc_current_timestamp = dbt_utils.current_timestamp_in_utc() -%}          

    {%- set date_part_added_date_sql =  dbt_utils.datediff(added_date, utc_current_timestamp, date_part_diff) -%}

    {%- set flag_sql -%}
    SELECT {{date_part_added_date_sql}} as COLUMN_
    {%- endset -%}

    {%- set flag = get_sql_value(flag_sql)-%}

    {% if flag == None %}
    {{ return(0) }}
    {%- elif flag|int > 0 -%}
    {{ return(0) }}
    {%- else -%}
    {{ return(1) }}
    {%- endif  -%}

{% endmacro %}

