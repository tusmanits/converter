{% macro get_persisted_schema() %}
        {{ return(target.schema~'_meta') }}
{% endmacro %}

{% macro get_persisted_relation() %}

    {%- set persisted_schema=get_persisted_schema() -%}

    {%- set persisted_table =
        api.Relation.create(
            database=target.database,
            schema=persisted_schema,
            identifier='dbt_persisted_log',
            type='table'
        ) -%}

    {{ return(persisted_table) }}

{% endmacro %}

{% macro create_persisted_schema() %}
    create schema if not exists {{ get_persisted_schema() }}
{% endmacro %}


{% macro create_persisted_table() -%}

    {% set required_columns = [
       ["persisted_schema", "varchar(512)"],
       ["persisted_model", "varchar(512)"],
       ["persisted_relation","varchar(512)"],
       ["persisted_type", "varchar(512)"],
       ["persisted_timestamp", dbt_utils.type_timestamp()],
       ["persisted_user", "varchar(512)"],
       ["persisted_target", "varchar(512)"],
       ["persisted_is_full_refresh", "boolean"],
       ["persisted_sql", "varchar(1000)"],
       ["persisted_status", "varchar(250)"],
       ["persisted_transition_status", "varchar(250)"],       
       ["last_loaded_persisted_value", "varchar(512)"]
    ] -%}

    {% set persisted_table = get_persisted_relation() -%}

    {% set persisted_table_exists = adapter.get_relation(persisted_table.database, persisted_table.schema, persisted_table.name) -%}

    {% if persisted_table_exists -%}

        {%- set columns_to_create = [] -%}

        {# map to lower to cater for snowflake returning column names as upper case #}
        {%- set existing_columns = adapter.get_columns_in_relation(persisted_table)|map(attribute='column')|map('lower')|list -%}

        {%- for required_column in required_columns -%}
            {%- if required_column[0] not in existing_columns -%}
                {%- do columns_to_create.append(required_column) -%}
            {%- endif -%}
        {%- endfor -%}


        {%- for column in columns_to_create -%}
            alter table {{ persisted_table }}
            add column {{ column[0] }} {{ column[1] }}
            default null;
        {% endfor -%}

        {%- if columns_to_create|length > 0 %}
            commit;
        {% endif -%}

    {%- else -%}
        create table if not exists {{ persisted_table }}
        (
        {% for column in required_columns %}
            {{ column[0] }} {{ column[1] }}{% if not loop.last %},{% endif %}
        {% endfor %}
        )
    {%- endif -%}

{%- endmacro %}


{% macro log_persisted_event(persisted_type, persisted_timestamp, persisted_schema, persisted_relation, persisted_model, persisted_user, persisted_target, persisted_is_full_refresh,
    persisted_sql, persisted_status, persisted_transition_status, last_loaded_persisted_value) %}

    {%- set insert_sql -%}

    insert into {{ get_persisted_relation() }} (
        persisted_type,
        persisted_timestamp,
        persisted_schema,
        persisted_relation,
        persisted_model,
        persisted_user,
        persisted_target,
        persisted_is_full_refresh,
        persisted_sql,
        persisted_status,
        persisted_transition_status,
        last_loaded_persisted_value
    )

    values (
        '{{ persisted_type }}',
        {{ dbt_utils.current_timestamp_in_utc() }},
        {% if persisted_schema != None %}'{{ persisted_schema }}'{% else %}null::varchar(512){% endif %},
        {% if persisted_relation != None %}'{{ persisted_relation }}'{% else %}null::varchar(512){% endif %},
        {% if persisted_model != None %}'{{ persisted_model }}'{% else %}null::varchar(512){% endif %},
        {% if persisted_user != None %}'{{ persisted_user }}'{% else %}null::varchar(512){% endif %},
        {% if persisted_target != None %}'{{ persisted_target }}'{% else %}null::varchar(512){% endif %},
        {% if persisted_is_full_refresh %}TRUE{% else %}FALSE{% endif %},
        {% if persisted_sql != None %}'{{ persisted_sql }}'{% else %}null::varchar(512){% endif %},
        {% if persisted_status != None %}'{{ persisted_status }}'{% else %}null::varchar(1000){% endif %},
        {% if persisted_transition_status != None %}'{{ persisted_transition_status }}'{% else %}null::varchar(512){% endif %},
        {% if last_loaded_persisted_value != None %}'{{ last_loaded_persisted_value }}'::varchar(512){% else %}null::varchar(512){% endif %}
    );

    commit;

    {%- endset -%}

    {{return(insert_sql)}}

{% endmacro %}


{% macro log_persisted_event_started(persisted_type, persisted_sql, alias) %}
    {{  log_persisted_event
        (
            persisted_type = persisted_type,
            persisted_user=target.user,
            persisted_target=target.name,
            persisted_is_full_refresh=flags.FULL_REFRESH,
            persisted_model = this.name,
            persisted_relation = this.name,
            persisted_schema = this.schema,
            last_loaded_persisted_value =  None,
            persisted_status =  "STARTED",
            persisted_sql =  persisted_sql
        )
    }}  
{% endmacro %}

{% macro log_persisted_event_completed(persisted_type, persisted_sql) %}
    {{  log_persisted_event
        (
            persisted_type = persisted_type,
            persisted_user=target.user,
            persisted_target=target.name,
            persisted_is_full_refresh=flags.FULL_REFRESH,
            persisted_model = this.name,
            persisted_relation = this.name,
            persisted_schema = this.schema,
            last_loaded_persisted_value =  None,
            persisted_status =  "COMPLETED",
            persisted_sql =  persisted_sql
        )
    }}  
{% endmacro %}


{% macro log_persisted_event_transition(persisted_type, persisted_sql, persisted_transition_status) %}
    
    {%- set sql = log_persisted_event
        (
            persisted_type = persisted_type,
            persisted_user=target.user,
            persisted_target=target.name,
            persisted_is_full_refresh=flags.FULL_REFRESH,
            persisted_model = this.name,
            persisted_relation = this.name,
            persisted_schema = this.schema,
            last_loaded_persisted_value =  None,
            persisted_status =  "TRANSITION",
            persisted_transition_status = persisted_transition_status,
            persisted_sql =  persisted_sql
        ) 
    -%}

    {%- call statement('insert_transition', fetch_result=False) -%}

    {{sql}}

    {%- endcall -%}

{% endmacro %}

{% macro log_persisted_event_loaded(persisted_type, persisted_sql, last_loaded_persisted_value) %}
    
    {%- set sql = log_persisted_event
    (
        persisted_type = persisted_type,
        persisted_user=target.user,
        persisted_target=target.name,
        persisted_is_full_refresh=flags.FULL_REFRESH,
        persisted_model = this.name,
        persisted_relation = this.name,
        persisted_schema = this.schema,
        last_loaded_persisted_value =  last_loaded_persisted_value,
        persisted_status =  "LOADED",
        persisted_transition_status = None,
        persisted_sql =  persisted_sql
    ) 
    -%}

    {{log(sql, info = True)}}

    {%- call statement('insert', fetch_result=False) -%}

    {{sql}}

    {%- endcall -%}

{% endmacro %}


{% macro log_persisted_value_for_persist_for(persisted_type, persisted_sql) %}
    
    {%- set log_table = get_persisted_relation() -%}

    {% set model_name = this.name %}
    {% set model_schema = this.schema %}

    {%- set sql -%}
    SELECT UPPER(PERSISTED_TRANSITION_STATUS) AS COLUMN_ from {{ log_table }}
    WHERE 
        LOWER(PERSISTED_MODEL) = LOWER('{{ model_name }}') 
    AND LOWER(PERSISTED_SCHEMA) = LOWER('{{ model_schema }}') 
    AND LOWER(PERSISTED_TYPE) = lower('{{persisted_type}}')
    AND LOWER(PERSISTED_STATUS) = LOWER('TRANSITION')
    ORDER BY PERSISTED_TIMESTAMP DESC
    LIMIT 1
    {%- endset -%}

    {%- set transition_status = get_sql_value(sql) -%}


    {%- if transition_status == 'LOADED' -%}

        {% set split_list = persisted_sql.split(' ') %}
        
        {% set interval = split_list[0] %}
        {% set date_part = split_list[1] %}

        {%- set date_part1 -%}
        '{{ date_part }}'
        {%- endset -%}

        {%- set utc_query = dbt_utils.current_timestamp_in_utc() -%}
    
        {%- set valueQuery -%}
        SELECT CAST({{ utc_query }} AS VARCHAR(512)) AS COLUMN_    
        {%- endset -%}

        {%- set last_persisted_value = get_sql_value(valueQuery) -%}

        {%- set insert_loaded_value = log_persisted_event_loaded(persisted_type, persisted_sql, last_persisted_value) -%}

    {%- endif-%}
      
{% endmacro %}


{% macro log_persited_load_for_sql_trigger_value(persisted_type, persisted_sql) %}
    
    {%- set log_table = get_persisted_relation() -%}

    {% set model_name = this.name %}
    {% set model_schema = this.schema %}

    {%- set sql -%}
    SELECT UPPER(PERSISTED_TRANSITION_STATUS) AS COLUMN_ from {{ log_table }}
    WHERE 
        LOWER(PERSISTED_MODEL) = LOWER('{{ model_name }}') 
    AND LOWER(PERSISTED_SCHEMA) = LOWER('{{ model_schema }}') 
    AND LOWER(PERSISTED_TYPE) = lower('{{persisted_type}}')
    AND LOWER(PERSISTED_STATUS) = LOWER('TRANSITION')
    ORDER BY PERSISTED_TIMESTAMP DESC
    LIMIT 1
    {%- endset -%}

    {%- set transition_status = get_sql_value(sql) -%}


    {%- if transition_status == 'LOADED' -%}

        {%- set last_persisted_value_query -%}
        {{persisted_sql}}    
        {%- endset -%}

        {%- set last_persisted_value = get_sql_value(last_persisted_value_query) -%}

        {%- set insert_loaded_value = log_persisted_event_loaded(persisted_type, persisted_sql, last_persisted_value) -%}

    {%- endif-%}
      
{% endmacro %}


{% macro log_persisted_load(persisted_type, persisted_sql) %}
    {%- if persisted_type == 'PERSIST_FOR' -%}

    {%- set loaded = log_persisted_value_for_persist_for(persisted_type, persisted_sql) -%}

    {%- elif persisted_type == 'SQL_TRIGGER_VALUE' -%}

    {%- set loaded = log_persited_load_for_sql_trigger_value(persisted_type, persisted_sql) -%}

    {%- endif -%}  
{% endmacro %}


{% macro get_status_value(value) %}
        {% if value == "true"%}
        {{ return("SKIPPED") }}
        {% else %}
        {{return("LOADED")}}
        {%- endif -%}
{% endmacro %}


---This need to be removed

{% macro log_persisted_load1(persisted_type, status, last_loaded_persisted_value) %}

    {%- if status == "LOADED" -%}
    
    {%- if persisted_type == 'PERSIST_FOR'-%}

    {%- set utc_query = dbt_utils.current_timestamp_in_utc() -%}
    
    {%- set valueQuery -%}
    SELECT CAST({{ utc_query }} AS VARCHAR(512)) AS COLUMN_    
    {%- endset -%}

    {%- set value = get_sql_value(valueQuery) -%}

    {{log(value, info = True)}}

    {{ log_persisted_event(persisted_type = persisted_type, persisted_user=target.user, persisted_target=target.name, persisted_is_full_refresh=flags.FULL_REFRESH, persisted_model = this.name, persisted_relation = this.name, persisted_schema = this.schema, last_loaded_persisted_value =  value) }}    
    {%- else -%}
    {{ log_persisted_event(persisted_type = persisted_type, persisted_user=target.user, persisted_target=target.name, persisted_is_full_refresh=flags.FULL_REFRESH, persisted_model = this.name, persisted_relation = this.name, persisted_schema = this.schema, last_loaded_persisted_value = last_loaded_persisted_value) }}
    
    {%- endif -%}
    
    {%- endif -%}

{% endmacro %}
