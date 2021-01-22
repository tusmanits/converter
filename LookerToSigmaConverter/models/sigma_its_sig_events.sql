        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "events") }}

        {% set persisted_type = '' %}

        {% set alias = "events" %}

        {% set persisted_sql = "" %}

        {{ config( post_hook=after_commit ("{{log_persisted_event_completed(\"\",\"\")}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

         SELECT id AS EVENT_ID, SESSION_ID, ip_address AS IP, USER_ID, SEQUENCE_NUMBER, (CASE WHEN sequence_number = 1 THEN TRUE ELSE FALSE END) AS IS_ENTRY_EVENT, (CASE WHEN sequence_number = ${sessions.number_of_events_in_session} THEN TRUE ELSE FALSE END) AS IS_EXIT_EVENT, uri AS FULL_PAGE_URL, CASE WHEN event_type = 'Product' THEN right(uri,length(uri)-9) END AS VIEWED_PRODUCT_ID, EVENT_TYPE, CASE WHEN event_type IN ('Login', 'Home') THEN '(1) Land' WHEN event_type IN ('Category', 'Brand') THEN '(2) Browse Inventory' WHEN event_type = 'Product' THEN '(3) View Product' WHEN event_type = 'Cart' THEN '(4) Add Item to Cart' WHEN event_type = 'Purchase' THEN '(5) Purchase' END AS FUNNEL_STEP, AS LOCATION, AS APPROX_LOCATION, (CASE WHEN ${users.id} > 0 THEN TRUE ELSE FALSE END) AS HAS_USER_ID, BROWSER, OS FROM PUBLIC.events 

        {%- endset -%}

        {%- set condition_ = get_persisted_regenration_flag(persisted_type, persisted_sql) -%}

        {{log(condition_, info = True)}}               

        {%- if condition_ == 1 -%}

        {%- set finalSQL -%}
        SELECT * FROM {{ this }}
        WHERE FALSE
        {%- endset -%}

        {% set ns.skipped = "true" %}
    
        {%- else -%}

        {%- set finalSQL = sourceSQL -%}

        {%- endif-%}        

        {%- if is_incremental() -%}

        {{ finalSQL }}

        {%- else -%}

        {{ sourceSQL }}

        {%- endif -%}

        {%- set bs.status = get_status_value(ns.skipped) -%}

        {%- set log_transition = log_persisted_event_transition(persisted_type, persisted_sql, bs.status) -%}