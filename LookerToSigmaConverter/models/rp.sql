        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "repeat_purchase_orders") }}

        {% set persisted_type = 'PERSIST_FOR' %}

        {% set alias = "repeat_purchase_orders" %}

        {% set persisted_sql = '1 HOUR' %}

        {{ config( pre_hook=before_begin ("{{log_persisted_event_started('PERSIST_FOR', '1 HOUR')}}")) }}

        {{ config( pre_hook=after_commit ("{{log_persisted_event_completed('PERSIST_FOR', '1 HOUR')}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

        SELECT
            ORDER_ID,
            NEXT_ORDER_ID,
            (CASE 
            WHEN NOT 
        (CASE 
            WHEN NOT 
        (CASE 
            WHEN NOT 
        (CASE 
            WHEN number_subsequent_orders >= 1 AND number_subsequent_orders <= 10 THEN TRUE 
            ELSE FALSE 
        END) AND number_subsequent_orders <= 20 THEN TRUE 
            ELSE FALSE 
        END) AND number_subsequent_orders <= 30 THEN TRUE 
            ELSE FALSE 
        END) AND number_subsequent_orders <= 30 THEN TRUE 
            ELSE FALSE 
        END) AS LESS_THAN_40,
(CASE 
            WHEN next_order_id > 0 THEN TRUE 
            ELSE FALSE 
        END) AS HAS_SUBSEQUENT_ORDER,
(CASE 
            WHEN number_subsequent_orders >= 1 AND number_subsequent_orders <= 10 THEN TRUE 
            ELSE FALSE 
        END) AS LESS_THAN_10,
(CASE 
            WHEN NOT 
        (CASE 
            WHEN number_subsequent_orders >= 1 AND number_subsequent_orders <= 10 THEN TRUE 
            ELSE FALSE 
        END) AND number_subsequent_orders <= 20 THEN TRUE 
            ELSE FALSE 
        END) AS LESS_THAN_20,
(CASE 
            WHEN NOT 
        (CASE 
            WHEN NOT 
        (CASE 
            WHEN number_subsequent_orders >= 1 AND number_subsequent_orders <= 10 THEN TRUE 
            ELSE FALSE 
        END) AND number_subsequent_orders <= 20 THEN TRUE 
            ELSE FALSE 
        END) AND number_subsequent_orders <= 30 THEN TRUE 
            ELSE FALSE 
        END) AS LESS_THAN_30,
        NUMBER_SUBSEQUENT_ORDERS,
        CURRENT_TIMESTAMP AS LOAD_TIME
            FROM (SELECT
        order_items.order_id
        , COUNT(DISTINCT repeat_order_items.id) AS number_subsequent_orders
        , MIN(repeat_order_items.created_at) AS next_order_date
        , MIN(repeat_order_items.order_id) AS next_order_id
      FROM order_items
      LEFT JOIN order_items repeat_order_items
        ON order_items.user_id = repeat_order_items.user_id
        AND order_items.created_at < repeat_order_items.created_at
      GROUP BY 1)

        {%- endset -%}

        {%- set condition_ = get_persisted_flag(this.name, persisted_type, persisted_sql) -%}

        {{log(condition_, info = True)}}               

        {%- if condition_ == 1 -%}

        {%- set finalSQL -%}
        SELECT * FROM {{ this }}
        WHERE FALSE
        {%- endset -%}

        {% set ns.skipped = "true" %}

        {{log(ns.skipped, info = True)}}
    
        {%- else -%}

        {%- set finalSQL = sourceSQL -%}

        {%- endif-%}        

        {%- if is_incremental() -%}

        {{ finalSQL }}

        {%- else -%}

        {{ sourceSQL }}

        {%- endif -%}

        {{log(ns.skipped, info = True)}}

        {%- set bs.status = get_status_value(ns.skipped) -%}

        {{log(bs.status, info = True)}}

        {%- set log_transition = log_persisted_event_transition(persisted_type, persisted_sql, bs.status) -%}

        {%- set log_load = log_persisted_load(persisted_type, persisted_sql) -%}