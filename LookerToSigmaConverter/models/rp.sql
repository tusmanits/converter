        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        {{ config(schema = "SIGMA_ITS_SIG") }}

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
        NUMBER_SUBSEQUENT_ORDERS
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

        {%- set condition = 0 -%}

        {%- if condition == 1 -%}

        {%- set finalSQL -%}
        SELECT * FROM {{ this }}
        WHERE FALSE 
        {%- endset -%}

        {%- else -%}

        {%- set finalSQL = sourceSQL -%}        

        {%- endif-%}        

        {%- if is_incremental() -%}

        {{ finalSQL }}

        {%- else -%}

        {{ sourceSQL }}        

        {%- endif -%}
