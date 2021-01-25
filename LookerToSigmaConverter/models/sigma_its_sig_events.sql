        {{ 
            config(
                materialized='view',
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "events") }}


         SELECT id AS EVENT_ID, SESSION_ID, ip_address AS IP, USER_ID, SEQUENCE_NUMBER, (CASE WHEN sequence_number = 1 THEN TRUE ELSE FALSE END) AS IS_ENTRY_EVENT, uri AS FULL_PAGE_URL, CASE WHEN event_type = 'Product' THEN right(uri,length(uri)-9) END AS VIEWED_PRODUCT_ID, EVENT_TYPE, CASE WHEN event_type IN ('Login', 'Home') THEN '(1) Land' WHEN event_type IN ('Category', 'Brand') THEN '(2) Browse Inventory' WHEN event_type = 'Product' THEN '(3) View Product' WHEN event_type = 'Cart' THEN '(4) Add Item to Cart' WHEN event_type = 'Purchase' THEN '(5) Purchase' END AS FUNNEL_STEP, longitude||','||latitude AS LOCATION, round(longitude,1)||','||round(latitude,1) AS APPROX_LOCATION, BROWSER, OS FROM PUBLIC.events 