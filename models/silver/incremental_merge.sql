{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    query_tag='dbt'
) }}

WITH deduplicated_orders AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
        FROM {{ source('raw_cust', 'orders') }}
    ) AS ordered
    WHERE rn = 1
)

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    amount
FROM deduplicated_orders

{% if is_incremental() %}
WHERE order_id >= (SELECT MAX(order_id) FROM {{ this }})
{% endif %}
