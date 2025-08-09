{{ 
    config(
        materialized='incremental',
        incremental_strategy='append'
    ) 
}}

select 
    order_id,
    customer_id,
    order_date,
    order_status,
    amount
from {{ source('raw_cust', 'orders') }}

{% if is_incremental() %}
-- Optional filter for new records based on date
where order_date > (select max(order_date) from {{ this }})
{% endif %}
