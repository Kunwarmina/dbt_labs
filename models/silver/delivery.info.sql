{{ config(
    materialized = 'table',
    schema = 'silver_sch'
) }}

SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,

    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.PRICE,
    p.PURCHASE_DATE,

    CASE 
        WHEN p.DELIVERY_ID = 1 THEN 'Delivered'
        WHEN p.DELIVERY_ID = 2 THEN 'Shipped'
        WHEN p.DELIVERY_ID = 3 THEN 'In Transit'
        WHEN p.DELIVERY_ID = 4 THEN 'Pending'
        ELSE 'Unknown'
    END AS DELIVERY_STATUS,

    d.DELIVERY_DATE,
    d.COURIER_NAME,
    d.TRACKING_ID,
    d.DELIVERY_REMARKS

FROM {{ source('raw_cust', 'cust') }} c
JOIN {{ source('raw_prod', 'PRODUCT') }} p
    ON c.CUSTOMER_ID = p.CUSTOMER_ID
LEFT JOIN {{ source('raw_cust', 'delivery') }} d
    ON p.PRODUCT_ID = d.PRODUCT_ID;

    