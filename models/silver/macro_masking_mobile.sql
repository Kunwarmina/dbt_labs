SELECT
    customer_id,
    full_name,
    {{ mask_mobile('mobile_number') }} AS masked_mobile,
    email,
    gender,
    age,
    registered_on
FROM {{ source('raw_cust', 'TELECOM_CUSTOMERS') }}
