{{
    config(
        materialized='table',
        secure = true
    )
}}

select * from {{ ref('delivery.info') }}