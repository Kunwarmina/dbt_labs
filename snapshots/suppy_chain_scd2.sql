{% snapshot supply_chain_scd2_snapshot %}
{{
    config(
        target_schema='SNAPSHOT_SCH',
        target_database='DBT_OUTPUT',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='updated_at',
        query_tag='dbt'
    )
}}

select *
from (
    select
        order_id,
        supplier_id,
        supplier_name,
        product_id,
        product_name,
        warehouse_id,
        warehouse_region,
        order_date,
        delivery_date,
        quantity,
        unit_cost,
        total_cost,
        status,
        updated_at,
        row_number() over (
            partition by order_id, updated_at
            order by updated_at desc
        ) as rn
    from {{ source('raw_cust', 'SUPPLY_CHAIN_ORDERS') }}
)
where rn = 1

{% endsnapshot %}
