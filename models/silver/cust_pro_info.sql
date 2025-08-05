select 
    c.cust_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.full_name,
    c.EMAIL,
    c.PHN_NUMBER,
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.PRICE,
    p.PURCHASE_DATE
from 
{{ ref('customer_info') }} c
join
{{ ref('prod_info') }} p
 on c.cust_ID = p.CUSTOMER_ID