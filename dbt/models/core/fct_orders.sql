{{ config(materialized="table") }}

SELECT 
    o.cart_id,
    o.user_id,
    o.product_id,
    d.date_id,
    o.quantity,
    o.price,
    o.total,
    o.discounted_total
FROM 
    {{ ref('dj_stg_orders') }} o
LEFT OUTER JOIN {{ ref('dim_dates') }} d ON o.order_date = d.date

