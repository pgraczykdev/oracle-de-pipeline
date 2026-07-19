{{ config(materialized='table') }}

SELECT 
    product_id,
    product_name,
    category,
    brand,
    price,
    rating,
    stock
FROM 
    {{ ref('dj_stg_products') }}