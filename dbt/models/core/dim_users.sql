{{ config(materialized='table') }}

SELECT 
    user_id,
    first_name,
    last_name,
    age,
    gender,
    city,
    state,
    country
FROM 
    {{ ref('dj_stg_users') }}