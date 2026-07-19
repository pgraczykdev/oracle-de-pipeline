{{ config(materialized='table') }}

SELECT 
    date_id,
    date,
    day,
    month,
    year,
    quarter
FROM 
    {{ ref('dj_stg_dates') }}