{{ config(materialized='table') }}


SELECT *
FROM {{ source('postgresql', 'dbt_table') }}