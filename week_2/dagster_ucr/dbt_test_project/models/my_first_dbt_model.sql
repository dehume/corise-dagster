{{ config(materialized='table') }}


SELECT *
FROM analytics.dbt_table