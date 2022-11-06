{{ config(materialized='table') }}


SELECT column_2 AS my_column
FROM {{ ref('my_first_dbt_model') }}
