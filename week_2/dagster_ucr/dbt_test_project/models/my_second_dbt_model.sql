{{ config(materialized='table') }}


SELECT column_2 AS my_column
FROM {{ ref('my_first_dbt_model') }}
/* Uncomment the section below to cause some errors, triggering conditional Dagster flows */
--'OOPS!!!!!!!'