{{
    config(
      materialized='table'
    ) 
}}


WITH cte__bse_stg_my_first__dbt_model AS (

    SELECT 
    
        * 
        
    FROM {{ ref('bse_stg_my_first__dbt_model') }}

)

, cte__deduped_renamed_recasted_renowned as (

  SELECT 
  
      column_2 as my_column
      
  FROM cte__bse_stg_my_first__dbt_model AS cte__bse_stg_my_first__dbt_model

)

, cte__final as (
  
    SELECT 
        
        * 
        
    FROM cte__deduped_renamed_recasted_renowned

)

SELECT 
  
    *
    
FROM cte__final
