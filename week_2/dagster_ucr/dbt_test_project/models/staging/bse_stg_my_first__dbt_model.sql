{{
    config(
      materialized='table'
    ) 
}}


WITH cte__the_source_table_select_star AS (

  SELECT 
    
      src_import_source_table_alias_name.* 
      
  FROM {{ source('analytics', 'dbt_table') }}
      AS src_import_source_table_alias_name

)

, cte__read_forward__no_op AS (

  SELECT 
    
      cte__the_source_table_select_star.* 
    
  FROM cte__the_source_table_select_star

)

, cte__final AS (

  SELECT 
  
      cte__read_forward__no_op.* 
  
  FROM cte__read_forward__no_op

)


SELECT 
    
    * 

FROM cte__final -- the final CTE
