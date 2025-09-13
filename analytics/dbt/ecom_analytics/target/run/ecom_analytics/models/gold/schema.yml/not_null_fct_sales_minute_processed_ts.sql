
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select processed_ts
from "delta"."gold"."fct_sales_minute"
where processed_ts is null



  
  
      
    ) dbt_internal_test