
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select minute_bucket
from "delta"."gold"."fct_sales_minute"
where minute_bucket is null



  
  
      
    ) dbt_internal_test