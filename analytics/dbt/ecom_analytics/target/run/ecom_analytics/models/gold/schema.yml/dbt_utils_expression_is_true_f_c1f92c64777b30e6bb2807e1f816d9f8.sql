
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "delta"."gold"."fct_sales_minute"

where not(paid_orders paid_orders >= 0)


  
  
      
    ) dbt_internal_test