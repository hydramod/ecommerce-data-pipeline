
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select paid_orders
from "delta"."gold"."fct_sales_minute"
where paid_orders is null



  
  
      
    ) dbt_internal_test