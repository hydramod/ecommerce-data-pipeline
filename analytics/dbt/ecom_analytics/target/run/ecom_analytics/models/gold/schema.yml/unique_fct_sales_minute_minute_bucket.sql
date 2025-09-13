
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    minute_bucket as unique_field,
    count(*) as n_records

from "delta"."gold"."fct_sales_minute"
where minute_bucket is not null
group by minute_bucket
having count(*) > 1



  
  
      
    ) dbt_internal_test