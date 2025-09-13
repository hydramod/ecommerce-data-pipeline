
    
    

select
    minute_bucket as unique_field,
    count(*) as n_records

from "delta"."gold"."fct_sales_minute"
where minute_bucket is not null
group by minute_bucket
having count(*) > 1


