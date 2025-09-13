



select
    1
from "delta"."gold"."fct_sales_minute"

where not(paid_orders paid_orders >= 0)

