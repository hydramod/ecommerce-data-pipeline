



select
    1
from "delta"."gold"."fct_sales_minute"

where not(gmv gmv >= 0)

