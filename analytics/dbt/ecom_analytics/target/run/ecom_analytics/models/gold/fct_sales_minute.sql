-- back compat for old kwarg name
  
  
        
            
            
        

        

        merge into "delta"."gold"."fct_sales_minute" as DBT_INTERNAL_DEST
            using "delta"."gold"."fct_sales_minute__dbt_tmp" as DBT_INTERNAL_SOURCE
            on (
                DBT_INTERNAL_SOURCE.minute_bucket = DBT_INTERNAL_DEST.minute_bucket
            )

        
        when matched then update set
            "minute_bucket" = DBT_INTERNAL_SOURCE."minute_bucket","gmv" = DBT_INTERNAL_SOURCE."gmv","paid_orders" = DBT_INTERNAL_SOURCE."paid_orders","processed_ts" = DBT_INTERNAL_SOURCE."processed_ts"
        

        when not matched then insert
            ("minute_bucket", "gmv", "paid_orders", "processed_ts")
        values
            (DBT_INTERNAL_SOURCE."minute_bucket", DBT_INTERNAL_SOURCE."gmv", DBT_INTERNAL_SOURCE."paid_orders", DBT_INTERNAL_SOURCE."processed_ts")

    
