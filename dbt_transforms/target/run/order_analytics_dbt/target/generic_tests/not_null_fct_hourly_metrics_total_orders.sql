
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_orders
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_hourly_metrics`
where total_orders is null



  
  
      
    ) dbt_internal_test