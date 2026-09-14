
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_hour
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_hourly_metrics`
where metric_hour is null



  
  
      
    ) dbt_internal_test