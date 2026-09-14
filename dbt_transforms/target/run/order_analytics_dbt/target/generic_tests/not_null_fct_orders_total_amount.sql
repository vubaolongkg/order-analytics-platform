
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_orders`
where total_amount is null



  
  
      
    ) dbt_internal_test