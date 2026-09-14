
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_orders`
where order_id is null



  
  
      
    ) dbt_internal_test