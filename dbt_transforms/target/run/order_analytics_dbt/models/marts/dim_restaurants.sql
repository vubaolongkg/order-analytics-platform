
  
    

    create or replace table `velvety-citizen-466214-r4`.`raw_staging_marts`.`dim_restaurants`
      
    
    

    
    OPTIONS()
    as (
      with orders as (
    select * from `velvety-citizen-466214-r4`.`raw_staging_staging`.`stg_orders`
)

select distinct
    restaurant_id,
    restaurant_name,
    restaurant_category,
    restaurant_lat,
    restaurant_lon
from orders
    );
  