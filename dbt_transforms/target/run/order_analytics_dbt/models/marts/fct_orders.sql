
  
    

    create or replace table `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_orders`
      
    
    

    
    OPTIONS()
    as (
      with orders as (
    select * from `velvety-citizen-466214-r4`.`raw_staging_staging`.`stg_orders`
)

select
    order_id,
    restaurant_id,
    customer_id,
    driver_id,
    status,
    total_amount,
    case 
        when status = 'PICKED_UP' then 'IN_TRANSIT'
        when status = 'CANCELLED' then 'CANCELLED'
        when status = 'ASSIGNED' then 'PREPARING'
        else 'PENDING'
    end as delivery_lifecycle_state,
    event_time as last_status_updated_at
from orders
    );
  