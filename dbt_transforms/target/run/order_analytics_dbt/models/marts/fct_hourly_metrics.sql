
  
    

    create or replace table `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_hourly_metrics`
      
    
    

    
    OPTIONS()
    as (
      with orders as (
    select * from `velvety-citizen-466214-r4`.`raw_staging_staging`.`stg_orders`
)

select
    timestamp_trunc(event_time, hour) as metric_hour,
    restaurant_category,
    count(order_id) as total_orders,
    countif(status = 'CANCELLED') as cancelled_orders,
    round(countif(status = 'CANCELLED') / count(order_id) * 100, 2) as cancellation_rate_pct,
    sum(case when status != 'CANCELLED' then total_amount else 0 end) as net_gmv
from orders
group by 1, 2
    );
  