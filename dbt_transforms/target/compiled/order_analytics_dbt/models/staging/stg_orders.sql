with source as (
    select * from `velvety-citizen-466214-r4`.`raw_staging`.`bronze_orders`
),

ranked_orders as (
    select
        order_id,
        status,
        restaurant_id,
        restaurant_name,
        restaurant_category,
        restaurant_lat,
        restaurant_lon,
        customer_id,
        customer_lat,
        customer_lon,
        driver_id,
        cast(total_amount as float64) as total_amount,
        timestamp_millis(event_timestamp) as event_time,
        row_number() over (
            partition by order_id 
            order by event_timestamp desc
        ) as rn
    from source
)

select
    order_id,
    status,
    restaurant_id,
    restaurant_name,
    restaurant_category,
    restaurant_lat,
    restaurant_lon,
    customer_id,
    customer_lat,
    customer_lon,
    driver_id,
    total_amount,
    event_time
from ranked_orders
where rn = 1