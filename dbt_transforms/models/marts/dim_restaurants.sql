with orders as (
    select * from {{ ref('stg_orders') }}
)

select distinct
    restaurant_id,
    restaurant_name,
    restaurant_category,
    restaurant_lat,
    restaurant_lon
from orders