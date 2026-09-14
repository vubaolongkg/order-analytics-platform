-- created_at: 2026-09-14T15:34:37.581628+00:00
-- finished_at: 2026-09-14T15:34:41.660343800+00:00
-- elapsed: 4.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.not_null_fct_orders_order_id.4e687af8d0
-- query_id: c8bFrPP8Wp7ezoObPnUr9SUz5V3
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.not_null_fct_orders_order_id.4e687af8d0", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_orders`
where order_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-09-14T15:34:37.581600+00:00
-- finished_at: 2026-09-14T15:34:41.664811400+00:00
-- elapsed: 4.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.not_null_dim_restaurants_restaurant_id.2e7e73f873
-- query_id: bljym9d0BUzsLLoCFz60TCa342A
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.not_null_dim_restaurants_restaurant_id.2e7e73f873", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select restaurant_id
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`dim_restaurants`
where restaurant_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-09-14T15:34:37.581600+00:00
-- finished_at: 2026-09-14T15:34:41.677047500+00:00
-- elapsed: 4.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.unique_fct_orders_order_id.523ddb6ce5
-- query_id: 62bviHlNQh6boyfpiPQJgavdKK2
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.unique_fct_orders_order_id.523ddb6ce5", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select order_id as unique_field
  from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_orders`
  where order_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-09-14T15:34:37.582231900+00:00
-- finished_at: 2026-09-14T15:34:41.742125200+00:00
-- elapsed: 4.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.not_null_fct_orders_total_amount.f8b7bfac38
-- query_id: 3ehhnsNbsljk93NhgW7xxMguBcw
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.not_null_fct_orders_total_amount.f8b7bfac38", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_orders`
where total_amount is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-09-14T15:34:37.581759800+00:00
-- finished_at: 2026-09-14T15:34:41.782816400+00:00
-- elapsed: 4.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.not_null_fct_hourly_metrics_total_orders.a6615cdb5f
-- query_id: qdXRyPgYUx8eA2j18yeT5kBtiQI
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.not_null_fct_hourly_metrics_total_orders.a6615cdb5f", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_orders
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_hourly_metrics`
where total_orders is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-09-14T15:34:37.581625900+00:00
-- finished_at: 2026-09-14T15:34:41.827390800+00:00
-- elapsed: 4.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.not_null_fct_hourly_metrics_metric_hour.62303c1782
-- query_id: nB2bz6lWpsURHJWkoIvCLHKMDoQ
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.not_null_fct_hourly_metrics_metric_hour.62303c1782", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_hour
from `velvety-citizen-466214-r4`.`raw_staging_marts`.`fct_hourly_metrics`
where metric_hour is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-09-14T15:34:37.581662400+00:00
-- finished_at: 2026-09-14T15:34:42.157502600+00:00
-- elapsed: 4.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.order_analytics_dbt.unique_dim_restaurants_restaurant_id.3bd3ca0fc9
-- query_id: diFDEdPjJ2ufPQdIScATuxbe1pz
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.order_analytics_dbt.unique_dim_restaurants_restaurant_id.3bd3ca0fc9", "profile_name": "order_analytics_profile", "target_name": "dev"} */

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select restaurant_id as unique_field
  from `velvety-citizen-466214-r4`.`raw_staging_marts`.`dim_restaurants`
  where restaurant_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
