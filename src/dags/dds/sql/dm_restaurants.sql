drop table if exists dds.dm_restaurants_new;
create table dds.dm_restaurants_new as
select
    object_value::json->>'_id' as restaurant_id,
    object_value::json->>'name' as restaurant_name,
    update_ts as active_from,
    timestamp'2099-12-31' as active_to
from stg.ordersystem_restaurants
where update_ts > (
    select workflow_settings->>'last_update_ts'
    from dds.srv_wf_settings
    where workflow_key = 'restaurants'
)::timestamp or 'restaurants' not in (select workflow_key from dds.srv_wf_settings);

insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
select restaurant_id, restaurant_name, active_from, active_to from dds.dm_restaurants_new
on conflict (restaurant_id, active_to) do update
set active_to = case when excluded.restaurant_name <> dm_restaurants.restaurant_name then excluded.active_from - interval '1 microsecond' else dm_restaurants.active_to end;

insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
select restaurant_id, restaurant_name, active_from, active_to from dds.dm_restaurants_new
on conflict (restaurant_id, active_to) do nothing;

drop table dds.dm_restaurants_new;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'restaurants', json_build_object('last_update_ts', max(update_ts)) from stg.ordersystem_restaurants
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
