insert into dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
select
    u.id as user_id,
    r.id as restaurant_id,
    t.id as timestamp_id,
    object_value::json->>'_id' as order_key,
    object_value::json->>'final_status' as order_status
from stg.ordersystem_orders
inner join dds.dm_users u
on u.user_id = (object_value::json->>'user')::json->>'id'
inner join dds.dm_restaurants r
on r.restaurant_id = (object_value::json->>'restaurant')::json->>'id'
    and r.active_to = timestamp'2099-12-31'
inner join dds.dm_timestamps t
on t.ts = (object_value::json->>'date')::timestamp
where update_ts > (
    select workflow_settings->>'last_update_ts'
    from dds.srv_wf_settings
    where workflow_key = 'orders'
)::timestamp or 'orders' not in (select workflow_key from dds.srv_wf_settings)
on conflict (order_key) do update
set user_id = excluded.user_id,
    restaurant_id = excluded.restaurant_id,
    timestamp_id = excluded.timestamp_id,
    order_status = excluded.order_status;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'orders', json_build_object('last_update_ts', max(update_ts)) from stg.ordersystem_orders
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
