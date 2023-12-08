insert into dds.dm_timestamps (ts, year, month, day, time, date)
select
    (object_value::json->>'date')::timestamp,
    extract(year from (object_value::json->>'date')::timestamp),
    extract(month from (object_value::json->>'date')::timestamp),
    extract(day from (object_value::json->>'date')::timestamp),
    ((object_value::json->>'date')::timestamp)::time,
    ((object_value::json->>'date')::timestamp)::date
from stg.ordersystem_orders
where update_ts > (
    select workflow_settings->>'last_update_ts'
    from dds.srv_wf_settings
    where workflow_key = 'timestamps'
)::timestamp or 'timestamps' not in (select workflow_key from dds.srv_wf_settings)
on conflict (ts) do nothing;


insert into dds.dm_timestamps (ts, year, month, day, time, date)
select
    delivery_ts,
    extract(year from delivery_ts),
    extract(month from delivery_ts),
    extract(day from delivery_ts),
    delivery_ts::time,
    delivery_ts::date
from stg.couriersystem_deliveries
where delivery_ts > (
    select coalesce(workflow_settings->>'last_delivery_ts', '2023-11-30T00:00:00')
    from dds.srv_wf_settings
    where workflow_key = 'timestamps'
)::timestamp or 'timestamps' not in (select workflow_key from dds.srv_wf_settings)
on conflict (ts) do nothing;


insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'timestamps', json_build_object(
    'last_update_ts', (select max(update_ts) from stg.ordersystem_orders),
    'last_delivery_ts', (select max(delivery_ts) from stg.couriersystem_deliveries)
)
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
