insert into dds.dm_deliveries (delivery_id, order_id, courier_id, address, timestamp_id, rate, tip_sum)
select
    object_value::json->>'delivery_id' as delivery_id,
    o.id as order_id,
    c.id as courier_id,
    object_value::json->>'address' as address,
    t.id as timestamp_id,
    (object_value::json->>'rate')::smallint as rate,
    (object_value::json->>'tip_sum')::numeric(14, 2) as tip_sum
from stg.couriersystem_deliveries
inner join dds.dm_orders o
on object_value::json->>'order_id' = o.order_key
inner join dds.dm_couriers c
on object_value::json->>'courier_id' = c.courier_id
inner join dds.dm_timestamps t
on delivery_ts = t.ts
where delivery_ts > (
    select workflow_settings->>'last_delivery_ts'
    from dds.srv_wf_settings
    where workflow_key = 'deliveries'
)::timestamp or 'deliveries' not in (select workflow_key from dds.srv_wf_settings)
on conflict (delivery_id) do update
set order_id = excluded.order_id,
    courier_id = excluded.courier_id,
    address = excluded.address,
    timestamp_id = excluded.timestamp_id,
    rate = excluded.rate,
    tip_sum = excluded.tip_sum;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'deliveries', json_build_object('last_delivery_ts', max(delivery_ts)) from stg.couriersystem_deliveries
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
