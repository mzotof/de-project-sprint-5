insert into dds.dm_couriers (courier_id, courier_name)
select
    object_value::json->>'_id' as courier_id,
    object_value::json->>'name' as courier_name
from stg.couriersystem_couriers
where processed_ts > (
    select workflow_settings->>'last_processed_ts'
    from dds.srv_wf_settings
    where workflow_key = 'couriers'
)::timestamp or 'couriers' not in (select workflow_key from dds.srv_wf_settings)
on conflict (courier_id) do update
set courier_name = excluded.courier_name;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'couriers', json_build_object('last_processed_ts', max(processed_ts)) from stg.couriersystem_couriers
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
