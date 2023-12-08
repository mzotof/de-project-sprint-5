insert into dds.dm_users (user_id, user_name, user_login)
select
    object_value::json->>'_id' as user_id,
    object_value::json->>'name' as user_name,
    object_value::json->>'login' as user_login
from stg.ordersystem_users
where update_ts > (
    select workflow_settings->>'last_update_ts'
    from dds.srv_wf_settings
    where workflow_key = 'users'
)::timestamp or 'users' not in (select workflow_key from dds.srv_wf_settings)
on conflict (user_id) do update
set user_name = excluded.user_name,
    user_login = excluded.user_login;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'users', json_build_object('last_update_ts', max(update_ts)) from stg.ordersystem_users
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
