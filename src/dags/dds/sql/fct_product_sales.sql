insert into dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
with stg_ as (
select
    json_array_elements((event_value::json->>'product_payments')::json)->>'product_id' product_id,
    event_value::json->>'order_id' order_id,
    json_array_elements((event_value::json->>'product_payments')::json)->>'quantity' count,
    json_array_elements((event_value::json->>'product_payments')::json)->>'price' price,
    json_array_elements((event_value::json->>'product_payments')::json)->>'product_cost' total_sum,
    json_array_elements((event_value::json->>'product_payments')::json)->>'bonus_payment' bonus_payment,
    json_array_elements((event_value::json->>'product_payments')::json)->>'bonus_grant' bonus_grant,
    event_ts
from stg.bonussystem_events
)
select
    p.id as product_id,
    o.id as order_id,
    count::int as count,
    price::numeric(14, 2) as price,
    total_sum::numeric(14, 2) as total_sum,
    bonus_payment::numeric(14, 2) as bonus_payment,
    bonus_grant::numeric(14, 2) as bonus_grant
from stg_
inner join dds.dm_products p
on stg_.product_id = p.product_id
    and p.active_to = timestamp'2099-12-31'
inner join dds.dm_orders o
on stg_.order_id = o.order_key
where event_ts > (
    select workflow_settings->>'last_update_ts'
    from dds.srv_wf_settings
    where workflow_key = 'fct_product_sales'
)::timestamp or 'fct_product_sales' not in (select workflow_key from dds.srv_wf_settings)
on conflict (product_id, order_id) do update
set count = excluded.count,
    price = excluded.price,
    total_sum = excluded.total_sum,
    bonus_payment = excluded.bonus_payment,
    bonus_grant = excluded.bonus_grant;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'fct_product_sales', json_build_object('last_update_ts', max(event_ts)) from stg.bonussystem_events
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
