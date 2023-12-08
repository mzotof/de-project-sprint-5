drop table if exists dds.dm_products_new;
create table dds.dm_products_new as
select
    r.id as restaurant_id,
    json_array_elements((object_value::json->>'menu')::json)->>'_id' as product_id,
    json_array_elements((object_value::json->>'menu')::json)->>'name' as product_name,
    (json_array_elements((object_value::json->>'menu')::json)->>'price')::numeric(14, 2) as product_price,
    update_ts as active_from,
    timestamp'2099-12-31' as active_to
from stg.ordersystem_restaurants
inner join dds.dm_restaurants r
on r.restaurant_id = object_value::json->>'_id'
    and r.active_to = timestamp'2099-12-31'
where update_ts > (
    select workflow_settings->>'last_update_ts'
    from dds.srv_wf_settings
    where workflow_key = 'products'
)::timestamp or 'products' not in (select workflow_key from dds.srv_wf_settings);

insert into dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
select restaurant_id, product_id, product_name, product_price, active_from, active_to from dds.dm_products_new
on conflict (product_id, active_to) do update
set active_to = case
    when excluded.restaurant_id <> dm_products.restaurant_id
        or excluded.product_name <> dm_products.product_name
        or excluded.product_price <> dm_products.product_price
    then excluded.active_from - interval '1 microsecond'
    else dm_products.active_to
end;

insert into dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
select restaurant_id, product_id, product_name, product_price, active_from, active_to from dds.dm_products_new
on conflict (product_id, active_to) do nothing;

drop table dds.dm_products_new;

insert into dds.srv_wf_settings (workflow_key, workflow_settings)
select 'products', json_build_object('last_update_ts', max(update_ts)) from stg.ordersystem_restaurants
on conflict (workflow_key) do update set workflow_settings = excluded.workflow_settings;
