insert into cdm.dm_settlement_report (
    restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum,
    orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum
)
select
    r.restaurant_id,
    r.restaurant_name,
    t.date as settlement_date,
    count(distinct o.id) as orders_count,
    sum(p.product_price * f.count) as orders_total_sum,
    sum(f.bonus_payment) as orders_bonus_payment_sum,
    sum(f.bonus_grant) as orders_bonus_granted_sum,
    sum(f.total_sum / 4) as order_processing_fee,
    sum(f.total_sum - f.total_sum / 4 - f.bonus_payment) as restaurant_reward_sum
from dds.fct_product_sales f
inner join dds.dm_orders o
    on f.order_id = o.id
inner join dds.dm_timestamps t
    on o.timestamp_id = t.id
inner join dds.dm_restaurants r
    on o.restaurant_id = r.id
    and r.active_to = timestamp'2099-12-31'
inner join dds.dm_products p
    on f.product_id = p.id
    and p.active_to = timestamp'2099-12-31'
where upper(o.order_status) = 'CLOSED'
group by 1,2,3
on conflict (settlement_date, restaurant_id)
do update
set restaurant_name = excluded.restaurant_name,
    orders_count = excluded.orders_count,
    orders_total_sum = excluded.orders_total_sum,
    orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
    orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
    order_processing_fee = excluded.order_processing_fee,
    restaurant_reward_sum = excluded.restaurant_reward_sum;
