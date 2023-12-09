insert into cdm.dm_courier_ledger (
    courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg,
    order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
)
with temp as (
    select
        c.courier_id
        ,c.courier_name
        ,t.year as settlement_year
        ,t.month as settlement_month
        ,count(distinct o.id) as orders_count
        ,sum(fps.total_sum) as orders_total_sum
        ,avg(d.rate) as rate_avg
        ,sum(fps.total_sum) * 0.25 as order_processing_fee
        ,case
            when avg(d.rate) < 4
                then sum(case when fps.total_sum * 0.05 < 100 then 100 else fps.total_sum * 0.05 end)
            when avg(d.rate) >= 4 and avg(d.rate) < 4.5
                then sum(case when fps.total_sum * 0.07 < 150 then 150 else fps.total_sum * 0.07 end)
            when avg(d.rate) >= 4.5 and avg(d.rate) < 4.9
                then sum(case when fps.total_sum * 0.08 < 175 then 175 else fps.total_sum * 0.08 end)
            when avg(d.rate) >= 4.9
                then sum(case when fps.total_sum * 0.1 < 200 then 200 else fps.total_sum * 0.1 end)
        end as courier_order_sum
        ,sum(d.tip_sum) as courier_tips_sum
    from dds.dm_couriers c
    inner join dds.dm_deliveries d
    on d.courier_id = c.id
    inner join dds.dm_orders o
    on d.order_id = o.id
    inner join dds.dm_timestamps t
    on o.timestamp_id = t.id
    left join dds.fct_product_sales fps
    on o.id = fps.order_id
    where t.date between date_trunc('month', date'{current_date}' - interval '1' month)::date and date_trunc('month', date'{current_date}')::date - 1
    group by c.courier_id, c.courier_name, t.year, t.month
)
select
    courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg,
    order_processing_fee, courier_order_sum, courier_tips_sum,
    courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
from temp
on conflict (courier_id, settlement_year, settlement_month)
do update
set courier_name = excluded.courier_name,
    orders_count = excluded.orders_count,
    orders_total_sum = excluded.orders_total_sum,
    rate_avg = excluded.rate_avg,
    order_processing_fee = excluded.order_processing_fee,
    courier_order_sum = excluded.courier_order_sum,
    courier_tips_sum = excluded.courier_tips_sum,
    courier_reward_sum = excluded.courier_reward_sum;
