-- Новая таблица cdm.dm_courier_ledger

drop table if exists cdm.dm_courier_ledger cascade;
create table cdm.dm_courier_ledger (
    id serial primary key,
    courier_id varchar not null,
    courier_name varchar not null,
    settlement_year int not null check (settlement_year between 2022 and 2499),
    settlement_month int not null check (settlement_month between 1 and 12),
    orders_count int not null default 0 check (orders_count >= 0),
    orders_total_sum numeric(14, 2) not null default 0 check (orders_total_sum >= 0),
    rate_avg numeric(3, 2) not null default 0 check (rate_avg between 1 and 5),
    order_processing_fee numeric(14, 2) not null default 0 check (order_processing_fee >= 0),
    courier_order_sum numeric(14, 2) not null default 0 check (courier_order_sum >= 0),
    courier_tips_sum numeric(14, 2) not null default 0 check (courier_tips_sum >= 0),
    courier_reward_sum numeric(14, 2) not null default 0 check (courier_reward_sum >= 0),
    constraint one_courier_by_period unique (courier_id, settlement_year, settlement_month)
);

-- новые таблицы dds

drop table if exists dds.dm_couriers cascade;
create table dds.dm_couriers (
    id serial primary key,
    courier_id varchar not null unique,
    courier_name varchar not null
);

drop table if exists dds.dm_deliveries cascade;
create table dds.dm_deliveries (
    id serial primary key,
    delivery_id varchar not null unique,
    order_id integer not null references dds.dm_orders,
    courier_id integer not null references dds.dm_couriers,
    address text not null,
    timestamp_id integer not null references dds.dm_timestamps,
    rate smallint not null default 0 check (rate between 0 and 5),
    tip_sum numeric(14, 2) not null default 0 check (tip_sum >= 0)
);

-- новые таблицы stg

drop table if exists stg.couriersystem_couriers cascade;
create table stg.couriersystem_couriers (
    id serial primary key,
    object_id varchar not null unique,
    object_value text not null,
    processed_ts timestamp not null
);

drop table if exists stg.couriersystem_deliveries cascade;
create table stg.couriersystem_deliveries (
    id serial primary key,
    object_id varchar not null unique,
    object_value text not null,
    delivery_ts timestamp not null
);
