drop schema if exists dds cascade;
create schema dds;

drop table if exists dds.dm_users cascade;
create table dds.dm_users (
    id serial primary key,
    user_id varchar not null unique,
    user_name varchar not null,
    user_login varchar not null
);

drop table if exists dds.dm_restaurants cascade;
create table dds.dm_restaurants (
    id serial primary key,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    active_from timestamp not null,
    active_to timestamp not null,
    constraint dm_restaurants_restaurant_id_active_to unique (restaurant_id, active_to)
);

drop table if exists dds.dm_products cascade;
create table dds.dm_products (
    id serial primary key,
    restaurant_id integer not null references dds.dm_restaurants,
    product_id varchar not null,
    product_name varchar not null,
    product_price numeric(14, 2) not null default 0 check ( product_price >= 0 ),
    active_from timestamp not null,
    active_to timestamp not null,
    constraint dm_products_product_id_active_to_unique unique (product_id, active_to)
);

drop table if exists dds.dm_timestamps cascade;
create table dds.dm_timestamps (
    id serial primary key,
    ts timestamp not null unique,
    year smallint not null check (year >= 2022 and year < 2500),
    month smallint not null check (month between 1 and 12),
    day smallint not null check (day between 1 and 31),
    time time not null,
    date date not null
);

drop table if exists dds.dm_orders cascade;
create table dds.dm_orders (
    id serial primary key,
    user_id integer not null references dds.dm_users,
    restaurant_id integer not null references dds.dm_restaurants,
    timestamp_id integer not null references dds.dm_timestamps,
    order_key varchar not null unique,
    order_status varchar not null
);

drop table if exists dds.fct_product_sales cascade;
create table dds.fct_product_sales (
    id serial primary key,
    product_id integer not null references dds.dm_products,
    order_id integer not null references dds.dm_orders,
    count integer not null default 0 check ( count >= 0 ),
    price numeric(14, 2) not null default 0 check ( price >= 0 ),
    total_sum numeric(14, 2) not null default 0 check ( total_sum >= 0 ),
    bonus_payment numeric(14, 2) not null default 0 check ( bonus_payment >= 0 ),
    bonus_grant numeric(14, 2) not null default 0 check ( bonus_grant >= 0 ),
    constraint fct_product_sales_product_id_order_id_unique unique (product_id, order_id)
);

drop table if exists dds.srv_wf_settings cascade;
create table dds.srv_wf_settings (
    id serial primary key,
    workflow_key varchar not null unique,
    workflow_settings json not null
);
