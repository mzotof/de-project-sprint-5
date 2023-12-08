drop schema if exists stg cascade;
create schema stg;

drop table if exists stg.bonussystem_users cascade;
create table stg.bonussystem_users
(
    id integer primary key,
    order_user_id text not null
);

drop table if exists stg.bonussystem_ranks cascade;
create table stg.bonussystem_ranks
(
    id integer primary key,
    name                  varchar(2048)            not null,
    bonus_percent         numeric(19, 5) not null,
    min_payment_threshold numeric(19, 5) not null
);

drop table if exists stg.bonussystem_events cascade;
create table stg.bonussystem_events
(
    id          integer primary key,
    event_ts    timestamp not null,
    event_type  varchar   not null,
    event_value text      not null
);

create index idx_bonussystem_events__event_ts
    on stg.bonussystem_events (event_ts);

drop table if exists stg.ordersystem_orders cascade;
create table stg.ordersystem_orders (
    id serial primary key,
    object_id varchar not null,
    object_value text not null,
    update_ts timestamp not null
);

drop table if exists stg.ordersystem_restaurants cascade;
create table stg.ordersystem_restaurants (
    id serial primary key,
    object_id varchar not null,
    object_value text not null,
    update_ts timestamp not null
);

drop table if exists stg.ordersystem_users cascade;
create table stg.ordersystem_users (
    id serial primary key,
    object_id varchar not null,
    object_value text not null,
    update_ts timestamp not null
);

alter table stg.ordersystem_orders add constraint ordersystem_orders_object_id_uindex unique (object_id);
alter table stg.ordersystem_restaurants add constraint ordersystem_restaurants_object_id_uindex unique (object_id);
alter table stg.ordersystem_users add constraint ordersystem_users_object_id_uindex unique (object_id);

drop table if exists stg.srv_wf_settings cascade;
create table if not exists stg.srv_wf_settings (
    id serial primary key,
    workflow_key varchar not null unique,
    workflow_settings json not null
);
