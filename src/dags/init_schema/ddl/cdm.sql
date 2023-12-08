drop schema if exists cdm cascade;
create schema cdm;

drop table if exists cdm.dm_settlement_report cascade;
create table cdm.dm_settlement_report (
    id serial not null,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    settlement_date date not null,
    orders_count integer not null,
    orders_total_sum numeric(14, 2) not null,
    orders_bonus_payment_sum numeric(14, 2) not null,
    orders_bonus_granted_sum numeric(14, 2) not null,
    order_processing_fee numeric(14, 2) not null,
    restaurant_reward_sum numeric(14, 2) not null
);

alter table cdm.dm_settlement_report add constraint pk primary key (id);
alter table cdm.dm_settlement_report add constraint dm_settlement_report_settlement_date_check check (settlement_date >= date'2022-01-01' AND settlement_date < date'2500-01-01');

alter table cdm.dm_settlement_report alter column orders_count set default 0;
alter table cdm.dm_settlement_report alter column orders_total_sum set default 0;
alter table cdm.dm_settlement_report alter column orders_bonus_payment_sum set default 0;
alter table cdm.dm_settlement_report alter column orders_bonus_granted_sum set default 0;
alter table cdm.dm_settlement_report alter column order_processing_fee set default 0;
alter table cdm.dm_settlement_report alter column restaurant_reward_sum set default 0;

alter table cdm.dm_settlement_report add constraint orders_count_gte_0 check (orders_count >= 0);
alter table cdm.dm_settlement_report add constraint orders_total_sum_gte_0 check (orders_total_sum >= 0);
alter table cdm.dm_settlement_report add constraint orders_bonus_payment_sum_gte_0 check (orders_bonus_payment_sum >= 0);
alter table cdm.dm_settlement_report add constraint orders_bonus_granted_sum_gte_0 check (orders_bonus_granted_sum >= 0);
alter table cdm.dm_settlement_report add constraint order_processing_fee_gte_0 check (order_processing_fee >= 0);
alter table cdm.dm_settlement_report add constraint restaurant_reward_sum_gte_0 check (restaurant_reward_sum >= 0);

alter table cdm.dm_settlement_report add constraint one_restaurant_by_period unique (settlement_date, restaurant_id);
