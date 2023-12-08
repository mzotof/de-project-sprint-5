# Витрина cdm.dm_courier_ledger

Состав полей:
- id - автосгенерированный идентификатор записи 
- courier_id — ID курьера, которому перечисляем
- courier_name — Ф. И. О. курьера
- settlement_year — год отчёта
- settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь
- orders_count — количество заказов за период (месяц)
- orders_total_sum — общая стоимость заказов
- rate_avg — средний рейтинг курьера по оценкам пользователей
- order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25
- courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже)
- courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых
- courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

# Необходимые таблицы dds

- dm_orders - описание заказов, необходимые поля:
  - timestamp_id - для связи с таблицей dm_timestamps
- dm_couriers (необходимо создать) - описание курьеров, создаваемые поля:
  - id - автосгенерированный идентификатор курьера
  - courier_id — ID курьера из источника
  - courier_name — Ф. И. О. курьера
- dm_timestamps - временные метки заказов, необходимые поля:
  - year = settlement_year
  - month = settlement_month
- fct_product_sales - факты продаж, необходимые поля:
  - total_sum - для расчета orders_total_sum и order_processing_fee, courier_order_sum и courier_reward_sum
- dm_deliveries (необходимо создать) - описание доставок, создаваемые поля:
  - id - автосгенерированный идентификатор доставки
  - delivery_id - ID доставки из источника
  - order_id - ссылка на заказ
  - courier_id - ссылка на курьера
  - address - адрес доставки
  - timestamp_id - ссылка на временную метку завершения доставки
  - rate - рейтинг курьера, для расчета rate_avg, courier_order_sum и courier_reward_sum
  - tip_sum - чаевые курьера, для расчета courier_tips_sum и courier_reward_sum

# Новые таблицы stg

- couriersystem_couriers - на ее основе будет построена таблица dds.dm_couriers, получается запросом GET /couriers
- couriersystem_deliveries - на ее основе будет построена таблица dds.dm_deliveries, получается запросом GET /deliveries
