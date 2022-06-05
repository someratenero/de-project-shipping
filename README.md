
# DE Project 1
## Уточнение задания
Задача — построить витрину для RFM-классификации. В базе две схемы: production и analysis. В схеме production содержатся оперативные таблицы.
Для анализа нужно отобрать только успешно выполненные заказы.
Витрина должна располагаться в базе в схеме analysis.
Витрина должна состоять из таких полей:
- user_id
- recency (число от 1 до 5)
- frequency (число от 1 до 5)
- monetary_value (число от 1 до 5)

В витрине нужны данные с начала 2021 года.
Название витрины: dm_rfm_segments.
Успешно выполненный заказ - это заказ со статусом Closed.
## План работы
Взять пользователей из схемы продакшн Users, информацию по заказам из таблицы Orders, информацию о статусе из таблицы orderStatuses. Добавить условия фильтрации по успешности заказа и периоду (с начала 2021)
https://disk.yandex.ru/i/RQyi1a5dBcVBuw
## Качество данных
- Нет данных за 2021 год, хотя в условии просили данные с прошлого года. Первая дата в данных по заказам 12 февраля 2022 года (последняя дата - 14 марта 2022 года).
- Дублей в юзерах нет.
- Названия колонок в таблице с пользователями перепутаны местами для name и login.
- Для цен обычно оставляют только два знака после запятой, а в этих таблицах - 5. 
- Нет ограничения на поле status в таблице Orders, можно было бы ссылаться на id из таблицы OrderStatuses.
- Тип даты одинаковый во всех таблицах timestamp.
- Использованы следующие инструменты для обеспечения качества данных:
  - Ограничения NOT NULL, указывающие, что столбец не может принимать значение NULL в большинстве полей таблиц
  - Ограничения-проверки, обозначающие, что значение столбца должно удовлетворять определённому логическому выражению. Например, цена товара может быть строго положительной в таблице production.Products, скидка не должна быть больше цены продукта в таблице production.OrderItems, цена в таблице production.Orders должна быть суммой основной цены и бонуса
  - Ограничения уникальности, например на комбинацию полей order_id и product_id в   production.OrderItems и  order_id, status_id в таблице OrderStatusLog
  - Первичные ключи во всех таблицах
  - Ограничения внешнего ключа, например в таблицах production.OrderItems внешний ключ на product_id, order_id; в production.OrderStatusLog на order_id и status_id

## Создание представлений в схеме analysis
CREATE OR replace VIEW analysis.products
AS
  SELECT *
  FROM   production.products;

CREATE OR replace VIEW analysis.orderstatuses
AS
  SELECT *
  FROM   production.orderstatuses;

CREATE OR replace VIEW analysis.users
AS
  SELECT *
  FROM   production.users;

CREATE OR replace VIEW analysis.orders
AS
  SELECT *
  FROM   production.orders;

CREATE OR replace VIEW analysis.orderitems
AS
  SELECT *
  FROM   production.orderitems;


## DDL-запрос для создания витрины

DROP TABLE IF EXISTS analysis.dm_rfm_segments;
CREATE TABLE analysis.dm_rfm_segments (
       user_id INT PRIMARY KEY,
       recency INT CHECK (recency BETWEEN 1 AND 5 ),
       frequency INT CHECK (frequency BETWEEN 1 AND 5 ),
       monetary_value INT CHECK (monetary_value BETWEEN 1 AND 5 ) );
 
 
## SQL-запрос для заполнения витрины
INSERT INTO analysis.dm_rfm_segments
(user_id, recency, frequency, monetary_value)
WITH t AS (
SELECT DISTINCT u.id AS user_id,
CAST((MAX(order_ts) OVER ()) AS DATE) - CAST((MAX(CASE WHEN os.key = 'Closed' THEN order_ts ELSE NULL END) OVER(PARTITION BY u.id)) AS DATE) days_FROM_last_order,
COUNT(CASE WHEN os.key = 'Closed' THEN order_ts ELSE NULL END) OVER (PARTITION BY u.id) orders_cnt,
COALESCE(SUM(CASE WHEN os.key = 'Closed' THEN payment ELSE NULL END) OVER (PARTITION BY u.id),0) AS payment_BY_user
FROM analysis.Users u
LEFT JOIN analysis.Orders o
ON u.id = o.user_id
LEFT JOIN analysis.OrderStatuses os
ON o.status=os.id
WHERE EXTRACT(YEAR FROM order_ts) >= 2021
)
SELECT
DISTINCT user_id,
CASE
WHEN row_number() OVER (order BY days_FROM_last_order desc) between 1 and 200 THEN 1
WHEN row_number() OVER (order BY days_FROM_last_order desc) between 201 and 400 THEN 2
WHEN row_number() OVER (order BY days_FROM_last_order desc) between 401 and 600 THEN 3
WHEN row_number() OVER (order BY days_FROM_last_order desc) between 601 and 800 THEN 4
ELSE 5
END AS recency,
CASE
WHEN row_number() OVER (order BY orders_cnt) between 1 and 200 THEN 1
WHEN row_number() OVER (order BY orders_cnt) between 201 and 400 THEN 2
WHEN row_number() OVER (order BY orders_cnt) between 401 and 600 THEN 3
WHEN row_number() OVER (order BY orders_cnt) between 601 and 800 THEN 4
ELSE 5 END AS frequency, 
CASE
WHEN row_number() OVER (order BY payment_BY_user) between 1 and 200 THEN 1
WHEN row_number() OVER (order BY payment_BY_user) between 201 and 400 THEN 2
WHEN row_number() OVER (order BY payment_BY_user) between 401 and 600 THEN 3
WHEN row_number() OVER (order BY payment_BY_user) between 601 and 800 THEN 4
ELSE 5 END AS monetary_value
FROM t;

## Доработка представлений 
CREATE OR REPLACE VIEW analysis.Orders AS
SELECT o.*, osl.status AS status FROM production.Orders o
LEFT JOIN
       (SELECT
       osl.order_id,
       osl.status_id AS status
       FROM production.OrderStatusLog osl
       LEFT JOIN
            (SELECT
             DISTINCT order_id,
             MAX(dttm) OVER (PARTITION BY order_id) last_dt
             FROM production.OrderStatusLog) t2
       ON osl.order_id=t2.order_id
       WHERE t2.last_dt=osl.dttm) osl
ON o.order_id=osl.order_id;
 


