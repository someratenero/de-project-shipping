-- Создание представлений в схеме analysis
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

-- DDL-запрос для создания витрины

DROP TABLE IF EXISTS analysis.dm_rfm_segments; 
CREATE TABLE analysis.dm_rfm_segments ( 
        user_id INT PRIMARY KEY, 
        recency INT CHECK (recency BETWEEN 1 AND 5 ), 
        frequency INT CHECK (frequency BETWEEN 1 AND 5 ), 
        monetary_value INT CHECK (monetary_value BETWEEN 1 AND 5 ) );

-- SQL-запрос для заполнения витрины
-- INSERT INTO analysis.dm_rfm_segments
-- (user_id, recency, frequency, monetary_value)
-- WITH t AS (
-- SELECT DISTINCT u.id AS user_id,
-- CAST((MAX(order_ts) OVER ()) AS DATE) - CAST((MAX(CASE WHEN os.key = 'Closed' THEN order_ts ELSE NULL END) OVER(PARTITION BY u.id)) AS DATE) days_FROM_last_order,
-- COUNT(CASE WHEN os.key = 'Closed' THEN order_ts ELSE NULL END) OVER (PARTITION BY u.id) orders_cnt,
-- COALESCE(SUM(CASE WHEN os.key = 'Closed' THEN payment ELSE NULL END) OVER (PARTITION BY u.id),0) AS payment_BY_user
-- FROM analysis.Users u
-- LEFT JOIN analysis.Orders o
-- ON u.id = o.user_id
-- LEFT JOIN analysis.OrderStatuses os
-- ON o.status=os.id
-- WHERE EXTRACT(YEAR FROM order_ts) >= 2021 
-- ) 
-- SELECT 
--  DISTINCT user_id,
--  CASE 
--  WHEN row_number() OVER (order BY days_FROM_last_order desc) between 1 and 200 THEN 1 
--  WHEN row_number() OVER (order BY days_FROM_last_order desc) between 201 and 400 THEN 2
--  WHEN row_number() OVER (order BY days_FROM_last_order desc) between 401 and 600 THEN 3
--  WHEN row_number() OVER (order BY days_FROM_last_order desc) between 601 and 800 THEN 4
--  ELSE 5
--  END AS recency,
--  CASE 
--  WHEN row_number() OVER (order BY orders_cnt) between 1 and 200 THEN 1 
--  WHEN row_number() OVER (order BY orders_cnt) between 201 and 400 THEN 2
--  WHEN row_number() OVER (order BY orders_cnt) between 401 and 600 THEN 3
--  WHEN row_number() OVER (order BY orders_cnt) between 601 and 800 THEN 4
--  ELSE 5 END AS frequency
-- , CASE 
--  WHEN row_number() OVER (order BY payment_BY_user) between 1 and 200 THEN 1 
--  WHEN row_number() OVER (order BY payment_BY_user) between 201 and 400 THEN 2
--  WHEN row_number() OVER (order BY payment_BY_user) between 401 and 600 THEN 3
--  WHEN row_number() OVER (order BY payment_BY_user) between 601 and 800 THEN 4
--  ELSE 5 END AS monetary_value
-- FROM t;

-- SQL-запрос для заполнения витрины

WITH t AS (
SELECT 
distinct u.id as user_id, 
CAST((MAX(order_ts) OVER ()) AS DATE) - CAST((MAX(order_ts) OVER(PARTITION BY u.id)) AS DATE) days_from_last_order,
COUNT(order_ts) OVER (PARTITION BY u.id) orders_cnt,
SUM(payment) OVER (PARTITION BY u.id) AS payment_by_user
FROM analysis.Users u
LEFT JOIN (SELECT * FROM analysis.orders 
WHERE EXTRACT(YEAR FROM order_ts) >= 2021 AND status = 4) o
ON u.id = o.user_id)

SELECT DISTINCT user_id, 
NTILE(5) OVER (ORDER BY days_from_last_order DESC NULLS FIRST) AS recency,
NTILE(5) OVER (ORDER BY orders_cnt NULLS FIRST) AS frequency,
NTILE(5) OVER (ORDER BY payment_by_user NULLS FIRST) AS monetary_value
FROM t

-- Доработка представления
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
