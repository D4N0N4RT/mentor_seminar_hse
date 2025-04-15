--1
SELECT payment_status, COUNT(*) AS orders_count,
    SUM(total_amount) AS orders_total_amount, AVG(total_amount) AS orders_average_totals
FROM orders
GROUP BY payment_status;

--2
SELECT or.order_id, sum(quantity) AS items_count,
sum(product_price * quantity) AS total_product_price
FROM orders or
    JOIN order_items or_it ON or.order_id = or_it.order_id
GROUP BY or.order_id;

--3
SELECT product_name, sum(quantity) AS total_quantity, AVG(product_price) AS avg_product_price,
       SUM(product_price * quantity) AS total_product_price
FROM orders or
         JOIN order_items or_it ON or.order_id = or_it.order_id
GROUP BY product_name;

--4
SELECT formatDateTime(toStartOfDay(order_date), '%a %d %b %G') AS day, COUNT(*) AS transactions_amount,
                      SUM(total_amount) AS orders_total_amount, AVG(total_amount) AS orders_avg_amount
FROM orders
GROUP BY toStartOfDay(order_date)
ORDER BY toStartOfDay(order_date);

--5
SELECT user_id, COUNT(*) AS orders_count,
    SUM(total_amount) AS orders_total_amount
FROM orders
GROUP BY user_id
ORDER BY orders_count DESC, orders_total_amount DESC;