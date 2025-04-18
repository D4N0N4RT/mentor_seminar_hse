--1
SELECT currency, SUM(amount) AS total_amount_in_currency
FROM transactions
WHERE currency IN ('USD', 'EUR', 'RUB')
GROUP BY currency;

--2
SELECT is_fraud, COUNT(*) AS transactions_count, 
SUM(amount) AS total_amount, AVG(amount) AS transactions_avg_amount
FROM transactions
GROUP BY is_fraud;

--3
SELECT to_date(transaction_date) as transaction_date, COUNT(*) AS transactions_count,
SUM(amount) AS total_amount, AVG(amount) AS avg_amount
FROM transactions
GROUP BY to_date(transaction_date);

--4
SELECT t.transaction_id, COUNT(*) AS logs_count
FROM logs AS l
JOIN transactions AS t ON l.transaction_id=t.transaction_id
GROUP BY t.transaction_id
ORDER BY logs_count;

--5
SELECT agg.category_name, MAX(agg.transactions_count) AS transactions_count,
SUM(agg.avg_amount) AS total_amount,
AVG(agg.avg_amount) AS avg_amount
FROM (
SELECT DISTINCT l.category AS category_name,
(DENSE_RANK() over (partition by category order by t.transaction_id) 
+ DENSE_RANK() over (partition by category order by t.transaction_id DESC) 
- 1) AS transactions_count,
AVG(amount) OVER (partition by t.transaction_id) as avg_amount
FROM logs AS l
JOIN transactions AS t ON l.transaction_id=t.transaction_id
ORDER BY category_name DESC
) AS agg
GROUP BY agg.category_name;
