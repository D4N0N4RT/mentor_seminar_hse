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
SELECT to_date(transaction_date) as transaction_date, COUNT(*) AS transactions_amount,
SUM(amount) AS total_amount, AVG(amount) AS transactions_avg_amount
FROM transactions
GROUP BY to_date(transaction_date);

--4
SELECT t.transaction_id, COUNT(*) AS logs_count
FROM logs AS l
JOIN transactions AS t ON l.transaction_id=t.transaction_id
GROUP BY t.transaction_id
ORDER BY logs_count;

--5
SELECT DISTINCT l.category,
(DENSE_RANK() over (partition by category order by t.transaction_id) 
+ DENSE_RANK() over (partition by category order by t.transaction_id DESC) 
- 1) AS transactions_count
FROM logs AS l
JOIN transactions AS t ON l.transaction_id=t.transaction_id
ORDER BY transactions_count DESC;
