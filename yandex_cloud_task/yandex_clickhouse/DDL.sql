CREATE TABLE orders(
    order_id INT, user_id INT, order_date DATETIME,
    total_amount DECIMAL(12,2), payment_status String
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE order_items (
    item_id INT, order_id INT, product_name String,
    product_price DECIMAL(12,2), quantity INT
)
ENGINE = MergeTree
ORDER BY item_id;

INSERT INTO orders (order_id, user_id, order_date, total_amount, payment_status)
SELECT *
FROM s3(
        'https://storage.yandexcloud.net/data-proc-main-bucket/mentor_seminar_dataproc_hw/orders.csv',
        'CSVWithNames', 'order_id INT, user_id INT, order_date DATETIME, total_amount DECIMAL(12,2), payment_status String'
    );

INSERT INTO order_items (item_id, order_id, product_name, product_price, quantity)
SELECT *
FROM s3(
        'https://storage.yandexcloud.net/data-proc-main-bucket/mentor_seminar_dataproc_hw/order_items.txt',
        'CSVWithNames', 'item_id INT, order_id INT, product_name String, product_price DECIMAL(12,2), quantity INT'
    ) SETTINGS format_csv_delimiter = ';';