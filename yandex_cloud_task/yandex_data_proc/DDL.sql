CREATE TABLE transactions(
transaction_id int, user_id int, amount double, currency string, 
transaction_date timestamp, is_fraud int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE logs(
log_id int, transaction_id int, category string, comment string, 
logtimestamp timestamp) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';

LOAD DATA INPATH 's3a://data-proc-main-bucket/mentor_seminar_dataproc_hw/transactions_v2.csv' OVERWRITE INTO TABLE transactions;

LOAD DATA INPATH 's3a://data-proc-main-bucket/mentor_seminar_dataproc_hw/logs_v2.txt' OVERWRITE INTO TABLE logs;