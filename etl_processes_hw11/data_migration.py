import logging
from datetime import datetime
from pyhive import hive
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.utils.dates import days_ago

logging.basicConfig(level=logging.INFO)

hive_host = "89.169.151.77"
hive_port = 10000
hive_username = "ubuntu"
hive_database = "homework"


def extract_results(rows):
    data = []
    for row in rows:
        if len(row) < 4:
            continue
        date_str = row[0]
        if not date_str:
            continue
        try:
            first_col = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            try:
                first_col = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()
            except ValueError:
                first_col = date_str
        transaction_count = int(row[1]) if row[1] else 0
        total_amount = float(row[2]) if row[2] else 0.0
        avg_amount = float(row[3]) if row[3] else 0.0
        data.append((first_col, transaction_count, total_amount, avg_amount))
    return data


def extract_hive_data(**kwargs):
    conn = hive.Connection(host=hive_host, port=hive_port, username=hive_username, database=hive_database)
    cur = conn.cursor()
    transactions_query = """
    SELECT to_date(transaction_date) as transaction_date, COUNT(*) AS transactions_count,
    SUM(amount) AS total_amount, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY to_date(transaction_date)
    """
    cur.execute(transactions_query)
    transactions_result = cur.fetchall()

    logging.info("Extracted transactions")

    categories_query = """
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
    GROUP BY agg.category_name
    """
    cur.execute(categories_query)
    categories_result = cur.fetchall()

    logging.info("Finished data extraction")

    kwargs['ti'].xcom_push(key='extracted_transactions', value=transactions_result)
    kwargs['ti'].xcom_push(key='extracted_categories', value=categories_result)
    cur.close()
    conn.close()


def load_data_to_clickhouse(**kwargs):
    ti = kwargs['ti']

    extracted_transactions = ti.xcom_pull(task_ids='extract_hive', key='extracted_transactions')
    extracted_categories = ti.xcom_pull(task_ids='extract_hive', key='extracted_categories')
    click_hook = ClickHouseHook(clickhouse_conn_id='click_conn')

    transactions_data = extract_results(extracted_transactions)

    click_hook.execute('INSERT INTO transaction_dates (transaction_date, transactions_count, total_amount, avg_amount) VALUES', transactions_data)

    categories_data = extract_results(extracted_categories)

    click_hook.execute('INSERT INTO categories (category_name, transactions_count, total_amount, avg_amount) VALUES', categories_data)
    logging.info("Finished data loading")


with DAG(
        dag_id='data_migration_dag',
        schedule_interval=None,
        catchup=False,
        start_date=days_ago(1),
        tags=["migration"],
) as dag:
    task_start = EmptyOperator(task_id='start', dag=dag)
    task_finish = EmptyOperator(task_id='finish', dag=dag)
    create_tables = ClickHouseOperator(
        task_id='create_tables_clickhouse',
        database='homework',
        sql=(
            '''
                CREATE TABLE IF NOT EXISTS transaction_dates(
                transaction_date DATE, transactions_count INT, 
                total_amount DECIMAL(14,2), avg_amount DECIMAL(14,2)
                ) ENGINE = ReplacingMergeTree
                ORDER BY transaction_date;
            ''', '''
                CREATE TABLE IF NOT EXISTS categories(
                category_name String, transactions_count INT, 
                total_amount DECIMAL(14,2), avg_amount DECIMAL(14,2)
                ) ENGINE = ReplacingMergeTree
                ORDER BY category_name;
            ''',
        ),
        clickhouse_conn_id='click_conn',
        dag=dag
    )
    hive_extract = PythonOperator(task_id='extract_hive', python_callable=extract_hive_data, provide_context=True, dag=dag)
    clickhouse_load = PythonOperator(task_id='clickhouse_load', python_callable=load_data_to_clickhouse, provide_context=True, dag=dag)

    task_start >> create_tables >> hive_extract >> clickhouse_load >> task_finish
