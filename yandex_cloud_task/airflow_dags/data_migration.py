from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

logging.basicConfig(level=logging.INFO)


def extract_hive_data(**kwargs):
    hive_hook = HiveCliHook(hive_cli_conn_id='hive_conn')
    transactions_result = hive_hook.run_cli(hql='SELECT * FROM transactions;')
    logs_result = hive_hook.run_cli(hql='SELECT * FROM logs;')

    logging.info("Finished data extraction")

    kwargs['ti'].xcom_push(key='extracted_transactions', value=transactions_result)
    kwargs['ti'].xcom_push(key='extracted_logs', value=logs_result)


def load_data_to_clickhouse(**kwargs):
    ti = kwargs['ti']

    extracted_transactions = ti.xcom_pull(task_ids='extract_hive', key='extracted_transactions')
    extracted_logs = ti.xcom_pull(task_ids='extract_hive', key='extracted_logs')
    click_hook = ClickHouseHook(clickhouse_conn_id='click_conn')

    click_hook.execute('INSERT INTO transactions VALUES', extracted_transactions)
    click_hook.execute('INSERT INTO logs VALUES', extracted_logs)
    logging.info("Finished data loading")


with DAG(
        dag_id='data_migration_dag',
        start_date=datetime(2025, 4, 14),
        catchup=False,
        tags=["migration"],
) as dag:
    task_start = EmptyOperator(task_id='start', dag=dag)
    task_finish = EmptyOperator(task_id='finish', dag=dag)
    create_tables = ClickHouseOperator(
        task_id='create_tables_clickhouse',
        database='homework',
        sql=(
            '''
                CREATE TABLE IF NOT EXISTS transactions(
                transaction_id INT, user_id INT, amount DECIMAL(12,2), currency String, 
                transaction_date DATETIME, is_fraud INT
                ) ENGINE = MergeTree
                ORDER BY transaction_id;
            ''', '''
                CREATE TABLE IF NOT EXISTS logs(
                log_id INT, transaction_id INT, category String, 
                comment String, logtimestamp DATETIME
                ) ENGINE = MergeTree
                ORDER BY log_id;
            ''',
        ),
        clickhouse_conn_id='click_conn',
        dag=dag
    )
    hive_extract = PythonOperator(task_id='extract_hive', python_callable=extract_hive_data, provide_context=True, dag=dag)
    clickhouse_load = PythonOperator(task_id='clickhouse_load', python_callable=load_data_to_clickhouse, provide_context=True, dag=dag)

    task_start >> create_tables >> hive_extract >> clickhouse_load >> task_finish