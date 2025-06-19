import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIF32WuDT0XtoS23R8tuj3Dc2T0ko+wlYLH50zwUl1JcC Yandex Cloud Data Proc Key'
YC_DP_SUBNET_ID = 'e9bii53l9tcfd082p8m0'
YC_DP_SA_ID = 'aje2cd52q35tf41qohb9'
YC_BUCKET = 'data-proc-main-bucket'

with DAG(
        dag_id='dataproc_dag',
        schedule_interval='@daily',
        tags=['dataproc'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create-cluster-task',
        cluster_name=f'dataproc-etl-final-cluster',
        cluster_description='',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=32,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=32,
        computenode_count=1,
        computenode_max_hosts_count=3,
        services=['YARN', 'SPARK'],
        datanode_count=0,
    )

    create_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id='create-pyspark-job-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/pyspark-scripts/spark-task-script.py',
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete-cluster-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_dataproc_cluster >> create_pyspark_job >> delete_dataproc_cluster
