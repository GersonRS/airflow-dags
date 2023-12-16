from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

table = "mart"
tsk = f"generate_sap_hana_{table}_ingest_table"

default_args = {
    "owner": "Gerson_S",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email": ["gerson.santos@owshq.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(1),
}

dag = DAG(
    dag_id="ingestion_sap_hana_to_minio.yaml",
    default_args=default_args,
    schedule_interval="@once",
    tags=["kubernetes", "SAP", "HANA", "minio"],
    catchup=False,
    max_active_runs=1,
    default_view="graph",
)

generate_ingest_table = SparkKubernetesOperator(
    task_id=tsk,
    namespace="processing",
    application_file="ingestion_sap_hana_to_minio.yaml",
    kubernetes_conn_id="kubernetes_in_cluster",
    do_xcom_push=True,
    dag=dag,
)

monitor_ingest_table = SparkKubernetesSensor(
    task_id=f"monitor_sap_hana_{table}_ingest_table",
    namespace="processing",
    poke_interval=5,
    application_name="{{ task_instance.xcom_pull(task_ids='" + tsk + "')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_in_cluster",
    attach_log=True,
    timeout=3600,
    dag=dag,
)


generate_ingest_table >> monitor_ingest_table
