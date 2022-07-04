from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

###############################################
# Parameters
###############################################
spark_app_name = "Spark Football Teams"
###############################################
# DAG Definition
###############################################
now = datetime.now()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=1)
}
dag = DAG(
        dag_id="football-teams2", 
        start_date=pendulum.datetime(2022, 6, 21, tz="UTC"),
        catchup=False,
        schedule_interval=timedelta(1),
        description="This DAG runs a simple dataframe show() from a football DF",
        default_args=default_args, 
    )
start = DummyOperator(
    task_id="start", 
    dag=dag,
)
spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/opt/airflow/dags/spark_jobs/sparksubmit_basic.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark",
    total_executor_cores=1,
    executor_memory="1g",
    verbose=1,
    dag=dag)
end = DummyOperator(task_id="end", dag=dag)
start >> spark_job >> end
