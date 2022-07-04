from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
     'owner' : 'airflow',
     #'start_date' : datetime(2020, 04, 21),
     'retries' : 0,
     'catchup' : False,
     'retry_delay' : timedelta(minutes=5),
    }

dag_spark = DAG(dag_id = 'sparkoperator_demo',
    		default_args=default_args,
    		schedule_interval='@once',	
			dagrun_timeout=timedelta(minutes=60),
			description='use case of mysql operator in airflow',
			start_date = airflow.utils.dates.days_ago(1))
	


spark_submit_local = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark_jobs/sparksubmit_basic.py',
		conn_id= 'spark', 
		task_id='spark_submit_task', 
		dag=dag_spark
		)


spark_submit_local 

if __name__ == "__main__":
  dag_spark.cli()
