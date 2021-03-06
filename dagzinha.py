import json
from datetime import datetime, timedelta
from http import client

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from pymongo import MongoClient

now = datetime.now()

def save_posts(ti) -> None:
    posts = ti.xcom_pull(task_ids=['get_posts'])
    CONNECTION_STRING = "mongodb://root:MXIMbnLIi4@mongodb-1656957495.default.svc.cluster.local:27017"
    cliente = MongoClient(CONNECTION_STRING)
    banco = cliente["dags"]
    tabela = banco["tabela"]
    for post in posts[0]:
        tabela.insert_one(post)

with DAG(
    dag_id='api_dag',
    schedule_interval=None,
    start_date=datetime(now.year, now.month, now.day),
    catchup=False
) as dag:
    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_posts',
        endpoint='posts/'
    )

    # 2. Get the posts
    task_get_posts = SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_posts',
        endpoint='posts/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # 3. Save the posts
    task_save = PythonOperator(
        task_id='save_posts',
        python_callable=save_posts
    )

    task_is_api_active >> task_get_posts >> task_save
