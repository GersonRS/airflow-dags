# [START pre_requisites]
# create connectivity to minio and yugabytedb on airflow ui [connections]
# file yelp_business.json inside of landing/business bucket on minio
# yugabytedb (postgres) database owshq created
# [END pre_requisites]
from datetime import timedelta
from os import getenv

import airflow

# [START import_module]
from airflow.models import DAG

# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.s3_delete_objects import (
#     S3DeleteObjectsOperator,
# )
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator

# from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

# from src.s3_etl_business import read_business_json_data

# [END import_module]

# [START env_variables]
FILE_LOCATION = getenv("FILE_LOCATION", "business/business.json")
PROCESSING_ZONE = getenv("PROCESSING_ZONE", "processing")
CURATED_ZONE = getenv("CURATED_ZONE", "curated")
CURATED_BUSINESS_CSV_FILE = getenv("CURATED_BUSINESS_CSV_FILE", "business/business.csv")
# [END env_variables]

# [START default_args]
default_args = {
    "owner": "Gerson_S",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email": ["gerson.santos@dellteam.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(1),
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "s3-etl-business-part-2",
    default_args=default_args,
    schedule_interval="@daily",
    tags=["development", "s3", "sensor", "minio", "python", "mongodb"],
)
# [END instantiate_dag]


# [START set_tasks]

# verify if new file has landed into bucket
# verify_file_existence_processing = S3KeySensor(
#     task_id="verify_file_existence_processing",
#     bucket_name=PROCESSING_ZONE,
#     bucket_key="business/*.json",
#     wildcard_match=True,
#     timeout=18 * 60 * 60,
#     poke_interval=120,
#     aws_conn_id="my_aws",
#     dag=dag,
# )

# list all files inside of a bucket processing
list_file_s3_processing_zone = S3ListOperator(
    task_id="list_file_s3_processing_zone",
    bucket=PROCESSING_ZONE,
    prefix="business/",
    delimiter="/",
    aws_conn_id="my_aws",
    dag=dag,
)

# apply transformation [python function]
# process_business_data = PythonOperator(
#     task_id="process_business_data", python_callable=read_business_json_data, dag=dag
# )

# verify if new file has landed into bucket
# verify_file_existence_curated = S3KeySensor(
#     task_id="verify_file_existence_curated",
#     bucket_name=CURATED_ZONE,
#     bucket_key="business/*.csv",
#     wildcard_match=True,
#     timeout=120,
#     poke_interval=30,
#     aws_conn_id="my_aws",
#     dag=dag,
# )

# delete file from processed zone
# delete_s3_file_processed_zone = S3DeleteObjectsOperator(
#     task_id="delete_s3_file_processed_zone",
#     bucket=PROCESSING_ZONE,
#     keys=FILE_LOCATION,
#     aws_conn_id="my_aws",
#     dag=dag,
# )

# end = EmptyOperator(task_id="end", dag=dag)

# [END set_tasks]

# [START task_sequence]
(
    # verify_file_existence_processing
    list_file_s3_processing_zone
    # >> process_business_data
    # >> delete_s3_file_processed_zone
    # >> end
)
# [END task_sequence]
