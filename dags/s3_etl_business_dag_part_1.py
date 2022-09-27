# [START pre_requisites]
# create connectivity to minio and yugabytedb on airflow ui [connections]
# file yelp_business.json inside of landing/business bucket on minio
# yugabytedb (postgres) database owshq created
# [END pre_requisites]

# [START import_module]
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import (
    S3DeleteObjectsOperator,
)
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

# [START env_variables]
from utils.constants import LANDING_ZONE, PROCESSING_ZONE

# [END env_variables]

# [END import_module]

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
with DAG(
    dag_id="s3-etl-business-part-1",
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    tags=["development", "s3", "sensor", "minio", "python", "mongodb"],
) as dag:
    # [END instantiate_dag]

    # [START set_tasks]
    # verify if new file has landed into bucket
    verify_file_existence_landing = S3KeySensor(
        task_id="verify_file_existence_landing",
        bucket_name=LANDING_ZONE,
        bucket_key="business/" + "{{ ds_nodash }}" + "/*.json",
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id="my_aws",
    )

    # copy file from landing to processing zone
    copy_s3_file_processed_zone = S3CopyObjectOperator(
        task_id="copy_s3_file_processed_zone",
        aws_conn_id="my_aws",
        source_bucket_name=LANDING_ZONE,
        source_bucket_key="business/" + "{{ ds_nodash }}" + "/",
        dest_bucket_name=PROCESSING_ZONE,
        dest_bucket_key="business/" + "{{ ds_nodash }}" + "/",
    )

    # delete file from landing zone [old file]
    delete_s3_file_landing_zone = S3DeleteObjectsOperator(
        task_id="delete_s3_file_landing_zone",
        aws_conn_id="my_aws",
        bucket=LANDING_ZONE,
        prefix="business/" + "{{ ds_nodash }}" + "/",
    )

    # [START task_sequence]
    (
        verify_file_existence_landing
        >> copy_s3_file_processed_zone
        >> delete_s3_file_landing_zone
    )
    # [END task_sequence]
