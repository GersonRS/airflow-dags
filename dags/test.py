from airflow import Dataset
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from astro import sql as aql
from astro.dataframes.pandas import DataFrame
from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.registry import (
    CreateModelVersionOperator,
    CreateRegisteredModelOperator,
    TransitionModelVersionStageOperator,
)
from sklearn.linear_model import LogisticRegression
from utils.constants import default_args
from typing import Dict

FILE_PATH = "data.parquet"

# AWS S3 parameters
AWS_CONN_ID = "conn_minio_s3"
DATA_BUCKET_NAME = "data"
MLFLOW_ARTIFACT_BUCKET = "mlflow"

# MLFlow parameters
MLFLOW_CONN_ID = "conn_mlflow"
EXPERIMENT_NAME = "poc"
REGISTERED_MODEL_NAME = "modelIris"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000

# Data parameters
TARGET_COLUMN = "target"


@dag(
    dag_id="teste_1",
    default_args=default_args,
    catchup=False,
    schedule=[Dataset("s3://" + DATA_BUCKET_NAME + "_" + FILE_PATH)],
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Train"],
)
def train():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def fetch_feature_test_df(**context):
        "Fetch the feature dataframe from the feature engineering DAG."
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering", task_ids="feature_eng", include_prior_dates=True
        )
        print(feature_df["X_test"].head())
        return feature_df["X_test"]

    @task
    def fetch_feature_target_df(**context):
        "Fetch the feature dataframe from the feature engineering DAG."
        feature_df = context["ti"].xcom_pull(
            dag_id="feaure_engineering", task_ids="feature_eng", include_prior_dates=True
        )
        print(feature_df["y_test"].head())
        return feature_df["y_test"]

    (start >> [fetch_feature_test_df, fetch_feature_target_df] >> end)


train()
