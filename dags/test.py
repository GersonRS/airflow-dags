"""
### Generate True Values with MLflow

Artificially generates feedback on the predictions made by the model in the predict DAG.
"""
from astro import sql as aql
from astro.sql.table import Table, Metadata
from utils.constants import default_args
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task


@dag(
    dag_id="seila",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Generate values"],
)
def generate_values():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def fetch_feature_df_test(**context):
        feature_df = context["ti"].xcom_pull(dag_id="feaure_engineering", task_ids="feature_eng")
        return feature_df["X_test"]

    (start >> fetch_feature_df_test >> end)


generate_true_values = generate_values()
