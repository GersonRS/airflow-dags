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

    @aql.dataframe()
    def generate_df_values():
        from sklearn import datasets
        import pandas as pd

        # load iris dataset
        iris = datasets.load_iris()
        # Since this is a bunch, create a dataframe
        df = pd.DataFrame(iris.data)
        df.columns = ["sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm"]

        df["target"] = iris.target

        df.dropna(how="all", inplace=True)  # remove any empty lines

        return df

    output_table = Table(
        name="iris",
        metadata=Metadata(
            schema="public",
            database="curated",
        ),
        conn_id="conn_curated",
    )

    true_values = generate_df_values(output_table=output_table)

    start >> true_values >> fetch_feature_df_test >> end


generate_true_values = generate_values()
