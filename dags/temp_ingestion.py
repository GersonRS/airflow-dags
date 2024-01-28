"""
### Generate True Values with MLflow

Artificially generates feedback on the predictions made by the model in the predict DAG.
"""
from __future__ import annotations

import pandas as pd
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.sql.table import Metadata
from astro.sql.table import Table

from utils.constants import default_args


@dag(
    dag_id="temp_ingestion",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval="@once",
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Generate values"],
)
def generate_values() -> None:
    @aql.dataframe()
    def generate_df_values() -> pd.DataFrame:
        from sklearn import datasets

        # load iris dataset
        iris = datasets.load_iris()
        # Since this is a bunch, create a dataframe
        df = pd.DataFrame(iris.data)
        df.columns = [
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
        ]

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

    true_values


generate_true_values = generate_values()
