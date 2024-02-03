"""
### Generate True Values with MLflow

Artificially generates feedback on the predictions made by the model in the predict DAG.
"""
from __future__ import annotations

import logging
import os

import pandas as pd
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.sql.table import Metadata
from astro.sql.table import Table

from utils.constants import default_args

log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))


@dag(
    dag_id="temp_ingestion",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=None,
    default_view="graph",
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Generate values"],
)
def generate_values() -> None:
    @aql.dataframe()
    def generate_df_values() -> pd.DataFrame:
        df = pd.read_csv(
            "http://dl.dropboxusercontent.com/s/xn2a4kzf0zer0xu/acquisition_train.csv?dl=0"
        )

        return df

    output_table = Table(
        name="risk_data",
        metadata=Metadata(
            schema="public",
            database="postgres",
        ),
        conn_id="conn_postgres",
    )

    true_values = generate_df_values(output_table=output_table)

    true_values


generate_true_values = generate_values()
