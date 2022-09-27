"""Basic Airflow unit tests, by calling operator.execute()."""

import datetime
from typing import Any, List

import pandas as pd
from airflow.decorators import task
from airflow.models import DagBag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from dags.src.s3_etl_business import read_business_json_data
from dags.utils.constants import PROCESSING_ZONE
from minio import Minio

# from unittest import mock


def return_today(**context: Any) -> str:
    return f"Today is {context['execution_date'].strftime('%d-%m-%Y')}"


def test_dag_tags_and_import() -> None:
    dag_bag = DagBag(include_examples=False)

    assert not dag_bag.import_errors

    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} in {dag.full_filepath} has no tags"


def test_bash_operator() -> None:
    """Validate a BashOperator"""
    test = BashOperator(task_id="test", bash_command="echo hello")
    result = test.execute(context={})
    assert result == "hello"


def test_python_operator() -> None:
    """Validate a PythonOperator with a manually supplied execution_date."""

    context: Context = Context()
    context["execution_date"] = datetime.datetime(2021, 1, 1)  # type: ignore
    test = PythonOperator(task_id="test", python_callable=return_today)
    result = test.execute(context)
    assert result == "Today is 01-01-2021"


def test_s3_etl_operator_with_docker(client: Minio, files: List[str]) -> None:
    @task
    def files():
        return [[file] for file in files]

    test = PythonOperator.partial(
        task_id="test", python_callable=read_business_json_data
    ).expand(
        op_args=files()
    )
    list_names = test.execute({})

    objects = client.list_objects(PROCESSING_ZONE)
    assert len(list(objects)) == len(files)

    for name in list_names:
        obj_curated = client.get_object("curated", name)
        df_curated = pd.read_csv(obj_curated)
        assert "title" in list(df_curated.columns) and "body" in list(
            df_curated.columns
        )
