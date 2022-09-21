"""Basic Airflow unit tests, by calling operator.execute()."""

import datetime
import io
import json
import os
from http import client
from unittest import mock

import pandas as pd
from airflow.models import DagBag
from airflow.models.connection import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dags.src.s3_etl_business import read_business_json_data
from dags.utils.constants import CURATED_ZONE, PROCESSING_ZONE


def return_today(**context):
    return f"Today is {context['execution_date'].strftime('%d-%m-%Y')}"


def test_dag_tags_and_import():
    dag_bag = DagBag(include_examples=False)

    assert not dag_bag.import_errors

    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} in {dag.full_filepath} has no tags"


def test_bash_operator():
    """Validate a BashOperator"""
    test = BashOperator(task_id="test", bash_command="echo hello")
    result = test.execute(context={})
    assert result == "hello"


def test_python_operator():
    """Validate a PythonOperator with a manually supplied execution_date."""

    test = PythonOperator(task_id="test", python_callable=return_today)
    result = test.execute(context={"execution_date": datetime.datetime(2021, 1, 1)})
    assert result == "Today is 01-01-2021"


# @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_s3_etl_operator_with_docker(base_hook_mock, client):    

    files = ["example.json"]

    test = PythonOperator(task_id="test", python_callable=read_business_json_data)
    list_names = test.execute(context={"files": files, "client": client})

    objects = client.list_objects(PROCESSING_ZONE)
    assert len(list(objects)) == len(files)

    for name in list_names:
        obj_curated = client.get_object("curated", name)
        df_curated = pd.read_csv(obj_curated)
        assert "title" in list(df_curated.columns) and "body" in list(df_curated.columns)
