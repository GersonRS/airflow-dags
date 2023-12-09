import logging

import pandas as pd
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from astro import sql as aql
from utils.constants import default_args
from astro.sql.table import Table, Metadata


@dag(
    dag_id="monitoring_feature",
    default_args=default_args,
    catchup=False,
    schedule=[Dataset("prediction_data")],
    default_view="graph",
    render_template_as_native_obj=True,
    tags=["development", "s3", "minio", "python", "postgres", "ML", "Monitoring"],
)
def feature_monitoring():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @aql.transform
    def get_ref_data(input_table: Table):
        return """
        SELECT sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm
        FROM {{input_table}}
        """

    @aql.transform
    def get_curr_data(input_table: Table):
        return """
        SELECT sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm
        FROM {{input_table}}
        """

    @aql.dataframe(columns_names_capitalization="lower")
    def generate_reports(ref_data: pd.DataFrame, curr_data: pd.DataFrame):
        from evidently.test_suite import TestSuite
        from evidently.test_preset import (
            # NoTargetPerformanceTestPreset,
            DataDriftTestPreset,
            # DataStabilityTestPreset
        )

        suite = TestSuite(
            tests=[
                # NoTargetPerformanceTestPreset(),
                DataDriftTestPreset(),
                # DataStabilityTestPreset()
            ]
        )
        suite.run(reference_data=ref_data, current_data=curr_data)

        return suite.as_dict()

    ref_table = Table(
        name="iris_ground_truth",
        metadata=Metadata(
            schema="public",
            database="feature_store",
        ),
        conn_id="postgres",
    )

    curr_table = Table(
        name="new_features_predictions",
        metadata=Metadata(
            schema="public",
            database="feature_store",
        ),
        conn_id="postgres",
    )

    ref_data = get_ref_data(input_table=ref_table)
    curr_data = get_curr_data(input_table=curr_table)

    reports = generate_reports(ref_data=ref_data, curr_data=curr_data)

    send_report = EmptyOperator(task_id="send_alert")
    # send_report = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_alert",
    #     text="""
    #     *Evidently Test Suite results:*
    #     ```{report}```
    #     """.format(
    #         report="{{ ti.xcom_pull(task_ids='generate_reports') }}"
    #     ),
    #     channel="#integrations",
    # )

    @task.short_circuit
    def check_drift(metrics: str):
        status = metrics["tests"][0]["status"]
        logging.info(status)
        if status == "FAIL":
            return True

    send_retrain_alert = EmptyOperator(task_id="send_retrain_alert")
    # send_retrain_alert = SlackAPIPostOperator(
    #     slack_conn_id="slack_default",
    #     task_id="send_retrain_alert",
    #     text="""
    #     *Warning:* Retrain was triggered because of data drift conditions.
    #     {description}
    #     """.format(
    #         description="{{ ti.xcom_pull(task_ids='generate_reports')['tests'][0]['description'] }}"
    #     ),
    #     channel="#integrations",
    # )

    trigger_retrain = TriggerDagRunOperator(task_id="trigger_retrain", trigger_dag_id="retrain")

    cleanup = aql.cleanup()

    (
        start
        >> chain(
            reports,
            check_drift(metrics="{{ ti.xcom_pull(task_ids='generate_reports') }}"),
            trigger_retrain,
            send_retrain_alert,
        )
        >> end
    )

    start >> reports >> [send_report, cleanup] >> end


feature_monitoring = feature_monitoring()
